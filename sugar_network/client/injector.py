# Copyright (C) 2012-2014 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import re
import sys
import json
import time
import random
import hashlib
import logging
from os.path import exists, join

from sugar_network import toolkit
from sugar_network.client import packagekit, journal, profile_path
from sugar_network.toolkit.spec import format_version
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import lsb_release, coroutine, i18n, pylru, http
from sugar_network.toolkit import enforce


_PREEMPTIVE_POOL_SIZE = 256

_logger = logging.getLogger('client.injector')


class Injector(object):

    seqno = 0

    def __init__(self, root, lifetime=None, limit_bytes=None,
            limit_percent=None):
        self._root = root
        self._pool = _PreemptivePool(join(root, 'releases'), lifetime,
                limit_bytes, limit_percent)
        self._api = None
        self._checkins = toolkit.Bin(join(root, 'checkins'), {})

        for dir_name in ('solutions', 'releases'):
            dir_path = join(root, dir_name)
            if not exists(dir_path):
                os.makedirs(dir_path)

    @property
    def api(self):
        if self._api is not None:
            return self._api.url

    @api.setter
    def api(self, value):
        if not value:
            self._api = None
        else:
            self._api = http.Connection(value)

    def close(self):
        self._pool.close()

    def recycle(self):
        self._pool.recycle()

    def launch(self, context, stability='stable', app=None, activity_id=None,
            object_id=None, uri=None, args=None):
        if object_id and not activity_id:
            activity_id = journal.get(object_id, 'activity_id')
        if not activity_id:
            activity_id = _activity_id_new()
        yield {'activity_id': activity_id}

        yield {'event': 'launch', 'state': 'init'}
        releases = []
        acquired = []
        checkedin = {}
        environ = {}

        def acquire(ctx):
            solution = self._solve(ctx, stability)
            environ.update({'context': ctx, 'solution': solution})
            self._pool.pop(solution.values())
            if ctx in self._checkins:
                checkedin[ctx] = (self.api, stability, self.seqno)
            else:
                _logger.debug('Acquire %r', ctx)
                acquired.extend(solution.values())
            releases.extend(solution.values())
            release = solution[ctx]
            return release, self._pool.path(release['blob'])

        try:
            yield {'event': 'launch', 'state': 'solve'}
            release, path = acquire(context)
            if app is None and \
                    release['content-type'] != 'application/vnd.olpc-sugar':
                app = _app_by_mimetype(release['content-type'])
                enforce(app, 'Cannot find proper application')
            if app is None:
                _logger.debug('Execute %r', context)
            else:
                uri = path
                environ['document'] = release['blob']
                release, path = acquire(app)
                _logger.debug('Open %r in %r', context, app)
                context = app

            for event in self._download(releases):
                event['event'] = 'launch'
                yield event
            for event in self._install(releases):
                event['event'] = 'launch'
                yield event

            if args is None:
                args = []
            args.extend(['-b', context])
            args.extend(['-a', activity_id])
            if object_id:
                args.extend(['-o', object_id])
            if uri:
                args.extend(['-u', uri])
            child = _exec(context, release, path, args, environ)
            yield {'event': 'launch', 'state': 'exec'}

            yield environ
            status = child.wait()
        finally:
            if acquired:
                _logger.debug('Release acquired contexts')
                self._pool.push(acquired)

        if checkedin:
            with self._checkins as checkins:
                checkins.update(checkedin)

        _logger.debug('Exit %s[%s]: %r', context, child.pid, status)
        enforce(status == 0, 'Process exited with %r status', status)
        yield {'event': 'launch', 'state': 'exit'}

    def checkin(self, context, stability='stable'):
        if context in self._checkins:
            _logger.debug('Refresh %r checkin', context)
        else:
            _logger.debug('Checkin %r', context)
        yield {'event': 'checkin', 'state': 'solve'}
        solution = self._solve(context, stability)
        for event in self._download(solution.values()):
            event['event'] = 'checkin'
            yield event
        self._pool.pop(solution.values())
        with self._checkins as checkins:
            checkins[context] = (self.api, stability, self.seqno)
        yield {'event': 'checkin', 'state': 'ready'}

    def checkout(self, context):
        if context not in self._checkins:
            return False
        _logger.debug('Checkout %r', context)
        with file(join(self._root, 'solutions', context)) as f:
            __, __, __, solution = json.load(f)
        self._pool.push(solution.values())
        with self._checkins as checkins:
            del checkins[context]
        return True

    def _solve(self, context, stability):
        path = join(self._root, 'solutions', context)
        solution = None

        if exists(path):
            with file(path) as f:
                api, stability_, seqno, solution = json.load(f)
            if self.api:
                if api != self.api or \
                        stability_ and set(stability_) != set(stability) or \
                        seqno < self.seqno or \
                        int(os.stat(path).st_mtime) < packagekit.mtime():
                    _logger.debug('Reset stale %r solution', context)
                    solution = None
                else:
                    _logger.debug('Reuse cached %r solution', context)
            else:
                _logger.debug('Reuse cached %r solution in offline', context)

        if not solution:
            enforce(self.api, 'Cannot solve in offline')
            _logger.debug('Solve %r', context)
            solution = self._api.get(['context', context], cmd='solve',
                    stability=stability, lsb_id=lsb_release.distributor_id(),
                    lsb_release=lsb_release.release())
            with toolkit.new_file(path) as f:
                json.dump((self.api, stability, self.seqno, solution), f)

        return solution

    def _download(self, solution):
        to_download = []
        download_size = 0
        size = 0

        for release in solution:
            digest = release.get('blob')
            if not digest or exists(self._pool.path(digest)):
                continue
            enforce(self._api is not None, 'Cannot download in offline')
            download_size = max(download_size, release['size'])
            size += release.get('unpack_size') or release['size']
            to_download.append((digest, release))

        if not to_download:
            return

        self._pool.ensure(size, download_size)
        for digest, release in to_download:
            yield {'state': 'download'}
            with toolkit.NamedTemporaryFile() as tmp_file:
                self._api.download(['blobs', digest], tmp_file.name)
                path = self._pool.path(digest)
                if 'unpack_size' in release:
                    with Bundle(tmp_file, 'application/zip') as bundle:
                        bundle.extractall(path, prefix=bundle.rootdir)
                    for exec_dir in ('bin', 'activity'):
                        bin_path = join(path, exec_dir)
                        if not exists(bin_path):
                            continue
                        for filename in os.listdir(bin_path):
                            os.chmod(join(bin_path, filename), 0755)
                else:
                    os.rename(tmp_file.name, path)

    def _install(self, solution):
        to_install = []

        for release in solution:
            packages = release.get('packages')
            if packages:
                to_install.extend(packages)

        if to_install:
            yield {'state': 'install'}
            packagekit.install(to_install)


class _PreemptivePool(object):

    def __init__(self, root, lifetime, limit_bytes, limit_percent):
        self._root = root
        self._lifetime = lifetime
        self._limit_bytes = limit_bytes
        self._limit_percent = limit_percent
        self._lru = None
        self._du = None

    def __iter__(self):
        """Least recently to most recently used iterator."""
        if self._lru is None:
            self._init()
        i = self._lru.head.prev
        while True:
            while i.empty:
                if i is self._lru.head:
                    return
                i = i.prev
            yield i.key, i.value
            if i is self._lru.head:
                break
            i = i.prev

    def close(self):
        if self._lru is not None:
            with toolkit.new_file(self._root + '.index') as f:
                json.dump((self._du, [i for i in self]), f)
            self._lru = None

    def path(self, digest):
        return join(self._root, digest)

    def push(self, solution):
        if self._lru is None:
            self._init()
        for release in solution:
            digest = release.get('blob')
            if not digest:
                continue
            path = join(self._root, digest)
            if not exists(path):
                continue
            size = release.get('unpack_size') or release['size']
            self._lru[digest] = (size, os.stat(path).st_mtime)
            self._du += size
            _logger.debug('Push %r release %s bytes', digest, size)

    def pop(self, solution):
        if self._lru is None:
            self._init()
        found = False
        for release in solution:
            digest = release.get('blob')
            if digest and digest in self._lru:
                self._pop(digest, False)
                found = True
        return found

    def ensure(self, requested_size, temp_size=0):
        if self._lru is None:
            self._init()
        to_free = self._to_free(requested_size, temp_size)
        if to_free <= 0:
            return
        enforce(self._du >= to_free, 'No free disk space')
        for digest, (size, __) in self:
            self._pop(digest)
            to_free -= size
            if to_free <= 0:
                break

    def recycle(self):
        if self._lru is None:
            self._init()
        ts = time.time()
        to_free = self._to_free(0, 0)
        for digest, (size, mtime) in self:
            if to_free > 0:
                self._pop(digest)
                to_free -= size
            elif self._lifetime and self._lifetime < (ts - mtime) / 86400.0:
                self._pop(digest)
            else:
                break

    def _init(self):
        self._lru = pylru.lrucache(_PREEMPTIVE_POOL_SIZE, self._pop)
        if not exists(self._root + '.index'):
            self._du = 0
        else:
            with file(self._root + '.index') as f:
                self._du, items = json.load(f)
            for key, value in items:
                self._lru[key] = value

    def _pop(self, digest, unlink=True):
        size, __ = self._lru.peek(digest)
        _logger.debug('Pop %r release and save %s bytes', digest, size)
        self._du -= size
        del self._lru[digest]
        path = join(self._root, digest)
        if unlink and exists(path):
            os.unlink(path)

    def _to_free(self, requested_size, temp_size):
        if not self._limit_bytes and not self._limit_percent:
            return 0

        stat = os.statvfs(self._root)
        if stat.f_blocks == 0:
            # TODO Sounds like a tmpfs or so
            return 0

        limit = sys.maxint
        free = stat.f_bfree * stat.f_frsize
        if self._limit_percent:
            total = stat.f_blocks * stat.f_frsize
            limit = self._limit_percent * total / 100
        if self._limit_bytes:
            limit = min(limit, self._limit_bytes)
        to_free = max(limit, temp_size) - (free - requested_size)

        if to_free > 0:
            _logger.debug(
                    'Need to recycle %d bytes, '
                    'free_size=%d requested_size=%d temp_size=%d',
                    to_free, free, requested_size, temp_size)
        return to_free


def _exec(context, release, path, args, environ):
    # pylint: disable-msg=W0212
    datadir = profile_path('data', context)
    logdir = profile_path('logs')

    for i in [
            join(datadir, 'instance'),
            join(datadir, 'data'),
            join(datadir, 'tmp'),
            logdir,
            ]:
        if not exists(i):
            os.makedirs(i)

    log_path = toolkit.unique_filename(logdir, context + '.log')
    environ['logs'] = [
            profile_path('logs', 'shell.log'),
            profile_path('logs', 'sugar-network-client.log'),
            log_path,
            ]

    __, command = release['command']
    args = command.split() + args
    environ['args'] = args

    child = coroutine.fork()
    if child is not None:
        _logger.debug('Exec %s[%s]: %r', context, child.pid, args)
        return child
    try:
        with file('/dev/null', 'r') as f:
            os.dup2(f.fileno(), 0)
        with file(log_path, 'a+') as f:
            os.dup2(f.fileno(), 1)
            os.dup2(f.fileno(), 2)
        toolkit.init_logging()

        os.chdir(path)

        environ = os.environ
        environ['PATH'] = ':'.join([
            join(path, 'activity'),
            join(path, 'bin'),
            environ['PATH'],
            ])
        environ['PYTHONPATH'] = path + ':' + environ.get('PYTHONPATH', '')
        environ['SUGAR_BUNDLE_PATH'] = path
        environ['SUGAR_BUNDLE_ID'] = context
        environ['SUGAR_BUNDLE_NAME'] = i18n.decode(release['title'])
        environ['SUGAR_BUNDLE_VERSION'] = format_version(release['version'])
        environ['SUGAR_ACTIVITY_ROOT'] = datadir
        environ['SUGAR_LOCALEDIR'] = join(path, 'locale')

        os.execvpe(args[0], args, environ)
    except BaseException:
        logging.exception('Failed to execute %r args=%r', release, args)
    finally:
        os._exit(1)


def _activity_id_new():
    from uuid import getnode
    data = '%s%s%s' % (
            time.time(),
            random.randint(10000, 100000),
            getnode())
    return hashlib.sha1(data).hexdigest()


def _app_by_mimetype(mime_type):
    import gconf
    mime_type = _MIMETYPE_INVALID_CHARS.sub('_', mime_type)
    key = '/'.join([_MIMETYPE_DEFAULTS_KEY, mime_type])
    return gconf.client_get_default().get_string(key)


_MIMETYPE_DEFAULTS_KEY = '/desktop/sugar/journal/defaults'
_MIMETYPE_INVALID_CHARS = re.compile('[^a-zA-Z0-9-_/.]')
