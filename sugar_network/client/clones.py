# Copyright (C) 2012 Aleksey Lim
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
import errno
import shutil
import hashlib
import logging
from os.path import join, exists, lexists, relpath, dirname, basename, isdir
from os.path import abspath, islink

from sugar_network import db, client
from sugar_network.toolkit.spec import Spec
from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_CLOSE_WRITE, \
        IN_MOVED_TO, IN_MOVED_FROM
from sugar_network.toolkit import coroutine, util, exception


_logger = logging.getLogger('client.clones')


def walk(context):
    root = _context_path(context, '')
    if not exists(root):
        return

    for filename in os.listdir(root):
        path = join(root, filename)
        if exists(path):
            yield os.readlink(path)


def wipeout(context):
    for path in walk(context):
        _logger.info('Wipe out %r implementation from %r', context, path)
        if isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)


def monitor(contexts, paths):
    inotify = _Inotify(contexts)
    inotify.setup(paths)
    inotify.serve_forever()


def populate(contexts, paths):
    inotify = _Inotify(contexts)
    inotify.add_watch = lambda *args: None
    inotify.setup(paths)


class _Inotify(Inotify):

    def __init__(self, contexts):
        Inotify.__init__(self)

        self._contexts = contexts
        self._roots = []
        self._jobs = coroutine.Pool()

        xdg_data_home = os.environ.get('XDG_DATA_HOME') or \
                join(os.environ['HOME'], '.local', 'share')
        self._icons_dir = join(xdg_data_home,
                'icons', 'sugar', 'scalable', 'mimetypes')
        self._mime_dir = join(xdg_data_home, 'mime')

    def setup(self, paths):
        mtime = 0
        for path in paths:
            path = abspath(path)
            if not exists(path):
                if not os.access(dirname(path), os.W_OK):
                    _logger.warning('No permissions to create %s '
                            'directory, do not monitor it', path)
                    continue
                os.makedirs(path)
            mtime = max(mtime, os.stat(path).st_mtime)
            self._roots.append(_Root(self, path))

        if mtime <= self._contexts.mtime:
            return

        docs, __ = self._contexts.find(limit=db.MAX_LIMIT, clone=[1, 2])
        for context in docs:
            root = _context_path(context.guid, '')
            found = False
            if exists(root):
                for filename in os.listdir(root):
                    path = join(root, filename)
                    if lexists(path):
                        if not exists(path):
                            os.unlink(path)
                        else:
                            found = True
                            break
            if found:
                if context['clone'] != 2:
                    self._contexts.update(context.guid, {'clone': 2})
            else:
                self._contexts.update(context.guid, {'clone': 0})

    def serve_forever(self):
        while True:
            coroutine.select([self.fileno()], [], [])
            if self.closed:
                break
            for filename, event, cb in self.read():
                try:
                    cb(filename, event)
                except Exception:
                    exception('Cannot dispatch 0x%X event for %r',
                            event, filename)
                coroutine.dispatch()

    def found(self, clone_path):
        hashed_path, checkin_path = _checkin_path(clone_path)
        if exists(checkin_path):
            return

        _logger.debug('Checking in activity from %r', clone_path)

        try:
            spec = Spec(root=clone_path)
        except Exception:
            exception(_logger, 'Cannot read %r spec', clone_path)
            return

        context = spec['implement']

        context_path = _context_path(context, hashed_path)
        _ensure_path(context_path)
        os.symlink(clone_path, context_path)

        _ensure_path(checkin_path)
        os.symlink(relpath(context_path, dirname(checkin_path)), checkin_path)

        if self._contexts.exists(context):
            self._contexts.update(context, {'clone': 2})
        else:
            _logger.debug('Register unknown local activity, %r', context)

            mtime = os.stat(spec.root).st_mtime
            self._contexts.create({
                'guid': context,
                'type': 'activity',
                'title': spec['name'],
                'summary': spec['summary'],
                'description': spec['description'],
                'clone': 2,
                'ctime': mtime,
                'mtime': mtime,
                })

            icon_path = join(spec.root, spec['icon'])
            if exists(icon_path):
                with file(icon_path, 'rb') as f:
                    self._contexts.update(context,
                            {'artifact_icon': {'blob': f}})
                with util.NamedTemporaryFile() as f:
                    util.svg_to_png(icon_path, f.name, 32, 32)
                    self._contexts.update(context, {'icon': {'blob': f.name}})

        self._checkin_activity(spec)

    def found_mimetypes(self, impl_path):
        hashed_path, __ = _checkin_path(impl_path)
        src_path = join(impl_path, 'activity', 'mimetypes.xml')
        dst_path = join(self._mime_dir, 'packages', hashed_path + '.xml')

        if exists(dst_path):
            return

        _logger.debug('Update MIME database to process found %r', src_path)

        util.symlink(src_path, dst_path)
        util.spawn('update-mime-database', self._mime_dir)

    def lost(self, clone_path):
        __, checkin_path = _checkin_path(clone_path)
        if not lexists(checkin_path):
            return

        _logger.debug('Checking out activity from %r', clone_path)

        context_path = _read_checkin_path(checkin_path)
        context_dir = dirname(context_path)
        impls = set(os.listdir(context_dir)) - set([basename(context_path)])

        if not impls:
            context = basename(context_dir)
            if self._contexts.exists(context):
                self._contexts.update(context, {'clone': 0})

        if lexists(context_path):
            os.unlink(context_path)
        os.unlink(checkin_path)

        self._checkout_activity(clone_path)

    def lost_mimetypes(self, impl_path):
        hashed_path, __ = _checkin_path(impl_path)
        dst_path = join(self._mime_dir, 'packages', hashed_path + '.xml')

        if not lexists(dst_path):
            return

        _logger.debug('Update MIME database to process lost %r', impl_path)

        os.unlink(dst_path)
        util.spawn('update-mime-database', self._mime_dir)

    def _checkin_activity(self, spec):
        icon_path = join(spec.root, spec['icon'])
        if spec['mime_types'] and exists(icon_path):
            _logger.debug('Register %r icons for %r',
                    spec['mime_types'], spec)
            if not exists(self._icons_dir):
                os.makedirs(self._icons_dir)
            for mime_type in spec['mime_types']:
                util.symlink(icon_path,
                        join(self._icons_dir,
                            mime_type.replace('/', '-') + '.svg'))

    def _checkout_activity(self, clone_path):
        if exists(self._icons_dir):
            for filename in os.listdir(self._icons_dir):
                path = join(self._icons_dir, filename)
                if islink(path) and \
                        os.readlink(path).startswith(clone_path + os.sep):
                    os.unlink(path)


class _Root(object):

    def __init__(self, monitor_, path):
        self.path = path
        self._monitor = monitor_
        self._nodes = {}

        _logger.info('Start monitoring %r implementations root', self.path)

        self._monitor.add_watch(self.path,
                IN_DELETE_SELF | IN_CREATE | IN_DELETE |
                        IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        for filename in os.listdir(self.path):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(self._monitor, path)

    def __watch_cb(self, filename, event):
        if event & IN_DELETE_SELF:
            _logger.warning('Lost ourselves, cannot monitor anymore')
            self._nodes.clear()
            return

        if event & (IN_CREATE | IN_MOVED_TO):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(self._monitor, path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            node = self._nodes.get(filename)
            if node is not None:
                node.unlink()
                del self._nodes[filename]


class _Node(object):

    def __init__(self, monitor_, path):
        self._path = path
        self._monitor = monitor_
        self._activity_path = join(path, 'activity')
        self._activity_dir = None

        _logger.debug('Start monitoring %r root activity directory', path)

        self._wd = self._monitor.add_watch(path,
                IN_CREATE | IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        if exists(self._activity_path):
            self._activity_dir = \
                    _ActivityDir(self._monitor, self._activity_path)

    def unlink(self):
        if self._activity_dir is not None:
            self._activity_dir.unlink()
            self._activity_dir = None
        _logger.debug('Stop monitoring %r root activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def __watch_cb(self, filename, event):
        if filename != 'activity':
            return
        if event & (IN_CREATE | IN_MOVED_TO):
            self._activity_dir = \
                    _ActivityDir(self._monitor, self._activity_path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self._activity_dir.unlink()
            self._activity_dir = None


class _ActivityDir(object):

    def __init__(self, monitor_, path):
        self._path = path
        self._monitor = monitor_
        self._found = False
        self._node_path = dirname(path)

        _logger.debug('Start monitoring %r activity directory', path)

        self._wd = self._monitor.add_watch(path,
                IN_CREATE | IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_TO |
                        IN_MOVED_FROM,
                self.__watch_cb)

        for filename in ('activity.info', 'mimetypes.xml'):
            if exists(join(path, filename)):
                self.found(filename)

    def unlink(self):
        self.lost('activity.info')
        _logger.debug('Stop monitoring %r activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def found(self, filename):
        if filename == 'mimetypes.xml':
            self._monitor.found_mimetypes(self._node_path)
            return
        if self._found:
            return
        _logger.debug('Found %r', self._node_path)
        self._found = True
        self._monitor.found(self._node_path)
        if exists(join(self._path, 'mimetypes.xml')):
            self._monitor.found_mimetypes(self._node_path)

    def lost(self, filename):
        if filename == 'mimetypes.xml':
            self._monitor.lost_mimetypes(self._node_path)
            return
        if not self._found:
            return
        _logger.debug('Lost %r', self._node_path)
        self._found = False
        self._monitor.lost(self._node_path)

    def __watch_cb(self, filename, event):
        if filename not in ('activity.info', 'mimetypes.xml'):
            return
        if event & IN_CREATE:
            # There is only one case when newly created file can be read,
            # if number of hardlinks is bigger than one, i.e., its content
            # already populated
            if os.stat(join(self._path, filename)).st_nlink > 1:
                self.found(filename)
        elif event & (IN_CLOSE_WRITE | IN_MOVED_TO):
            self.found(filename)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self.lost(filename)


def _checkin_path(clone_path):
    hashed_path = hashlib.sha1(clone_path).hexdigest()
    return hashed_path, client.path('clones', 'checkin', hashed_path)


def _read_checkin_path(checkin_path):
    return join(dirname(checkin_path), os.readlink(checkin_path))


def _context_path(context, hashed_path):
    return client.path('clones', 'context', context, hashed_path)


def _ensure_path(path):
    if lexists(path):
        os.unlink(path)
        return

    dir_path = dirname(path)
    if exists(dir_path):
        return

    try:
        os.makedirs(dir_path)
    except OSError, error:
        # In case if another process already create directory
        if error.errno != errno.EEXIST:
            raise
