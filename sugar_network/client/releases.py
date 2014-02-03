# Copyright (C) 2013 Aleksey Lim
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

# pylint: disable=E1101

import os
import re
import sys
import time
import json
import random
import shutil
import hashlib
import logging
from copy import deepcopy
from os.path import join, exists, basename, dirname, relpath

from sugar_network import client, toolkit
from sugar_network.client.cache import Cache
from sugar_network.client import journal, packagekit
from sugar_network.toolkit.router import Request, Response, route
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import http, coroutine, enforce


_MIMETYPE_DEFAULTS_KEY = '/desktop/sugar/journal/defaults'
_MIMETYPE_INVALID_CHARS = re.compile('[^a-zA-Z0-9-_/.]')

_logger = logging.getLogger('releases')


class Routes(object):

    def __init__(self, local_volume):
        self._volume = local_volume
        self._node_mtime = None
        self._call = lambda **kwargs: \
                self._map_exceptions(self.fallback, **kwargs)
        self._cache = Cache(local_volume)

    def invalidate_solutions(self, mtime):
        self._node_mtime = mtime

    @route('GET', ['context', None], cmd='path')
    def path(self, request):
        clone_path = self._volume['context'].path(request.guid, '.clone')
        enforce(exists(clone_path), http.NotFound)
        clone_impl = basename(os.readlink(clone_path))
        return self._volume['release'].path(clone_impl, 'data')

    @route('GET', ['context', None], cmd='launch', arguments={'args': list},
            mime_type='text/event-stream')
    def launch(self, request):
        activity_id = request.get('activity_id')
        if 'object_id' in request and not activity_id:
            activity_id = journal.get(request['object_id'], 'activity_id')
        if not activity_id:
            activity_id = _activity_id_new()
        request.session['activity_id'] = activity_id

        for context in self._checkin_context(request):
            yield {'event': 'launch', 'activity_id': activity_id}, request

            acquired = []
            try:
                impl = self._solve_impl(context, request)
                if 'activity' not in context['type']:
                    app = request.get('context') or \
                            _mimetype_context(impl['data']['mime_type'])
                    enforce(app, 'Cannot find proper application')
                    acquired += self._checkin_impl(
                            context, request, self._cache.acquire)
                    request = Request(path=['context', app],
                            object_id=impl['path'], session=request.session)
                    for context in self._checkin_context(request):
                        impl = self._solve_impl(context, request)
                acquired += self._checkin_impl(
                        context, request, self._cache.acquire)

                child = _exec(context, request, impl)
                yield {'event': 'exec', 'activity_id': activity_id}
                status = child.wait()
            finally:
                self._cache.release(*acquired)

            _logger.debug('Exit %s[%s]: %r', context.guid, child.pid, status)
            enforce(status == 0, 'Process exited with %r status', status)
            yield {'event': 'exit', 'activity_id': activity_id}

    @route('PUT', ['context', None], cmd='clone', arguments={'requires': list},
            mime_type='text/event-stream')
    def clone(self, request):
        enforce(not request.content or self.inline(), http.ServiceUnavailable,
                'Not available in offline')
        for context in self._checkin_context(request, 'clone'):
            cloned_path = context.path('.clone')
            if request.content:
                impl = self._solve_impl(context, request)
                self._checkin_impl(context, request, self._cache.checkout)
                impl_path = relpath(dirname(impl['path']), context.path())
                os.symlink(impl_path, cloned_path)
                yield {'event': 'ready'}
            else:
                cloned_impl = basename(os.readlink(cloned_path))
                meta = self._volume['release'].get(cloned_impl).meta('data')
                size = meta.get('unpack_size') or meta['blob_size']
                self._cache.checkin(cloned_impl, size)
                os.unlink(cloned_path)

    @route('GET', ['context', None], cmd='clone',
            arguments={'requires': list})
    def get_clone(self, request, response):
        return self._get_clone(request, response)

    @route('HEAD', ['context', None], cmd='clone',
            arguments={'requires': list})
    def head_clone(self, request, response):
        self._get_clone(request, response)

    @route('PUT', ['context', None], cmd='favorite')
    def favorite(self, request):
        for __ in self._checkin_context(request, 'favorite'):
            pass

    @route('GET', cmd='recycle')
    def recycle(self):
        return self._cache.recycle()

    def _map_exceptions(self, fun, *args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except http.NotFound, error:
            if self.inline():
                raise
            raise http.ServiceUnavailable, error, sys.exc_info()[2]

    def _checkin_context(self, request, layer=None):
        contexts = self._volume['context']
        guid = request.guid
        if layer and not request.content and not contexts.exists(guid):
            return

        if not contexts.exists(guid):
            context = self._call(method='GET', path=['context', guid])
            contexts.create(context, setters=True)
            for prop in ('icon', 'artifact_icon', 'logo'):
                blob = self._call(method='GET', path=['context', guid, prop])
                if blob is not None:
                    contexts.update(guid, {prop: {'blob': blob}})
        context = contexts.get(guid)
        if layer and bool(request.content) == (layer in context['layer']):
            return

        yield context

        if layer:
            if request.content:
                layer_value = set(context['layer']) | set([layer])
            else:
                layer_value = set(context['layer']) - set([layer])
            contexts.update(guid, {'layer': list(layer_value)})
            self.broadcast({
                'event': 'update',
                'resource': 'context',
                'guid': guid,
                })
            _logger.debug('Checked %r in: %r', guid, layer_value)

    def _solve_impl(self, context, request):
        stability = request.get('stability') or \
                client.stability(request.guid)

        request.session['stability'] = stability
        request.session['logs'] = [
                client.profile_path('logs', 'shell.log'),
                client.profile_path('logs', 'sugar-network-client.log'),
                ]

        _logger.debug('Solving %r stability=%r', request.guid, stability)

        solution, stale = self._cache_solution_get(request.guid, stability)
        if stale is False:
            _logger.debug('Reuse cached %r solution', request.guid)
        elif solution is not None and not self.inline():
            _logger.debug('Reuse stale %r in offline', request.guid)
        elif 'activity' in context['type']:
            from sugar_network.client import solver
            solution = self._map_exceptions(solver.solve,
                    self.fallback, request.guid, stability)
        else:
            response = Response()
            blob = self._call(method='GET', path=['context', request.guid],
                    cmd='clone', stability=stability, response=response)
            response.meta['data']['blob'] = blob
            solution = [response.meta]

        request.session['solution'] = solution
        return solution[0]

    def _checkin_impl(self, context, request, cache_call):
        if 'clone' in context['layer']:
            cache_call = self._cache.checkout
        impls = self._volume['release']

        if 'activity' in context['type']:
            to_install = []
            for sel in request.session['solution']:
                if 'install' in sel:
                    enforce(self.inline(), http.ServiceUnavailable,
                            'Installation is not available in offline')
                    to_install.extend(sel.pop('install'))
            if to_install:
                packagekit.install(to_install)

        def cache_impl(sel):
            guid = sel['guid']
            data = sel['data']
            sel['path'] = impls.path(guid, 'data')
            size = data.get('unpack_size') or data['blob_size']

            blob = None
            if 'blob' in data:
                blob = data.pop('blob')

            if impls.exists(guid):
                return cache_call(guid, size)

            if blob is None:
                blob = self._call(method='GET', path=['release', guid, 'data'])

            blob_dir = dirname(sel['path'])
            if not exists(blob_dir):
                os.makedirs(blob_dir)

            with toolkit.mkdtemp(dir=blob_dir) as blob_dir:
                if 'activity' in context['type']:
                    self._cache.ensure(size, data['blob_size'])
                    with toolkit.TemporaryFile() as tmp_file:
                        shutil.copyfileobj(blob, tmp_file)
                        tmp_file.seek(0)
                        with Bundle(tmp_file, 'application/zip') as bundle:
                            bundle.extractall(blob_dir, prefix=bundle.rootdir)
                    for exec_dir in ('bin', 'activity'):
                        bin_path = join(blob_dir, exec_dir)
                        if not exists(bin_path):
                            continue
                        for filename in os.listdir(bin_path):
                            os.chmod(join(bin_path, filename), 0755)
                    blob = blob_dir
                else:
                    self._cache.ensure(size)
                    with file(join(blob_dir, 'data'), 'wb') as f:
                        shutil.copyfileobj(blob, f)
                        blob = f.name
                impl = deepcopy(sel)
                impl['mtime'] = impl['ctime']
                impl['data']['blob'] = blob
                impls.create(impl)
                return cache_call(guid, size)

        result = []
        for sel in request.session['solution']:
            if 'path' not in sel and sel['stability'] != 'packaged':
                result.append(cache_impl(sel))
        self._cache_solution_set(context.guid,
                request.session['stability'], request.session['solution'])
        return result

    def _cache_solution_path(self, guid):
        return client.path('solutions', guid[:2], guid)

    def _cache_solution_get(self, guid, stability):
        path = self._cache_solution_path(guid)
        solution = None
        if exists(path):
            try:
                with file(path) as f:
                    cached_api_url, cached_stability, solution = json.load(f)
            except Exception, error:
                _logger.debug('Cannot open %r solution: %s', path, error)
        if solution is None:
            return None, None

        stale = (cached_api_url != client.api_url.value)
        if not stale and cached_stability is not None:
            stale = set(cached_stability) != set(stability)
        if not stale and self._node_mtime is not None:
            stale = (self._node_mtime > os.stat(path).st_mtime)
        if not stale:
            stale = (packagekit.mtime() > os.stat(path).st_mtime)
        return _CachedSolution(solution), stale

    def _cache_solution_set(self, guid, stability, solution):
        if isinstance(solution, _CachedSolution):
            return
        path = self._cache_solution_path(guid)
        if not exists(dirname(path)):
            os.makedirs(dirname(path))
        with file(path, 'w') as f:
            json.dump([client.api_url.value, stability, solution], f)

    def _get_clone(self, request, response):
        for context in self._checkin_context(request):
            if 'clone' not in context['layer']:
                return self._map_exceptions(self.fallback, request, response)
            guid = basename(os.readlink(context.path('.clone')))
            impl = self._volume['release'].get(guid)
            response.meta = impl.properties([
                'guid', 'ctime', 'layer', 'author', 'tags',
                'context', 'version', 'stability', 'license', 'notes', 'data',
                ])
            return impl.meta('data')


def _activity_id_new():
    from uuid import getnode
    data = '%s%s%s' % (
            time.time(),
            random.randint(10000, 100000),
            getnode())
    return hashlib.sha1(data).hexdigest()


def _mimetype_context(mime_type):
    import gconf
    mime_type = _MIMETYPE_INVALID_CHARS.sub('_', mime_type)
    key = '/'.join([_MIMETYPE_DEFAULTS_KEY, mime_type])
    return gconf.client_get_default().get_string(key)


def _exec(context, request, sel):
    # pylint: disable-msg=W0212
    datadir = client.profile_path('data', context.guid)
    logdir = client.profile_path('logs')

    for path in [
            join(datadir, 'instance'),
            join(datadir, 'data'),
            join(datadir, 'tmp'),
            logdir,
            ]:
        if not exists(path):
            os.makedirs(path)

    cmd = sel['data']['spec']['*-*']['commands']['activity']['exec']
    args = cmd.split() + [
            '-b', request.guid,
            '-a', request.session['activity_id'],
            ]
    if 'object_id' in request:
        args.extend(['-o', request['object_id']])
    if 'uri' in request:
        args.extend(['-u', request['uri']])
    if 'args' in request:
        args.extend(request['args'])
    request.session['args'] = args

    log_path = toolkit.unique_filename(logdir, context.guid + '.log')
    request.session['logs'].append(log_path)

    child = coroutine.fork()
    if child is not None:
        _logger.debug('Exec %s[%s]: %r', request.guid, child.pid, args)
        return child

    try:
        with file('/dev/null', 'r') as f:
            os.dup2(f.fileno(), 0)
        with file(log_path, 'a+') as f:
            os.dup2(f.fileno(), 1)
            os.dup2(f.fileno(), 2)
        toolkit.init_logging()

        impl_path = sel['path']
        os.chdir(impl_path)

        environ = os.environ
        environ['PATH'] = ':'.join([
            join(impl_path, 'activity'),
            join(impl_path, 'bin'),
            environ['PATH'],
            ])
        environ['PYTHONPATH'] = impl_path + ':' + \
                environ.get('PYTHONPATH', '')
        environ['SUGAR_BUNDLE_PATH'] = impl_path
        environ['SUGAR_BUNDLE_ID'] = context.guid
        environ['SUGAR_BUNDLE_NAME'] = \
                toolkit.gettext(context['title']).encode('utf8')
        environ['SUGAR_BUNDLE_VERSION'] = sel['version']
        environ['SUGAR_ACTIVITY_ROOT'] = datadir
        environ['SUGAR_LOCALEDIR'] = join(impl_path, 'locale')

        os.execvpe(args[0], args, environ)
    except BaseException:
        logging.exception('Failed to execute %r args=%r', sel, args)
    finally:
        os._exit(1)


class _CachedSolution(list):
    pass
