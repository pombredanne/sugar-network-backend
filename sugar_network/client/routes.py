# Copyright (C) 2012-2013 Aleksey Lim
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
import logging
import httplib
from zipfile import ZipFile, ZIP_DEFLATED
from os.path import join, basename

from sugar_network import db, client, node, toolkit, model
from sugar_network.client import journal, implementations
from sugar_network.node.slave import SlaveRoutes
from sugar_network.toolkit import netlink, mountpoints
from sugar_network.toolkit.router import ACL, Request, Response, Router
from sugar_network.toolkit.router import route, fallbackroute
from sugar_network.toolkit import zeroconf, coroutine, http, enforce


# Top-level directory name to keep SN data on mounted devices
_SN_DIRNAME = 'sugar-network'
# Flag file to recognize a directory as a synchronization directory
_SYNC_DIRNAME = 'sugar-network-sync'
_RECONNECT_TIMEOUT = 3
_RECONNECT_TIMEOUT_MAX = 60 * 15
_LOCAL_LAYERS = frozenset(['local', 'clone', 'favorite'])

_logger = logging.getLogger('client.routes')


class ClientRoutes(model.FrontRoutes, implementations.Routes, journal.Routes):

    def __init__(self, home_volume, api_url=None, no_subscription=False):
        model.FrontRoutes.__init__(self)
        implementations.Routes.__init__(self, home_volume)
        if not client.no_dbus.value:
            journal.Routes.__init__(self)

        self._local = _LocalRoutes(home_volume)
        self._inline = coroutine.Event()
        self._inline_job = coroutine.Pool()
        self._remote_urls = []
        self._node = None
        self._jobs = coroutine.Pool()
        self._no_subscription = no_subscription
        self._server_mode = not api_url

        self._got_offline()

        if self._server_mode:
            mountpoints.connect(_SN_DIRNAME,
                    self._found_mount, self._lost_mount)
        else:
            if client.discover_server.value:
                self._jobs.spawn(self._discover_node)
            else:
                self._remote_urls.append(api_url)
            self._jobs.spawn(self._wait_for_connectivity)

    def close(self):
        self._jobs.kill()
        self._got_offline()
        self._local.volume.close()

    @fallbackroute('GET', ['hub'])
    def hub(self, request, response):
        """Serve Hub via HTTP instead of file:// for IPC users.

        Since SSE doesn't support CORS for now.

        """
        if request.environ['PATH_INFO'] == '/hub':
            raise http.Redirect('/hub/')

        path = request.path[1:]
        if not path:
            path = ['index.html']
        path = join(client.hub_root.value, *path)

        mtime = int(os.stat(path).st_mtime)
        if request.if_modified_since >= mtime:
            raise http.NotModified()

        if path.endswith('.js'):
            response.content_type = 'text/javascript'
        if path.endswith('.css'):
            response.content_type = 'text/css'
        response.last_modified = mtime

        return file(path, 'rb')

    @fallbackroute('GET', ['packages'])
    def route_packages(self, request, response):
        if self._inline.is_set():
            return self.fallback(request, response)
        else:
            # Let caller know that we are in offline and
            # no way to process specified request on the node
            raise http.ServiceUnavailable()

    @route('GET', cmd='status',
            mime_type='application/json')
    def status(self):
        result = {'route': 'proxy' if self._inline.is_set() else 'offline'}
        if self._inline.is_set():
            result['node'] = self._node.api_url
        return result

    @route('GET', cmd='inline',
            mime_type='application/json')
    def inline(self):
        if not self._server_mode and not self._inline.is_set():
            self._remote_connect()
        return self._inline.is_set()

    def whoami(self, request, response):
        if self._inline.is_set():
            return self.fallback(request, response)
        else:
            return {'roles': [], 'guid': client.sugar_uid()}

    @route('GET', [None],
            arguments={
                'offset': int,
                'limit': int,
                'reply': ('guid',),
                'layer': list,
                },
            mime_type='application/json')
    def find(self, request, response, layer):
        if set(request.get('layer', [])) & set(['favorite', 'clone']):
            return self._local.call(request, response)

        reply = request.setdefault('reply', ['guid'])
        if 'layer' not in reply:
            return self.fallback(request, response)

        if 'guid' not in reply:
            # Otherwise there is no way to mixin local `layer`
            reply.append('guid')
        result = self.fallback(request, response)

        directory = self._local.volume[request.resource]
        for item in result['result']:
            if directory.exists(item['guid']):
                existing_layer = directory.get(item['guid'])['layer']
                item['layer'][:] = set(item['layer']) | set(existing_layer)

        return result

    @route('GET', [None, None], mime_type='application/json')
    def get(self, request, response):
        if self._local.volume[request.resource].exists(request.guid):
            return self._local.call(request, response)
        else:
            return self.fallback(request, response)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self, request, response):
        if self._local.volume[request.resource].exists(request.guid):
            return self._local.call(request, response)
        else:
            return self.fallback(request, response)

    @route('POST', ['report'], cmd='submit', mime_type='text/event-stream')
    def submit_report(self, request, response):
        logs = request.content.pop('logs')
        guid = self.fallback(method='POST', path=['report'],
                content=request.content, content_type='application/json')
        if logs:
            with toolkit.TemporaryFile() as tmpfile:
                with ZipFile(tmpfile, 'w', ZIP_DEFLATED) as zipfile:
                    for path in logs:
                        zipfile.write(path, basename(path))
                tmpfile.seek(0)
                self.fallback(method='PUT', path=['report', guid, 'data'],
                        content_stream=tmpfile, content_type='application/zip')
        yield {'event': 'done', 'guid': guid}

    @fallbackroute()
    def fallback(self, request=None, response=None, method=None, path=None,
            cmd=None, content=None, content_stream=None, content_type=None,
            **kwargs):
        if request is None:
            request = Request(method=method, path=path, cmd=cmd,
                    content=content, content_stream=content_stream,
                    content_type=content_type)
        if response is None:
            response = Response()
        request.update(kwargs)
        if self._inline.is_set():
            if client.layers.value and \
                    request.resource in ('context', 'implementation'):
                request.add('layer', *client.layers.value)
            try:
                reply = self._node.call(request, response)
                if hasattr(reply, 'read'):
                    return _ResponseStream(reply, self._restart_online)
                else:
                    return reply
            except (http.ConnectionError, httplib.IncompleteRead):
                self._restart_online()
                return self._local.call(request, response)
        else:
            return self._local.call(request, response)

    def _got_online(self):
        enforce(not self._inline.is_set())
        _logger.debug('Got online on %r', self._node)
        self._inline.set()
        self.broadcast({'event': 'inline', 'state': 'online'})
        self._local.volume.broadcast = None

    def _got_offline(self):
        if self._inline.is_set():
            _logger.debug('Got offline on %r', self._node)
            self._node.close()
            self._inline.clear()
        self.broadcast({'event': 'inline', 'state': 'offline'})
        self._local.volume.broadcast = self.broadcast

    def _fall_offline(self):
        if self._inline_job:
            _logger.debug('Fall to offline on %r', self._node)
            self._inline_job.kill()

    def _restart_online(self):
        self._fall_offline()
        _logger.debug('Try to become online in %s seconds', _RECONNECT_TIMEOUT)
        self._remote_connect(_RECONNECT_TIMEOUT)

    def _discover_node(self):
        for host in zeroconf.browse_workstations():
            url = 'http://%s:%s' % (host, node.port.default)
            if url not in self._remote_urls:
                self._remote_urls.append(url)
            self._remote_connect()

    def _wait_for_connectivity(self):
        for i in netlink.wait_for_route():
            self._fall_offline()
            if i:
                self._remote_connect()

    def _remote_connect(self, timeout=0):

        def pull_events():
            for event in self._node.subscribe():
                if event.get('resource') == 'implementation':
                    mtime = event.get('mtime')
                    if mtime:
                        self.invalidate_solutions(mtime)
                self.broadcast(event)

        def handshake(url):
            _logger.debug('Connecting to %r node', url)
            self._node = client.Connection(url)
            info = self._node.get(cmd='info')
            impl_info = info['resources'].get('implementation')
            if impl_info:
                self.invalidate_solutions(impl_info['mtime'])
            if self._inline.is_set():
                _logger.info('Reconnected to %r node', url)
            else:
                self._got_online()

        def connect():
            timeout = _RECONNECT_TIMEOUT
            while True:
                self.broadcast({'event': 'inline', 'state': 'connecting'})
                for url in self._remote_urls:
                    while True:
                        try:
                            handshake(url)
                            timeout = _RECONNECT_TIMEOUT
                            if self._no_subscription:
                                return
                            pull_events()
                        except (http.BadGateway, http.GatewayTimeout):
                            _logger.debug('Retry %r on gateway error', url)
                            continue
                        except Exception:
                            _logger.exception('Connection to %r failed', url)
                        break
                self._got_offline()
                if not timeout:
                    break
                _logger.debug('Try to reconect in %s seconds', timeout)
                coroutine.sleep(timeout)
                timeout *= _RECONNECT_TIMEOUT
                timeout = min(timeout, _RECONNECT_TIMEOUT_MAX)

        if not self._inline_job:
            self._inline_job.spawn_later(timeout, connect)

    def _found_mount(self, root):
        if self._inline.is_set():
            _logger.debug('Found %r node mount but %r is already active',
                    root, self._node.volume.root)
            return

        _logger.debug('Found %r node mount', root)

        db_path = join(root, _SN_DIRNAME, 'db')
        node.data_root.value = db_path
        node.stats_root.value = join(root, _SN_DIRNAME, 'stats')
        node.files_root.value = join(root, _SN_DIRNAME, 'files')

        volume = db.Volume(db_path, model.RESOURCES)
        self._node = _NodeRoutes(join(db_path, 'node'), volume,
                self.broadcast)
        self._jobs.spawn(volume.populate)

        logging.info('Start %r node on %s port', volume.root, node.port.value)
        server = coroutine.WSGIServer(('0.0.0.0', node.port.value), self._node)
        self._inline_job.spawn(server.serve_forever)
        self._got_online()

    def _lost_mount(self, root):
        if not self._inline.is_set() or \
                not self._node.volume.root.startswith(root):
            return
        _logger.debug('Lost %r node mount', root)
        self._inline_job.kill()
        self._got_offline()


class CachedClientRoutes(ClientRoutes):

    def __init__(self, home_volume, api_url=None, no_subscription=False):
        self._push_seq = toolkit.PersistentSequence(
                join(home_volume.root, 'push.sequence'), [1, None])
        self._push_job = coroutine.Pool()
        ClientRoutes.__init__(self, home_volume, api_url, no_subscription)

    def _got_online(self):
        ClientRoutes._got_online(self)
        self._push_job.spawn(self._push)

    def _got_offline(self):
        self._push_job.kill()
        ClientRoutes._got_offline(self)

    def _push(self):
        pushed_seq = toolkit.Sequence()
        skiped_seq = toolkit.Sequence()
        volume = self._local.volume

        def push(request, seq):
            try:
                self._node.call(request)
            except Exception:
                _logger.exception('Cannot push %r, will postpone', request)
                skiped_seq.include(seq)
            else:
                pushed_seq.include(seq)

        for res in volume.resources:
            if volume.mtime(res) <= self._push_seq.mtime:
                continue

            _logger.debug('Check %r local cache to push', res)

            for guid, patch in volume[res].diff(self._push_seq, layer='local'):
                diff = {}
                diff_seq = toolkit.Sequence()
                post_requests = []
                for prop, meta, seqno in patch:
                    if 'blob' in meta:
                        request = Request(method='PUT', path=[res, guid, prop])
                        request.content_type = meta['mime_type']
                        request.content_length = os.stat(meta['blob']).st_size
                        request.content_stream = \
                                toolkit.iter_file(meta['blob'])
                        post_requests.append((request, seqno))
                    elif 'url' in meta:
                        request = Request(method='PUT', path=[res, guid, prop])
                        request.content_type = 'application/json'
                        request.content = meta
                        post_requests.append((request, seqno))
                    else:
                        value = meta['value']
                        if prop == 'layer':
                            value = list(set(value) - _LOCAL_LAYERS)
                        diff[prop] = value
                        diff_seq.include(seqno, seqno)
                if not diff:
                    continue
                if 'guid' in diff:
                    request = Request(method='POST', path=[res])
                    access = ACL.CREATE | ACL.WRITE
                else:
                    request = Request(method='PUT', path=[res, guid])
                    access = ACL.WRITE
                for name in diff.keys():
                    if not (volume[res].metadata[name].acl & access):
                        del diff[name]
                request.content_type = 'application/json'
                request.content = diff
                push(request, diff_seq)
                for request, seqno in post_requests:
                    push(request, [[seqno, seqno]])

        if not pushed_seq:
            if not self._push_seq.mtime:
                self._push_seq.commit()
            self.broadcast({'event': 'push'})
            return

        _logger.info('Pushed %r local cache', pushed_seq)

        self._push_seq.exclude(pushed_seq)
        if not skiped_seq:
            self._push_seq.stretch()
            if 'report' in volume:
                # No any decent reasons to keep fail reports after uploding.
                # TODO The entire offlile synchronization should be improved,
                # for now, it is possible to have a race here
                volume['report'].wipe()

        self._push_seq.commit()
        self.broadcast({'event': 'push'})


class _LocalRoutes(model.VolumeRoutes, Router):

    def __init__(self, volume):
        model.VolumeRoutes.__init__(self, volume)
        Router.__init__(self, self)

    def on_create(self, request, props, event):
        props['layer'] = tuple(props['layer']) + ('local',)
        model.VolumeRoutes.on_create(self, request, props, event)


class _NodeRoutes(SlaveRoutes, Router):

    def __init__(self, key_path, volume, localcast):
        SlaveRoutes.__init__(self, key_path, volume)
        Router.__init__(self, self)

        self.api_url = 'http://127.0.0.1:%s' % node.port.value
        self._localcast = localcast
        self._mounts = toolkit.Pool()
        self._jobs = coroutine.Pool()

        users = volume['user']
        if not users.exists(client.sugar_uid()):
            profile = client.sugar_profile()
            profile['guid'] = client.sugar_uid()
            users.create(profile)

        mountpoints.connect(_SYNC_DIRNAME,
                self.__found_mountcb, self.__lost_mount_cb)

    def preroute(self, op, request):
        request.principal = client.sugar_uid()

    def whoami(self, request, response):
        return {'roles': [], 'guid': client.sugar_uid()}

    def broadcast(self, event=None, request=None):
        SlaveRoutes.broadcast(self, event, request)
        self._localcast(event)

    def close(self):
        self.volume.close()

    def __repr__(self):
        return '<LocalNode path=%s api_url=%s>' % \
                (self.volume.root, self.api_url)

    def _sync_mounts(self):
        self._localcast({'event': 'sync_start'})

        for mountpoint in self._mounts:
            self._localcast({'event': 'sync_next', 'path': mountpoint})
            try:
                self._offline_session = self._offline_sync(
                        join(mountpoint, _SYNC_DIRNAME),
                        **(self._offline_session or {}))
            except Exception, error:
                _logger.exception('Failed to complete synchronization')
                self._localcast({'event': 'sync_abort', 'error': str(error)})
                self._offline_session = None
                raise

        if self._offline_session is None:
            _logger.debug('Synchronization completed')
            self._localcast({'event': 'sync_complete'})
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._offline_session)
            self._localcast({'event': 'sync_paused'})

    def __found_mountcb(self, path):
        self._mounts.add(path)
        if self._jobs:
            _logger.debug('Found %r sync mount, pool it', path)
        else:
            _logger.debug('Found %r sync mount, start synchronization', path)
            self._jobs.spawn(self._sync_mounts)

    def __lost_mount_cb(self, path):
        if self._mounts.remove(path) == toolkit.Pool.ACTIVE:
            _logger.warning('%r was unmounted, break synchronization', path)
            self._jobs.kill()


class _ResponseStream(object):

    def __init__(self, stream, on_fail_cb):
        self._stream = stream
        self._on_fail_cb = on_fail_cb

    def __hasattr__(self, key):
        return hasattr(self._stream, key)

    def __getattr__(self, key):
        return getattr(self._stream, key)

    def read(self, size=None):
        try:
            return self._stream.read(size)
        except (http.ConnectionError, httplib.IncompleteRead):
            self._on_fail_cb()
            raise
