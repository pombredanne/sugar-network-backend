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
import logging
from os.path import isabs, exists, join, basename
from gettext import gettext as _

from sugar_network.zerosugar import injector
from sugar_network.toolkit import http, coroutine, exception, enforce
from sugar_network.toolkit.router import Request
from sugar_network.resources.volume import VolumeCommands
from sugar_network.client import journal
from sugar_network import db, client


_LOCAL_PROPS = frozenset(['favorite', 'clone'])

_logger = logging.getLogger('client.mounts')


class _Mount(object):

    def __init__(self):
        self.mountpoint = None
        self.publisher = None
        self.mounted = coroutine.Event()

    def __call__(self, response=None, **kwargs):
        request = Request(**kwargs)
        request.allow_redirects = True
        # pylint: disable-msg=E1101
        return self.call(request, response)

    @property
    def name(self):
        return basename(self.mountpoint)

    @property
    def private(self):
        return type(self) in (LocalMount, HomeMount)

    def set_mounted(self, value):
        if self.mounted.is_set() == value:
            return
        if value:
            self.mounted.set()
        else:
            self.mounted.clear()
        self.broadcast({
            'event': 'mount' if value else 'unmount',
            'mountpoint': self.mountpoint,
            'name': self.name,
            'private': self.private,
            })

    def broadcast(self, event):
        if self.publisher is not None:
            # pylint: disable-msg=E1102
            self.publisher(event)


class LocalMount(VolumeCommands, _Mount):

    def __init__(self, volume):
        VolumeCommands.__init__(self, volume)
        _Mount.__init__(self)

        volume.connect(self._events_cb)

    @db.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(db.ACCESS_WRITE)
        enforce(isabs(path), 'Path is not absolute')
        try:
            directory.set_blob(guid, prop, path)
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def url(self, *path):
        enforce(self.mounted.is_set(), 'Not mounted')
        api_url = 'http://localhost:%s' % client.ipc_port.value
        return '/'.join((api_url,) + path)

    def _events_cb(self, event):
        event['mountpoint'] = self.mountpoint
        self.broadcast(event)


class HomeMount(LocalMount):

    @property
    def name(self):
        return _('Home')

    @db.directory_command(method='POST', cmd='create_with_guid',
            permissions=db.ACCESS_AUTH, mime_type='application/json')
    def create_with_guid(self, request):
        with self._post(request, db.ACCESS_CREATE) as (directory, doc):
            enforce('guid' in doc.props, 'GUID should be specified')
            self.before_create(request, doc.props)
            return directory.create(doc.props)

    def _events_cb(self, event):
        if event.get('event') == 'update':
            props = event.get('props')
            if props and set(props.keys()) & _LOCAL_PROPS:
                # _LOCAL_PROPS are common for `~` and `/` mountpoints
                event['mountpoint'] = '/'
                self.broadcast(event)
        LocalMount._events_cb(self, event)


class _ProxyCommands(object):
    # pylint: disable-msg=E1101

    def __init__(self, home_mount):
        self._home_volume = home_mount

    def proxy_call(self, request, response):
        raise db.CommandNotFound()

    @db.directory_command(method='GET',
            arguments={'reply': db.to_list}, mime_type='application/json')
    def find(self, request, response, reply):
        return self._proxy_get(request, response)

    @db.document_command(method='GET',
            arguments={'reply': db.to_list}, mime_type='application/json')
    def get(self, request, response):
        return self._proxy_get(request, response)

    def _proxy_get(self, request, response):
        document = request['document']
        reply = request.get('reply')
        mixin = set(reply or []) & _LOCAL_PROPS
        if mixin:
            # Otherwise there is no way to mixin _LOCAL_PROPS
            if 'guid' not in request and 'guid' not in reply:
                reply.append('guid')
            if document == 'context' and 'type' not in reply:
                reply.append('type')

        result = self.proxy_call(request, response)
        if not mixin:
            return result

        request_guid = request.get('guid')
        if request_guid:
            items = [result]
        else:
            items = result['result']

        def mixin_jobject(props, guid):
            if 'clone' in mixin:
                props['clone'] = 2 if journal.exists(guid) else 0
            if 'favorite' in mixin:
                props['favorite'] = bool(int(journal.get(guid, 'keep') or 0))

        if document == 'context':
            contexts = self._home_volume['context']
            for props in items:
                guid = request_guid or props['guid']
                if 'activity' in props['type']:
                    if contexts.exists(guid):
                        patch = contexts.get(guid).properties(mixin)
                    else:
                        patch = dict([(i, contexts.metadata[i].default)
                                for i in mixin])
                    props.update(patch)
                elif 'content' in props['type']:
                    mixin_jobject(props, guid)
        elif document == 'artifact':
            for props in items:
                mixin_jobject(props, request_guid or props['guid'])

        return result


class RemoteMount(db.CommandsProcessor, _Mount, _ProxyCommands):

    @property
    def name(self):
        return _('Network')

    def url(self, *path):
        enforce(self.mounted.is_set(), 'Not mounted')
        return '/'.join((self._url,) + path)

    def __init__(self, home_volume, listen_events=True):
        db.CommandsProcessor.__init__(self)
        _Mount.__init__(self)
        _ProxyCommands.__init__(self, home_volume)

        self._listen_events = listen_events
        self._client = None
        self._remote_volume_guid = None
        self._url = None
        self._api_urls = []
        if client.api_url.value:
            self._api_urls.append(client.api_url.value)
        self._connections = coroutine.Pool()

    def proxy_call(self, request, response):
        if client.layers.value and request.get('document') in \
                ('context', 'implementation') and \
                'layer' not in request:
            request['layer'] = client.layers.value
        return self._client.call(request, response)

    def call(self, request, response=None):
        for a_try in range(2):
            if not self.mounted.is_set():
                enforce(request.sync, 'Not mounted')
                self.set_mounted(True)
                _logger.debug('Wait for %s second(s) for remote connection',
                        client.connect_timeout.value)
                self.mounted.wait(client.connect_timeout.value)
            try:
                try:
                    return db.CommandsProcessor.call(self, request, response)
                except db.CommandNotFound:
                    return self.proxy_call(request, response)
            except http.ConnectionError:
                if a_try:
                    raise
                exception('Got connection error, try to reconnect')
                continue

    def set_mounted(self, value):
        if value != self.mounted.is_set():
            if value:
                self.mount()
            else:
                self._connections.kill()

    @db.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        enforce(isabs(path), 'Path is not absolute')

        try:
            with file(path, 'rb') as f:
                self._client.request('PUT', [document, guid, prop],
                        files={'file': f})
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def mount(self, url=None):
        if url and url not in self._api_urls:
            self._api_urls.append(url)
        if self._api_urls and not self.mounted.is_set() and \
                not self._connections:
            self._connections.spawn(self._connect)

    def _connect(self):
        for url in self._api_urls:
            try:
                _logger.debug('Connecting to %r node', url)
                self._client = client.Client(url)
                info = self._client.get(cmd='info')
                if self._listen_events:
                    subscription = self._client.subscribe()
            except Exception:
                exception(_logger, 'Cannot connect to %r node', url)
                continue

            impl_info = info['documents'].get('implementation')
            if impl_info:
                injector.invalidate_solutions(impl_info['mtime'])
            self._remote_volume_guid = info['guid']

            _logger.info('Connected to %r node', url)
            self._url = url
            _Mount.set_mounted(self, True)

            if not self._listen_events:
                break

            try:
                for event in subscription:
                    if event.get('document') == 'implementation':
                        mtime = event.get('props', {}).get('mtime')
                        if mtime:
                            injector.invalidate_solutions(mtime)
                    event['mountpoint'] = self.mountpoint
                    self.broadcast(event)
            except Exception:
                exception(_logger, 'Failed to dispatch remote event')
            finally:
                _logger.info('Got disconnected from %r node', url)
                _Mount.set_mounted(self, False)
                self._client.close()
                self._client = None


class NodeMount(LocalMount, _ProxyCommands):

    def __init__(self, volume, home_volume):
        LocalMount.__init__(self, volume)
        _ProxyCommands.__init__(self, home_volume)

        with file(join(volume.root, 'node')) as f:
            self._node_guid = f.read().strip()
        with file(join(volume.root, 'master')) as f:
            self._master_guid = f.read().strip()

    @property
    def node_guid(self):
        return self._node_guid

    @property
    def master_guid(self):
        return self._master_guid

    def proxy_call(self, request, response):
        return LocalMount.call(self, request, response)
