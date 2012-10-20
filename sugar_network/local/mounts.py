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

import active_document as ad
from sugar_network.zerosugar import clones, injector
from sugar_network.resources.volume import Request, VolumeCommands
from sugar_network import local, sugar, Client
from active_toolkit import util, coroutine, enforce


_LOCAL_PROPS = {
        'keep': False,
        'keep_impl': 0,
        'position': (-1, -1),
        }

_logger = logging.getLogger('local.mounts')


class _Mount(object):

    def __init__(self):
        self.mountpoint = None
        self.publisher = None
        self.mounted = coroutine.Event()

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
        self.publish({
            'event': 'mount' if value else 'unmount',
            'mountpoint': self.mountpoint,
            'name': self.name,
            'private': self.private,
            })

    def publish(self, event):
        if self.publisher is not None:
            # pylint: disable-msg=E1102
            self.publisher(event)


class LocalMount(VolumeCommands, _Mount):

    def __init__(self, volume):
        VolumeCommands.__init__(self, volume)
        _Mount.__init__(self)

        volume.connect(self._events_cb)

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_WRITE)
        enforce(isabs(path), 'Path is not absolute')
        try:
            directory.set_blob(guid, prop, path)
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def url(self, *path):
        enforce(self.mounted.is_set(), 'Not mounter')
        api_url = 'http://localhost:%s' % local.ipc_port.value
        return '/'.join((api_url,) + path)

    def before_create(self, request, props):
        props['user'] = [sugar.uid()]
        props['author'] = [sugar.nickname()]
        VolumeCommands.before_create(self, request, props)

    def _events_cb(self, event):
        event['mountpoint'] = self.mountpoint
        self.publish(event)


class HomeMount(LocalMount):

    @property
    def name(self):
        return _('Home')

    @ad.directory_command(method='POST', cmd='create_with_guid',
            permissions=ad.ACCESS_AUTH, mime_type='application/json')
    def create_with_guid(self, request):
        with self._post(request, ad.ACCESS_CREATE) as (directory, doc):
            enforce('guid' in doc.props, 'GUID should be specified')
            self.before_create(request, doc.props)
            return directory.create(doc.props)

    def _events_cb(self, event):
        found_commons = False
        props = event.get('props')
        if props:
            for prop in _LOCAL_PROPS.keys():
                if prop not in props:
                    continue
                if prop == 'keep_impl':
                    if props[prop] == 0:
                        clones.wipeout(event['guid'])
                found_commons = True

        if not found_commons:
            # These local properties exposed from `_ProxyCommands` as well
            event['mountpoint'] = self.mountpoint
        self.publish(event)


class _ProxyCommands(object):
    # pylint: disable-msg=E1101

    def __init__(self, home_mount):
        self._home_volume = home_mount

    def proxy_call(self, request, response):
        raise ad.CommandNotFound()

    @ad.directory_command(method='GET',
            arguments={'reply': ad.to_list}, mime_type='application/json')
    def find(self, request, response, document, reply):
        if document != 'context':
            return self.proxy_call(request, response)

        if not reply:
            reply = request['reply'] = []
        else:
            # Do not modify original list
            reply = request['reply'] = request['reply'][:]

        mixin = {}
        for prop, default in _LOCAL_PROPS.items():
            if prop in reply:
                mixin[prop] = default
                reply.remove(prop)

        if not mixin:
            return self.proxy_call(request, response)

        if 'guid' not in reply:
            # GUID is needed to mixin local values
            reply.append('guid')
        result = self.proxy_call(request, response)

        if mixin:
            home = self._home_volume['context']
            for props in result['result']:
                if home.exists(props['guid']):
                    patch = home.get(props['guid']).properties(mixin.keys())
                else:
                    patch = mixin
                props.update(patch)

        return result

    @ad.document_command(method='GET',
            arguments={'reply': ad.to_list}, mime_type='application/json')
    def get(self, request, response, document, guid, reply):
        if document != 'context':
            return self.proxy_call(request, response)

        if reply:
            # Do not modify original list
            reply = request['reply'] = request['reply'][:]

        mixin = {}
        for prop, default in _LOCAL_PROPS.items():
            if not reply:
                mixin[prop] = default
            elif prop in reply:
                mixin[prop] = default
                reply.remove(prop)

        if not mixin:
            return self.proxy_call(request, response)

        if reply is None or reply:
            result = self.proxy_call(request, response)
        else:
            result = {}

        home = self._home_volume['context']
        if home.exists(guid):
            patch = home.get(guid).properties(mixin.keys())
        else:
            patch = mixin
        result.update(patch)

        return result

    @ad.property_command(method='GET', mime_type='application/json')
    def get_prop(self, request, response, document, guid, prop):
        if document == 'context' and prop in _LOCAL_PROPS:
            home = self._home_volume['context']
            if home.exists(guid):
                return home.get(guid)[prop]
            else:
                return _LOCAL_PROPS[prop]
        else:
            return self.proxy_call(request, response)

    @ad.directory_command(method='POST',
            permissions=ad.ACCESS_AUTH, mime_type='application/json')
    def create(self, request, response):
        return self._proxy_update(request, response)

    @ad.document_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update(self, request, response):
        self._proxy_update(request, response)

    @ad.property_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update_prop(self, request, response, prop):
        if prop not in _LOCAL_PROPS:
            self.proxy_call(request, response)
        else:
            request.content = {request.pop('prop'): request.content}
            self._proxy_update(request, response)

    def _proxy_update(self, request, response):
        if 'prop' in request or request['document'] != 'context':
            return self.proxy_call(request, response)

        home = self._home_volume['context']
        mixin = {}
        to_clone = False

        for prop in request.content.keys():
            if prop in _LOCAL_PROPS:
                mixin[prop] = request.content.pop(prop)

        if request['method'] == 'POST':
            guid = self.proxy_call(request, response)
        else:
            if request.content:
                self.proxy_call(request, response)
            guid = request['guid']

        if 'keep_impl' in mixin and (not home.exists(guid) or
                mixin['keep_impl'] != home.get(guid)['keep_impl']):
            if mixin['keep_impl']:
                to_clone = True
                mixin['keep_impl'] = 1

        if home.exists(guid):
            home.update(guid, mixin)
        elif [i for i in mixin.values() if i is not None]:
            copy = Request(method='GET', document='context', guid=guid,
                    reply=[
                        'type', 'implement', 'title', 'summary', 'description',
                        'homepage', 'mime_types', 'dependencies',
                        ])
            copy.accept_language = request.accept_language
            props = self.proxy_call(copy, ad.Response())
            props.update(mixin)
            props['guid'] = guid
            props['user'] = [sugar.uid()]
            home.create(props)
            for prop in ('icon', 'artifact_icon', 'preview'):
                copy['prop'] = prop
                blob = self.proxy_call(copy, ad.Response())
                if blob:
                    home.set_blob(guid, prop, blob)

        if to_clone:
            for event in injector.clone(self.mountpoint, guid):
                # TODO Publish clone progress
                if event['state'] == 'failure':
                    self.publish({
                        'event': 'alert',
                        'mountpoint': self.mountpoint,
                        'severity': 'error',
                        'message': _('Cannot clone %s implementation') % guid,
                        })
            for __ in clones.walk(guid):
                keep_impl = 2
                break
            else:
                keep_impl = 0
            self._home_volume['context'].update(guid, {'keep_impl': keep_impl})

        return guid


class RemoteMount(ad.CommandsProcessor, _Mount, _ProxyCommands):

    @property
    def name(self):
        return _('Network')

    def url(self, *path):
        enforce(self.mounted.is_set(), 'Not mounter')
        return '/'.join((self._url,) + path)

    def __init__(self, home_volume):
        ad.CommandsProcessor.__init__(self)
        _Mount.__init__(self)
        _ProxyCommands.__init__(self, home_volume)

        self._client = None
        self._remote_volume_guid = None
        self._url = None
        self._api_urls = []
        if local.api_url.value:
            self._api_urls.append(local.api_url.value)
        self._connections = coroutine.Pool()

    def proxy_call(self, request, response):
        if local.layers.value and request.get('document') in \
                ('context', 'implementation') and \
                'layer' not in request:
            request['layer'] = local.layers.value
        return self._client.call(request, response)

    def call(self, request, response=None):
        try:
            return ad.CommandsProcessor.call(self, request, response)
        except ad.CommandNotFound:
            return self.proxy_call(request, response)

    def set_mounted(self, value):
        if value != self.mounted.is_set():
            if value:
                self.mount()
            else:
                self._connections.kill()

    @ad.property_command(method='PUT', cmd='upload_blob')
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
                self._client = Client(url)
                subscription = self._client.subscribe()
            except Exception:
                util.exception(_logger, 'Cannot connect to %r node', url)
                continue

            try:
                stat = self._client.get(cmd='stat')
                if 'documents' in stat:
                    injector.invalidate_solutions(
                            stat['documents']['implementation']['mtime'])
                self._remote_volume_guid = stat['guid']

                _logger.info('Connected to %r node', url)
                self._url = url
                _Mount.set_mounted(self, True)

                for event in subscription:
                    if event.get('document') == 'implementation':
                        mtime = event.get('props', {}).get('mtime')
                        if mtime:
                            injector.invalidate_solutions(mtime)
                    event['mountpoint'] = self.mountpoint
                    self.publish(event)
            except Exception:
                util.exception(_logger, 'Failed to dispatch remote event')
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
