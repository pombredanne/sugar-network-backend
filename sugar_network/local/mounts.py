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
import shutil
import logging
from os.path import isabs, exists, join, basename, isdir
from gettext import gettext as _

import active_document as ad
from sugar_network.zerosugar import Bundle
from sugar_network.local import activities, cache
from sugar_network.zerosugar import Spec
from sugar_network.resources.volume import Request, VolumeCommands
from sugar_network import local, checkin, sugar, Client
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

    @ad.property_command(method='GET', cmd='get_blob',
            mime_type='application/json')
    def get_blob(self, document, guid, prop, request=None):
        directory = self.volume[document]
        prop = directory.metadata[prop]
        prop.assert_access(ad.ACCESS_READ)
        doc = directory.get(guid)
        return prop.on_get(doc, doc.meta(prop.name))

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

    @ad.document_command(method='GET', cmd='feed',
            mime_type='application/json')
    def feed(self, guid):
        result = []

        for path in activities.checkins(guid):
            try:
                spec = Spec(root=path)
            except Exception:
                util.exception('Failed to read %r spec file', path)
                continue

            result.append({
                'guid': spec.root,
                'version': spec['version'],
                'arch': '*-*',
                'stability': 'stable',
                'commands': {
                    'activity': {
                        'exec': spec['Activity', 'exec'],
                        },
                    },
                'requires': spec.requires,
                })

        enforce(result, 'No versions')
        return result

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

    @ad.property_command(method='GET', cmd='get_blob',
            mime_type='application/json')
    def get_blob(self, document, guid, prop, request=None):
        if document == 'implementation' and prop == 'data':
            path = activities.guid_to_path(guid)
            if exists(path):
                return {'path': path}
        else:
            return LocalMount.get_blob(self, document, guid, prop, request)

    def _events_cb(self, event):
        found_commons = False
        props = event.get('props')
        if props:
            for prop in _LOCAL_PROPS.keys():
                if prop not in props:
                    continue
                if prop == 'keep_impl':
                    if props[prop] == 0:
                        self._checkout(event['guid'])
                found_commons = True

        if not found_commons:
            # These local properties exposed from `_proxy_call` as well
            event['mountpoint'] = self.mountpoint
        self.publish(event)

    def _checkout(self, guid):
        for path in activities.checkins(guid):
            _logger.info('Checkout %r implementation from %r', guid, path)
            if isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)


class _ProxyCommands(object):
    # pylint: disable-msg=E1101

    def __init__(self, home_mount):
        self._home_volume = home_mount

    def _proxy_call(self, request, response, super_call):
        if 'document' not in request:
            return super_call(request, response)

        patch = {}
        result = None
        command = request['method'], request.get('cmd')
        document = request['document']
        guid = request.get('guid')

        if document == 'context':
            if command == ('GET', None):
                if 'reply' in request:
                    reply = request.get('reply', [])[:]
                    for prop, default in _LOCAL_PROPS.items():
                        if prop in reply:
                            patch[prop] = default
                            reply.remove(prop)
                    request['reply'] = reply
            elif command in (('POST', None), ('PUT', None)):
                for prop in _LOCAL_PROPS.keys():
                    if prop in request.content:
                        patch[prop] = request.content.pop(prop)
                if not request.content:
                    result = guid

        if result is None:
            result = super_call(request, response)

        if document == 'context' and patch:
            home = self._home_volume['context']
            if command == ('GET', None):
                if guid:
                    if home.exists(guid):
                        to_patch = home.get(guid).properties(patch.keys())
                    else:
                        to_patch = patch
                    result.update(to_patch)
                else:
                    for props in result['result']:
                        if home.exists(props['guid']):
                            to_patch = home.get(props['guid']).properties(
                                    patch.keys())
                        else:
                            to_patch = patch
                        props.update(to_patch)
            else:
                if command == ('POST', None):
                    guid = result

                to_checkin = False
                if 'keep_impl' in patch and \
                        (not home.exists(guid) or
                        patch['keep_impl'] != home.get(guid)['keep_impl']):
                    if patch['keep_impl']:
                        to_checkin = True
                        patch['keep_impl'] = 1

                if home.exists(guid):
                    home.update(guid, patch)
                elif [True for prop, value in patch.items() if value]:
                    copy = Request(method='GET', document='context', guid=guid,
                            reply=[
                                'type', 'implement', 'title', 'summary',
                                'description', 'homepage', 'mime_types',
                                'dependencies',
                                ])
                    copy.accept_language = request.accept_language
                    props = super_call(copy, ad.Response())
                    props.update(patch)
                    props['guid'] = guid
                    props['user'] = [sugar.uid()]
                    home.create(props)
                    for prop in ('icon', 'artifact_icon', 'preview'):
                        blob = self.get_blob('context', guid, prop)
                        if blob:
                            home.set_blob(guid, prop, blob['path'])

                if to_checkin:
                    self._checkin(guid)

        return result

    def _checkin(self, guid):
        for event in checkin(self.mountpoint, guid, 'activity'):
            # TODO Publish checkin progress
            if event['state'] == 'failure':
                self.publish({
                    'event': 'alert',
                    'mountpoint': self.mountpoint,
                    'severity': 'error',
                    'message': _('Cannot check-in %s implementation') % guid,
                    })
                for __ in activities.checkins(guid):
                    keep_impl = 2
                    break
                else:
                    keep_impl = 0
                self._home_volume['context'].update(guid,
                        {'keep_impl': keep_impl})
                break


class RemoteMount(ad.CommandsProcessor, _Mount, _ProxyCommands):

    @property
    def name(self):
        return _('Network')

    def __init__(self, home_volume):
        ad.CommandsProcessor.__init__(self)
        _Mount.__init__(self)
        _ProxyCommands.__init__(self, home_volume)

        self._client = None
        self._seqno = 0
        self._remote_volume_guid = None
        self._api_urls = []
        if local.api_url.value:
            self._api_urls.append(local.api_url.value)
        self._connections = coroutine.Pool()

    def call(self, request, response):

        def super_call(request, response):
            try:
                return ad.CommandsProcessor.call(self, request, response)
            except ad.CommandNotFound:
                if local.layers.value and request.get('document') in \
                        ('context', 'implementation') and \
                        'layer' not in request:
                    request['layer'] = local.layers.value
                return self._client.call(request, response)

        return self._proxy_call(request, response, super_call)

    def set_mounted(self, value):
        if value != self.mounted.is_set():
            if value:
                self.mount()
            else:
                self._connections.kill()

    @ad.property_command(method='GET', cmd='get_blob',
            mime_type='application/json')
    def get_blob(self, document, guid, prop):

        def download(path, seqno):
            return self._client.download([document, guid, prop], path, seqno,
                    document == 'implementation' and prop == 'data')

        return cache.get_blob(document, guid, prop, self._seqno,
                self._remote_volume_guid, download)

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

    @ad.property_command(method='GET',
            mime_type='application/json')
    def get_prop(self, document, guid, prop, response):
        directory = self._home_volume[document]
        prop = directory.metadata[prop]

        if not isinstance(prop, ad.BlobProperty):
            raise ad.CommandNotFound()

        meta = self.get_blob(document, guid, prop.name)
        enforce(meta is not None, ad.NotFound)
        response.content_type = meta['mime_type']
        return file(meta['path'], 'rb')

    def mount(self, url=None):
        if url and url not in self._api_urls:
            self._api_urls.append(url)
        if self._api_urls and not self.mounted.is_set() and \
                not self._connections:
            self._connections.spawn(self._connect)

    def _connect(self):
        for url in self._api_urls:
            try:
                _logger.debug('Connecting to %r master', url)
                self._client = Client(url)
                subscription = self._client.subscribe()
            except Exception:
                util.exception(_logger, 'Cannot connect to %r master', url)
                continue

            try:
                stat = self._client.get(cmd='stat')
                # pylint: disable-msg=E1103
                self._seqno = stat.get('seqno') or 0
                self._remote_volume_guid = stat.get('guid')

                _logger.info('Connected to %r master', url)
                _Mount.set_mounted(self, True)

                for event in subscription:
                    seqno = event.get('seqno')
                    if seqno:
                        self._seqno = seqno
                    event['mountpoint'] = self.mountpoint
                    self.publish(event)
            except Exception:
                util.exception(_logger, 'Failed to dispatch remote event')
            finally:
                _logger.info('Got disconnected from %r master', url)
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

    def call(self, request, response):
        return self._proxy_call(request, response, super(NodeMount, self).call)

    @ad.property_command(method='GET', cmd='get_blob',
            mime_type='application/json')
    def get_blob(self, document, guid, prop, request=None):
        meta = LocalMount.get_blob(self, document, guid, prop)
        if meta is None:
            return

        if document == 'implementation' and prop == 'data':

            def extract(path, seqno):
                with Bundle(meta['path'], 'application/zip') as bundle:
                    bundle.extractall(path)
                return meta['mime_type']

            return cache.get_blob(document, guid, prop, meta['seqno'],
                    self._node_guid, extract)

        return meta
