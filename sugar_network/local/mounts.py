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
import json
import shutil
import logging
from urlparse import urlparse
from os.path import isabs, exists, join, basename, isdir
from gettext import gettext as _

import sweets_recipe
import active_document as ad
from sweets_recipe import Bundle
from sugar_network.toolkit import sugar, http
from sugar_network.local import activities, cache
from sugar_network import local, checkin, sugar
from active_toolkit import sockets, util, coroutine, enforce


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
        self._mounted = False

    @property
    def name(self):
        return basename(self.mountpoint)

    @property
    def private(self):
        return type(self) in (LocalMount, HomeMount)

    @property
    def mounted(self):
        return self._mounted

    def set_mounted(self, value):
        if self._mounted == value:
            return
        self._mounted = value
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


class LocalMount(ad.VolumeCommands, _Mount):

    def __init__(self, volume):
        ad.VolumeCommands.__init__(self, volume)
        _Mount.__init__(self)

        volume.connect(self._events_cb)

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)
        return directory.get(guid).meta(prop)

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

    def before_create(self, request, props):
        props['user'] = [sugar.uid()]
        props['author'] = [sugar.nickname()]
        ad.VolumeCommands.before_create(self, request, props)

    def _events_cb(self, event):
        if 'mountpoint' not in event:
            event['mountpoint'] = self.mountpoint
        self.publish(event)


class HomeMount(LocalMount):

    @property
    def name(self):
        return _('Home')

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        if document == 'context' and prop == 'feed':
            return json.dumps(self._get_feed(request))
        elif document == 'implementation' and prop == 'data':
            path = activities.guid_to_path(guid)
            if exists(path):
                return {'path': path}
        else:
            return LocalMount.get_blob(self, document, guid, prop, request)

    def _get_feed(self, request):
        feed = {}

        for path in activities.checkins(request['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception:
                util.exception(_logger, 'Failed to read %r spec file', path)
                continue

            if request.access_level == ad.ACCESS_LOCAL:
                impl_id = spec.root
            else:
                impl_id = activities.path_to_guid(spec.root)

            feed[spec['version']] = {
                    '*-*': {
                        'guid': impl_id,
                        'stability': 'stable',
                        'commands': {
                            'activity': {
                                'exec': spec['Activity', 'exec'],
                                },
                            },
                        },
                    }

        return feed

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
                    clone = ad.Request(method='GET', document='context',
                            guid=guid)
                    clone.accept_language = request.accept_language
                    props = super_call(clone, ad.Response())
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
        for phase, __ in checkin(self.mountpoint, guid, 'activity'):
            # TODO Publish checkin progress
            if phase == 'failure':
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
                pass

            method = request.pop('method')
            document = request.pop('document')
            guid = request.pop('guid') if 'guid' in request else None
            prop = request.pop('prop') if 'prop' in request else None

            path = [document]
            if guid:
                path.append(guid)
            if prop:
                path.append(prop)

            return http.request(method, path, data=request.content,
                    params=request, headers={
                        'Content-Type': 'application/json',
                        'Accept-Language': ','.join(request.accept_language),
                        })

        return self._proxy_call(request, response, super_call)

    def set_mounted(self, value):
        if value != self.mounted:
            if value:
                self.mount()
            else:
                self._connections.kill()

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):

        def download(path, seqno):
            return http.download([document, guid, prop], path, seqno,
                    document == 'implementation' and prop == 'data')

        return cache.get_blob(document, guid, prop, self._seqno,
                self._remote_volume_guid, download)

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        enforce(isabs(path), 'Path is not absolute')

        try:
            with file(path, 'rb') as f:
                http.request('PUT', [document, guid, prop], files={'file': f})
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def mount(self, url=None):
        if url and url not in self._api_urls:
            self._api_urls.append(url)
        if self._api_urls and not self.mounted and not self._connections:
            self._connections.spawn(self._connect)

    def _connect(self):

        def connect(url):
            _logger.debug('Connecting to %r master', url)
            local.api_url.value = url
            subscription = http.request('POST', [''],
                    params={'cmd': 'subscribe'},
                    headers={'Content-Type': 'application/json'})
            conn = sockets.SocketFile(coroutine.socket())
            conn.connect((urlparse(url).hostname, subscription['port']))
            conn.write_message({'ticket': subscription['ticket']})
            return conn

        def listen_events(url, conn):
            stat = http.request('GET', [], params={'cmd': 'stat'},
                    headers={'Content-Type': 'application/json'})
            # pylint: disable-msg=E1103
            self._seqno = stat.get('seqno') or 0
            self._remote_volume_guid = stat.get('guid')

            _logger.info('Connected to %r master', url)
            _Mount.set_mounted(self, True)

            while True:
                coroutine.select([conn.fileno()], [], [])
                event = conn.read_message()
                if event is None:
                    break

                seqno = event.get('seqno')
                if seqno:
                    self._seqno = seqno

                event['mountpoint'] = self.mountpoint
                self.publish(event)

        for url in self._api_urls:
            try:
                conn = connect(url)
            except Exception:
                util.exception(_logger, 'Cannot connect to %r master', url)
                continue
            try:
                listen_events(url, conn)
            except Exception:
                util.exception(_logger, 'Failed to dispatch remote event')
            finally:
                _logger.info('Got disconnected from %r master', url)
                _Mount.set_mounted(self, False)


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

    @ad.property_command(method='GET', cmd='get_blob')
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
