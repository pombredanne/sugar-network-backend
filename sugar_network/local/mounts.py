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
from os.path import isabs, exists, join
from gettext import gettext as _

import sweets_recipe
import active_document as ad
from sugar_network.toolkit import http
from sugar_network.local import activities
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
    def mounted(self):
        return self._mounted

    def set_mounted(self, value):
        if self._mounted == value:
            return
        self._mounted = value
        self.publish({
            'event': 'mount' if value else 'unmount',
            'mountpoint': self.mountpoint,
            'document': '*',
            })

    def publish(self, event):
        if self.publisher is not None:
            # pylint: disable-msg=E1102
            self.publisher(event)


class _LocalMount(_Mount):

    volume = None

    def __init__(self, volume):
        _Mount.__init__(self)
        volume.connect(self._events_cb)

    @ad.directory_command(method='POST',
            permissions=ad.ACCESS_AUTH)
    def create(self, document, request):
        request.content['user'] = [sugar.uid()]
        request.content['author'] = [sugar.nickname()]
        raise ad.CommandNotFound()

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)
        return directory.get(guid).meta(prop)

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_WRITE)
        enforce(isabs(path), _('Path is not absolute'))
        try:
            directory.set_blob(guid, prop, path)
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def _events_cb(self, event):
        if 'mountpoint' not in event:
            event['mountpoint'] = self.mountpoint
        self.publish(event)


class HomeMount(ad.ProxyCommands, _LocalMount):

    def __init__(self, volume):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))
        _LocalMount.__init__(self, volume)

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        if document == 'context' and prop == 'feed':
            return json.dumps(self._get_feed(request))
        else:
            return _LocalMount.get_blob(self, document, guid, prop, request)

    @ad.document_command(method='GET')
    def get(self, document, guid, request, response, reply=None):
        enforce(document == 'implementation', ad.CommandNotFound)
        return {'guid': guid,
                'context': '',
                'license': '',
                'version': '',
                'date': 0,
                'stability': 'stable',
                'notes': '',
                'url': '',
                }

    @ad.property_command(method='GET')
    def get_prop(self, document, guid, prop, request, response):
        if document == 'context' and prop == 'feed':
            directory = self.volume[document]
            directory.metadata[prop].assert_access(ad.ACCESS_READ)
            return self._get_feed(request)
        elif document == 'implementation' and prop == 'bundle':
            path = activities.guid_to_path(guid)
            if not exists(path):
                return None
            dir_info, dir_reader = sockets.encode_directory(path)
            response.content_length = dir_info.content_length
            response.content_type = dir_info.content_type
            return dir_reader
        else:
            raise ad.CommandNotFound()

    def _get_feed(self, request):
        feed = {}

        for path in activities.checkins(request['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception:
                util.exception(_logger, _('Failed to read %r spec file'), path)
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
        if 'props' in event:
            found_commons = False
            props = event['props']

            for prop in _LOCAL_PROPS.keys():
                if prop not in props:
                    continue
                if prop == 'keep_impl':
                    if props[prop] == 0:
                        self._checkout(event['guid'])
                    elif props[prop] == 1:
                        self._checkin(event['guid'])
                found_commons = True

            if found_commons:
                # These local properties exposed from _ProxyCommands as well
                event['mountpoint'] = '*'

        _LocalMount._events_cb(self, event)

    def _checkout(self, guid):
        for path in activities.checkins(guid):
            _logger.info(_('Checkout %r implementation from %r'), guid, path)
            shutil.rmtree(path)

    def _checkin(self, guid):
        for phase, __ in checkin('/', guid, 'activity'):
            # TODO Publish checkin progress
            if phase == 'failure':
                self.publish({
                    'event': 'alert',
                    'mountpoint': self.mountpoint,
                    'severity': 'error',
                    'message': _("Cannot check-in '%s' implementation") % guid,
                    })
                for __ in activities.checkins(guid):
                    keep_impl = 2
                    break
                else:
                    keep_impl = 0
                self.volume['context'].update(guid, {'keep_impl': keep_impl})
                break


class _ProxyCommands(ad.CommandsProcessor):

    def __init__(self, home_volume):
        ad.CommandsProcessor.__init__(self)
        self.home_volume = home_volume

    def super_call(self, request, response):
        raise NotImplementedError()

    def call(self, request, response):
        try:
            return ad.CommandsProcessor.call(self, request, response)
        except ad.CommandNotFound:
            pass

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
            result = self.super_call(request, response)

        if document == 'context' and patch:
            directory = self.home_volume['context']
            if command == ('GET', None):
                if guid:
                    if directory.exists(guid):
                        to_patch = directory.get(guid).properties(patch.keys())
                    else:
                        to_patch = patch
                    result.update(to_patch)
                else:
                    for props in result['result']:
                        if directory.exists(props['guid']):
                            to_patch = directory.get(props['guid']).properties(
                                    patch.keys())
                        else:
                            to_patch = patch
                        props.update(to_patch)
            else:
                if command == ('POST', None):
                    guid = result
                if directory.exists(guid):
                    directory.update(guid, patch)
                elif [True for prop, value in patch.items() if value]:
                    clone_request = ad.Request(method='GET',
                            document='context', guid=guid)
                    clone_request.accept_language = request.accept_language
                    props = self.super_call(clone_request, ad.Response())
                    props.update(patch)
                    props['user'] = [sugar.uid()]
                    directory.create_with_guid(guid, props)
                    for prop in ('icon', 'artifact_icon', 'preview'):
                        # pylint: disable-msg=E1101
                        blob = self.get_blob('context', guid, prop)
                        if blob:
                            directory.set_blob(guid, prop, blob['path'])

        return result


class RemoteMount(_ProxyCommands, _Mount):

    def __init__(self, home_volume):
        _ProxyCommands.__init__(self, home_volume)
        _Mount.__init__(self)

        self._seqno = {}
        self._remote_volume_guid = None
        self._api_urls = []
        if local.api_url.value:
            self._api_urls.append(local.api_url.value)
        self._connections = coroutine.Pool()

    def set_mounted(self, value):
        if value != self.mounted:
            if value:
                self.mount()
            else:
                self._connections.kill()

    def super_call(self, request, response):
        method = request.pop('method')
        document = request.pop('document')
        guid = request.pop('guid') if 'guid' in request else None
        prop = request.pop('prop') if 'prop' in request else None

        path = [document]
        if guid:
            path.append(guid)
        if prop:
            path.append(prop)

        return http.request(method, path, data=request.content, params=request,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Language': ','.join(request.accept_language),
                    })

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):
        blob_path = join(local.local_root.value, 'cache', document, guid[:2],
                guid, prop)
        meta_path = blob_path + '.meta'
        meta = {}

        def download(seqno):
            mime_type = http.download([document, guid, prop], blob_path, seqno,
                    document == 'implementation' and prop == 'bundle')
            meta['mime_type'] = mime_type
            meta['seqno'] = self._seqno[document]
            meta['volume'] = self._remote_volume_guid
            with file(meta_path, 'w') as f:
                json.dump(meta, f)

        if exists(meta_path):
            with file(meta_path) as f:
                meta = json.load(f)
            if meta.get('volume') != self._remote_volume_guid:
                download(None)
            elif meta.get('seqno') < self._seqno[document]:
                download(meta['seqno'])
        else:
            download(None)

        if not exists(blob_path):
            return None
        meta['path'] = blob_path
        return meta

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        enforce(isabs(path), _('Path is not absolute'))

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
            for document, props in stat['documents'].items():
                self._seqno[document] = props.get('seqno') or 0
            self._remote_volume_guid = stat.get('guid')

            _logger.info(_('Connected to %r master'), url)
            _Mount.set_mounted(self, True)

            while True:
                coroutine.select([conn.fileno()], [], [])
                event = conn.read_message()
                if event is None:
                    break

                seqno = event.get('seqno')
                if seqno:
                    self._seqno[event['document']] = seqno

                event['mountpoint'] = self.mountpoint
                self.publish(event)

        for url in self._api_urls:
            try:
                conn = connect(url)
            except Exception, error:
                _logger.warning('Cannot connect to %r master: %s', url, error)
                continue
            try:
                listen_events(url, conn)
            except Exception:
                util.exception(_logger, _('Failed to dispatch remote event'))
            finally:
                _logger.info(_('Got disconnected from %r master'), url)
                _Mount.set_mounted(self, False)


class LocalMount(ad.ProxyCommands, _LocalMount):

    def __init__(self, volume):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))
        _LocalMount.__init__(self, volume)


class LocalProxyMount(_ProxyCommands, _LocalMount):

    def __init__(self, volume, home_volume):
        _ProxyCommands.__init__(self, home_volume)
        _LocalMount.__init__(self, volume)

        self.volume = volume
        self._proxy = ad.VolumeCommands(volume)

    def super_call(self, request, response):
        return self._proxy.call(request, response)
