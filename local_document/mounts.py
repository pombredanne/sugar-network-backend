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

import zerosugar
import sweets_recipe
import active_document as ad
from local_document import activities, sugar, http, env, zeroconf
from active_toolkit import sockets, util, coroutine, enforce


# TODO Incremental timeout
_RECONNECTION_TIMEOUT = 3

_LOCAL_PROPS = {
        'keep': False,
        'keep_impl': 0,
        'position': (-1, -1),
        }

_logger = logging.getLogger('local_document.mounts')


class Offline(Exception):
    pass


class Mounts(dict):

    def __init__(self, home_volume):
        self.home_volume = home_volume
        self._subscriptions = {}

        self['~'] = _LocalMount('~', home_volume, self.publish)
        if env.server_mode.value:
            self['/'] = _StubMount('/')
        else:
            self['/'] = _RemoteMount('/', home_volume, self.publish)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
        return self.get(mountpoint)

    def call(self, request, response=None):
        mountpoint = request.pop('mountpoint')
        mount = self[mountpoint]
        if request.get('cmd') == 'is_connected':
            return mount.connected
        else:
            if response is None:
                response = ad.Response()
            return mount.call(request, response)

    def connect(self, callback, condition=None):
        self._subscriptions[callback] = condition or {}

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def publish(self, event):
        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) not in ('*', value):
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, _('Failed to dispatch %r'), event)

    def close(self):
        while self:
            __, mount = self.popitem()
            mount.close()
        self.home_volume.close()


class _Mount(object):

    def __init__(self, mountpoint):
        self.mountpoint = mountpoint


class _LocalMount(ad.ProxyCommands, _Mount):

    def __init__(self, mountpoint, volume, publish_cb):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))
        _Mount.__init__(self, mountpoint)
        self._publish = publish_cb
        volume.connect(self.__events_cb)

    @property
    def connected(self):
        return True

    def close(self):
        pass

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)

        if document == 'context' and prop == 'feed':
            return json.dumps(self._get_feed(request))
        else:
            return directory.stat_blob(guid, prop) or None

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

    def _get_feed(self, request):
        feed = {}

        for path in activities.checkins(request['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception:
                util.exception(_logger, _('Failed to read %r spec file'), path)
                continue

            if request.access_level == ad.Request.ACCESS_LOCAL:
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

    def __events_cb(self, event):
        event['mountpoint'] = self.mountpoint
        self._publish(event)

        if 'props' not in event:
            return
        props = event.pop('props')

        found_commons = False
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
            # These local properties exposed from "/" mount as well
            event['mountpoint'] = '/'
            event['event'] = 'update'
            self._publish(event)

    def _checkout(self, guid):
        for path in activities.checkins(guid):
            _logger.info(_('Checkout %r implementation from %r'), guid, path)
            shutil.rmtree(path)

    def _checkin(self, guid):
        for phase, __ in zerosugar.checkin('/', guid, 'activity'):
            # TODO Publish checkin progress
            if phase == 'failure':
                self._publish({
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


class _RemoteMount(ad.CommandsProcessor, _Mount):

    def __init__(self, mountpoint, volume, publish_cb):
        ad.CommandsProcessor.__init__(self)
        _Mount.__init__(self, mountpoint)
        self._home_volume = volume
        self._publish = publish_cb
        self._events_job = coroutine.spawn(self._events_listerner)
        self._connected = False
        self._seqno = {}
        self._remote_volume_guid = None

    @property
    def connected(self):
        return self._connected

    def close(self):
        if self._events_job is not None:
            self._events_job.kill()
            self._events_job = None

    def call(self, request, response):
        enforce(self.connected, Offline, _('No connection to server'))

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

        patch = {}
        result = None
        command = method, request.get('cmd')

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
            result = http.request(method, path,
                    data=request.content, params=request,
                    headers={'Content-Type': 'application/json'})

        if document == 'context' and patch:
            directory = self._home_volume['context']
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
                    props = http.request('GET', ['context', guid])
                    props.update(patch)
                    props['user'] = [sugar.uid()]
                    directory.create_with_guid(guid, props)
                    for prop in ('icon', 'artifact_icon', 'preview'):
                        blob = self.get_blob('context', guid, prop)
                        if blob:
                            directory.set_blob(guid, prop, blob['path'])

        _logger.debug('Called %r(%s): request=%r result=%r',
                command, ', '.join(path), request, result)

        return result

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):
        blob_path = join(env.local_root.value, 'cache', document, guid[:2],
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

    def _events_listerner(self):

        def connect(host):
            subscription = http.request('POST', [''],
                    params={'cmd': 'subscribe'},
                    headers={'Content-Type': 'application/json'})
            conn = sockets.SocketFile(coroutine.socket())
            conn.connect((host, subscription['port']))
            conn = sockets.SocketFile(conn)
            conn.write_message({'ticket': subscription['ticket']})
            return conn

        def dispatch(conn):
            coroutine.select([conn.fileno()], [], [])
            event = conn.read_message()
            if event is None:
                return False

            seqno = event.get('seqno')
            if seqno:
                self._seqno[event['document']] = seqno

            event['mountpoint'] = self.mountpoint
            self._publish(event)

            return True

        def configured_host():
            url = urlparse(env.api_url.value)
            yield url.hostname, env.api_url.value

        def discover_hosts():
            for host in zeroconf.browse_workstation():
                yield host, 'http://%s:8000' % host

        get_hosts = configured_host if env.api_url.value else discover_hosts
        while True:
            for host, url in get_hosts():
                env.api_url.value = url
                try:
                    _logger.debug('Connecting to %r remote server', url)
                    conn = connect(host)
                except Exception:
                    _logger.debug('Cannot connect to %r remote server', url)
                else:
                    break
            else:
                _logger.debug('Wait for %r seconds before trying to connect',
                        _RECONNECTION_TIMEOUT)
                coroutine.sleep(_RECONNECTION_TIMEOUT)
                continue

            self._connected = True
            try:
                self._publish({
                    'event': 'connect',
                    'mountpoint': self.mountpoint,
                    'document': '*',
                    })

                stat = http.request('GET', [], params={'cmd': 'stat'},
                        headers={'Content-Type': 'application/json'})
                for document, props in stat['documents'].items():
                    self._seqno[document] = props.get('seqno') or 0
                self._remote_volume_guid = stat.get('guid')

                _logger.info(_('Connected to %r remote server'),
                        self._remote_volume_guid)

                while dispatch(conn):
                    pass

            except Exception:
                util.exception(_logger, _('Failed to dispatch remote event'))
            finally:
                _logger.info(_('Got disconnected from remote server'))
                self._connected = False
                self._publish({
                    'event': 'disconnect',
                    'mountpoint': self.mountpoint,
                    'document': '*',
                    })


class _StubMount(_Mount):

    def __init__(self, mountpoint):
        _Mount.__init__(self, mountpoint)

    @property
    def connected(self):
        return False

    def close(self):
        pass

    def call(self, request, response):
        raise Offline(_('Mount is empty'))

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):
        raise Offline(_('Mount is empty'))

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        raise Offline(_('Mount is empty'))
