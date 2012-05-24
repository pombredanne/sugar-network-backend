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
from os.path import isabs, exists
from gettext import gettext as _

import zerosugar
import sweets_recipe
import active_document as ad
from local_document import activities, cache, sugar, http, env, zeroconf
from active_toolkit import sockets, util, coroutine, enforce


_HOME_PROPS = {
        'context': [
            ad.ActiveProperty('keep',
                prefix='LK', typecast=bool, default=False),
            ad.ActiveProperty('keep_impl',
                prefix='LI', typecast=[0, 1, 2], default=0),
            ad.StoredProperty('position',
                typecast=[int], default=(-1, -1)),
            ],
        }

_COMMON_PROPS = {
        # prop_name: default_value
        'keep': False,
        'keep_impl': 0,
        }

# TODO Incremental timeout
_RECONNECTION_TIMEOUT = 3

_logger = logging.getLogger('local_document.mounts')


class Offline(Exception):
    pass


class Mounts(dict):

    def __init__(self, root, resources_path, events_callback=None):
        self.home_volume = ad.SingleVolume(root, resources_path, _HOME_PROPS)

        self['~'] = _LocalMount('~', self.home_volume, events_callback)
        if env.server_mode.value:
            self['/'] = self['~']
        else:
            self['/'] = _RemoteMount('/', self.home_volume, events_callback)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
        return self.get(mountpoint)

    def call(self, request, response):
        mountpoint = request.pop('mountpoint')
        mount = self[mountpoint]
        if request.get('cmd') == 'is_connected':
            return mount.connected
        else:
            return mount.call(request, response)

    def close(self):
        while self:
            __, mount = self.popitem()
            mount.close()
        self.home_volume.close()


class _Mount(object):

    def __init__(self, mountpoint, events_callback):
        self.mountpoint = mountpoint
        self._events_callback = events_callback

    def emit(self, event):
        callback = self._events_callback
        if callback is None:
            return
        if 'mountpoint' not in event:
            event['mountpoint'] = self.mountpoint
        try:
            callback(event)
        except Exception:
            util.exception(_logger, _('Failed to dispatch %r event'), event)


class _LocalMount(ad.ProxyCommands, _Mount):

    def __init__(self, mountpoint, volume, events_callback):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))
        _Mount.__init__(self, mountpoint, events_callback)
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
            return directory.stat_blob(guid, prop)

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
        self.emit(event)

        if 'props' not in event:
            return
        props = event.pop('props')

        found_commons = False
        for prop in _COMMON_PROPS.keys():
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
            self.emit(event)

    def _checkout(self, guid):
        for path in activities.checkins(guid):
            _logger.info(_('Checkout %r implementation from %r'), guid, path)
            shutil.rmtree(path)

    def _checkin(self, guid):
        for phase, __ in zerosugar.checkin('/', guid, 'activity'):
            # TODO Publish checkin progress
            if phase == 'failure':
                self.emit({
                    'event': 'alert',
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

    def __init__(self, mountpoint, volume, events_callback):
        ad.CommandsProcessor.__init__(self)
        _Mount.__init__(self, mountpoint, events_callback)
        self._home_volume = volume
        self._events_job = coroutine.spawn(self._events_listerner)
        self._connected = False

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
                reply = request.get('reply', [])
                for prop, default in _COMMON_PROPS.items():
                    if prop in reply:
                        patch[prop] = default
                        reply.remove(prop)
            elif command in (('POST', None), ('PUT', None)):
                for prop in _COMMON_PROPS.keys():
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
                        patch = directory.get(guid).properties(patch.keys())
                    result.update(patch)
                else:
                    for props in result['result']:
                        if directory.exists(props['guid']):
                            patch = directory.get(props['guid']).properties(
                                    patch.keys())
                        props.update(patch)
            else:
                if command == ('POST', None):
                    guid = result
                if directory.exists(guid):
                    directory.update(guid, patch)
                elif [True for prop, value in patch.items() if value]:
                    props = http.request('GET', ['context', guid])
                    props.update(patch)
                    props['author'] = [sugar.uid()]
                    directory.create_with_guid(guid, props)
                    for prop in ('icon', 'artifact_icon', 'preview'):
                        blob = self.get_blob('context', guid, prop)
                        if blob:
                            directory.set_blob(guid, prop, blob['path'])

        return result

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):
        path, mime_type = cache.get_blob(document, guid, prop)
        if path is None:
            return None
        return {'path': path, 'mime_type': mime_type}

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
            self.emit(event)
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

            _logger.info(_('Connected to remote server'))
            self._connected = True
            self.emit({'event': 'connect', 'document': '*'})

            try:
                while dispatch(conn):
                    pass
            except Exception:
                util.exception(_logger, _('Failed to dispatch remote event'))
            finally:
                _logger.info(_('Got disconnected from remote server'))
                self._connected = False
                self.emit({'event': 'disconnect', 'document': '*'})
