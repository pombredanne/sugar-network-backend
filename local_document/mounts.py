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
from os.path import isabs, exists
from gettext import gettext as _

import zerosugar
import sweets_recipe
import active_document as ad
from local_document import activities, cache, sugar, http, env
from local_document.sockets import SocketFile
from active_document import SingleVolume, util, coroutine, enforce


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


class Mounts(dict):

    def __init__(self, root, resources_path, events_callback=None):
        self.home_volume = SingleVolume(root, resources_path, _HOME_PROPS)

        self['~'] = _LocalMount('~', self.home_volume, events_callback)
        if env.server_mode.value:
            self['/'] = self['~']
        else:
            self['/'] = _RemoteMount('/', self.home_volume, events_callback)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
        return self.get(mountpoint)

    def call(self, request, response):
        mount = self[request.pop('mountpoint')]
        if request.command == 'is_connected':
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


class _LocalMount(_Mount):

    def __init__(self, mountpoint, volume, events_callback):
        _Mount.__init__(self, mountpoint, events_callback)
        self._volume = volume
        self._volume.connect(self.__events_cb)

    @property
    def connected(self):
        return True

    def close(self):
        pass

    def call(self, request, response):
        request.remote = False
        request.principal = None

        if request.command == 'upload_blob':
            return self._upload_blob(request, response)

        if request.command == 'get_blob':
            if request['document'] == 'context' and request['prop'] == 'feed':
                return self._get_feed(request, response)
            else:
                request.command = ('GET', 'stat-blob')

        return ad.call(self._volume, request, response)

    def _upload_blob(self, request, response):
        path = request.pop('path')
        pass_ownership = request.pop('pass_ownership')
        enforce(isabs(path), _('Path is not absolute'))

        try:
            request.command = 'PUT'
            request.content_stream = path
            return ad.call(self._volume, request, response)
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def _get_feed(self, request, response):
        feed = {}

        for path in activities.checkins(request['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception:
                util.exception(_logger, _('Failed to read %r spec file'), path)
                continue

            feed[spec['version']] = {
                    '*-*': {
                        'guid': spec.root,
                        'stability': 'stable',
                        'commands': {
                            'activity': {
                                'exec': spec['Activity', 'exec'],
                                },
                            },
                        },
                    }

        return json.dumps(feed)

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
                self._volume['context'].update(guid, {'keep_impl': keep_impl})
                break


class _RemoteMount(_Mount):

    def __init__(self, mountpoint, volume, events_callback):
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
        if request.command == 'get_blob':
            return self._get_blob(**request)
        elif request.command == 'upload_blob':
            return self._upload_blob(**request)

        enforce(self.connected, env.Offline, _('No connection to server'))

        if type(request.command) is list:
            method, request['cmd'] = request.command
        else:
            method = request.command

        document = request.pop('document')
        guid = None
        path = [document]
        if 'guid' in request:
            guid = request.pop('guid')
            path.append(guid)
        if 'prop' in request:
            path.append(request.pop('prop'))

        result = None
        patch = {}

        if document == 'context':
            if request.command == 'GET':
                reply = request.get('reply', [])
                for prop, default in _COMMON_PROPS.items():
                    if prop in reply:
                        patch[prop] = default
                        reply.remove(prop)
            elif request.command in ('POST', 'PUT'):
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
            if request.command == 'GET':
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
                if request.command == 'POST':
                    guid = result
                if directory.exists(guid):
                    directory.update(guid, patch)
                elif [True for prop, value in patch.items() if value]:
                    props = http.request('GET', ['context', guid])
                    props.update(patch)
                    props['author'] = [sugar.uid()]
                    directory.create_with_guid(guid, props)
                    for prop in ('icon', 'artifact_icon', 'preview'):
                        blob = self._get_blob('context', guid, prop)
                        if blob:
                            directory.set_blob(guid, prop, blob['path'])

        return result

    def _get_blob(self, document, guid, prop):
        path, mime_type = cache.get_blob(document, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}

    def _upload_blob(self, document, guid, prop, path, pass_ownership=False):
        try:
            with file(path, 'rb') as f:
                http.request('PUT', [document, guid, prop], files={'file': f})
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def _events_listerner(self):

        def connect():
            subscription = http.request('POST', [''],
                    params={'cmd': 'subscribe'},
                    headers={'Content-Type': 'application/json'})
            conn = SocketFile(coroutine.socket())
            conn.connect((subscription['host'], subscription['port']))
            conn = SocketFile(conn)
            conn.write_message({'ticket': subscription['ticket']})
            return conn

        def dispatch(conn):
            coroutine.select([conn.fileno()], [], [])
            event = conn.read_message()
            if event is None:
                return False
            self.emit(event)
            return True

        while True:
            try:
                _logger.debug('Connecting to %r remote server',
                        env.api_url.value)
                conn = connect()
            except Exception, error:
                _logger.debug('Cannot connect to remote server, ' \
                        'wait for %r seconds: %s',
                        _RECONNECTION_TIMEOUT, error)
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
