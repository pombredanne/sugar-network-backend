# Copyright (C) 2012, Aleksey Lim
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
from os.path import join, dirname, exists
from gettext import gettext as _

import gevent
from gevent import socket

import active_document as ad
from active_document import principal, SingleVolume, enforce
from local_document import cache, sugar, http
from local_document.socket import SocketFile


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
        # prop_name: (default_value, handler)
        'keep': (False, None),
        'keep_impl': (0, lambda guid, value: _set_keep_impl(guid, value)),
        }

_logger = logging.getLogger('local_document.mounts')


class Mounts(dict):

    def __init__(self, root, resources_path):
        principal.user = sugar.uid()
        self.home_volume = SingleVolume(root, resources_path, _HOME_PROPS)
        self['/'] = _RemoteMount('/', self.home_volume)
        self['~'] = _LocalMount('~', self.home_volume)

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self, _('Unknown mountpoint %r'), mountpoint)
        return self.get(mountpoint)

    def call(self, request, response):
        mount = self[request.pop('mountpoint')]
        return mount.call(request, response)

    def close(self):
        while self:
            __, mount = self.popitem()
            mount.close()
        self.home_volume.close()


class _LocalMount(object):

    def __init__(self, mountpoint, volume):
        self.mountpoint = mountpoint
        self._volume = volume

    def close(self):
        pass

    def call(self, request, response):
        if request.command == 'get_blob':
            return self._get_blob(**request)
        return ad.call(self._volume, request, response)

    def connect(self, callback):

        def signal_cb(event):
            props = None
            if 'props' in event:
                # For now, events are simply for notification
                # that some changes happened
                props = event.pop('props')

            event['mountpoint'] = self.mountpoint
            callback(self, event)

            if props is None:
                return

            if 'guid' not in event:
                # Creation event will contain GUID in `props`
                event['guid'] = props['guid']

            found_commons = False
            for prop, (__, handler) in _COMMON_PROPS.items():
                if prop in props:
                    found_commons = True
                    if handler is not None:
                        handler(event['guid'], props[prop])

            if found_commons:
                # These local properties exposed from "/" mount as well
                event['mountpoint'] = '/'
                callback(self, event)

        self._volume.connect(signal_cb)

    def _get_blob(self, document, guid, prop):
        stat = self._volume[document].stat_blob(guid, prop)
        if stat is None:
            return None
        return {'path': stat['path'], 'mime_type': stat['mime_type']}


class _RemoteMount(object):

    def __init__(self, mountpoint, volume):
        self.mountpoint = mountpoint
        self._home_volume = volume
        self._signal_job = None
        self._signal = None

    def close(self):
        if self._signal_job is not None:
            self._signal_job.kill()
            self._signal_job = None

    def call(self, request, response):
        if request.command == 'get_blob':
            return self._get_blob(**request)

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
                for prop, (default, __) in _COMMON_PROPS.items():
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
                    directory.create_with_guid(guid, props)

        return result

    def connect(self, callback):
        # TODO Replace by regular events handler
        enforce(self._signal is None)
        if self._signal_job is None:
            self._signal_job = gevent.spawn(self._signal_listerner)
        self._signal = callback

    def _signal_listerner(self):
        subscription = http.request('POST', [''], params={'cmd': 'subscribe'},
                headers={'Content-Type': 'application/json'})

        conn = SocketFile(socket.socket())
        conn.connect((subscription['host'], subscription['port']))
        conn = SocketFile(conn)
        conn.write_message({'ticket': subscription['ticket']})

        while True:
            socket.wait_read(conn.fileno())
            event = conn.read_message()
            event['mountpoint'] = self.mountpoint
            signal = self._signal
            if signal is not None:
                signal(self, event)

    def _get_blob(self, document, guid, prop):
        path, mime_type = cache.get_blob(document, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}


def _set_keep_impl(guid, value):
    if value == 0:
        _logger.debug('Checkout %r', guid)
        command = 'checkout'
    elif value == 1:
        _logger.debug('Checkin %r', guid)
        command = 'checkin'
    else:
        return

    pid = os.fork()
    if pid:
        return

    cmd = ['sugar-network', command, guid]

    cmd_path = join(dirname(__file__), '..', 'sugar-network')
    if exists(cmd_path):
        os.execv(cmd_path, cmd)
    else:
        os.execvp(cmd[0], cmd)
