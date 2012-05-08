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

import logging
import urllib2
from gettext import gettext as _

import active_document as ad
from active_document import util, principal, SingleFolder, enforce
from local_document import cache, sugar, http


_logger = logging.getLogger('local_document.commands')


class Mounts(object):

    def __init__(self, resources_path):
        principal.user = sugar.uid()

        self._home_folder = SingleFolder(resources_path, {
            'context': [
                ad.ActiveProperty('keep',
                    prefix='LK', typecast=bool, default=False),
                ad.ActiveProperty('keep_impl',
                    prefix='LI', typecast=bool, default=False),
                ad.StoredProperty('position',
                    typecast=[int], default=(-1, -1)),
                ],
            })

        self._mounts = {
                '/': _OnlineMount(self._home_folder),
                '~': _OfflineMount(self._home_folder),
                }

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self._mounts,
                _('Unknown mountpoint %r'), mountpoint)
        return self._mounts[mountpoint]

    def call(self, socket, cmd, mountpoint, params):
        mount = self[mountpoint]

        enforce(hasattr(mount, cmd), _('Unknown %r command'), cmd)
        method = getattr(mount, cmd)
        enforce(hasattr(method, 'is_command'), _('Unknown %r command'), cmd)

        if hasattr(method, 'need_socket'):
            params['socket'] = socket
        return method(**params)

    def close(self):
        self._home_folder.close()

    def connect(self, callback):
        pass


def _command(func):
    func.is_command = True
    return func


def _socket_command(func):
    func.need_socket = True
    return func


class _Mount(object):

    def __init__(self, folder):
        self.folder = folder

    @_command
    def ping(self, hello=None):
        return 'pong: %s' % hello

    @_command
    def get_blob(self, resource, guid, prop):
        path, mime_type = cache.get_blob(resource, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}

    @_command
    def set_blob(self, socket, resource, guid, prop, files=None, url=None):
        raise NotImplementedError()


class _OfflineMount(_Mount):

    @_command
    def create(self, resource, props):
        obj = self.folder[resource].create(props)
        return obj.guid

    @_command
    def update(self, resource, guid, props):
        self.folder[resource].update(guid, props)

    @_command
    def get(self, resource, guid, reply=None):
        return self.folder[resource](guid).properties(reply)

    @_command
    def find(self, resource, reply=None, **params):
        result, total = self.folder[resource].find(reply=reply, **params)
        return {'total': total.value,
                'result': [i.properties(reply) for i in result],
                }

    @_command
    def delete(self, resource, guid):
        self.folder[resource].delete(guid)

    @_command
    @_socket_command
    def set_blob(self, socket, resource, guid, prop, files=None, url=None):
        if url:
            stream = urllib2.urlopen(url)
            try:
                cache.set_blob(resource, guid, prop, stream)
            finally:
                stream.close()
        elif files:
            raise RuntimeError('Not supported')
        else:
            cache.set_blob(resource, guid, prop, socket)


class _OnlineMount(_Mount):

    @_command
    def create(self, resource, props):
        response = http.request('POST', [resource], data=props,
                headers={'Content-Type': 'application/json'})
        return response['guid']

    @_command
    def update(self, resource, guid, props):
        if resource == 'context':
            keep = props.get('keep')
            if keep is not None:
                del props['keep']

        if props:
            http.request('PUT', [resource, guid], data=props,
                    headers={'Content-Type': 'application/json'})

        if keep is not None:
            home_obj = self.folder[resource](guid)
            if home_obj.exists:
                home_obj['keep'] = keep
                home_obj.post()
            elif keep:
                props = http.request('GET', [resource, guid])
                props.pop('guid')
                props['keep'] = keep
                home_obj.create_with_guid(guid, props)

    @_command
    def delete(self, resource, guid):
        http.request('DELETE', [resource, guid])

    @_command
    def get(self, resource, guid, reply=None):
        params = self._compose_params(resource, reply, {})
        return http.request('GET', [resource, guid], params=params)

    @_command
    def find(self, resource, reply=None, **params):
        params = self._compose_params(resource, reply, params)
        try:
            return http.request('GET', [resource], params=params)
        except Exception:
            util.exception(_('Failed to query resources'))
            return {'total': 0, 'result': []}

    @_command
    @_socket_command
    def set_blob(self, socket, resource, guid, prop, files=None, url=None):
        url_path = [resource, guid, prop]

        if url:
            http.request('PUT', url_path, params={'url': url})
        elif files:
            file_objects = {}
            try:
                for filename, path in files.items():
                    file_objects[filename] = file(path)
                http.request('PUT', url_path, files=file_objects)
            finally:
                for i in file_objects.values():
                    i.close()
        else:
            http.request('PUT', url_path, files={prop: socket})

    def _compose_params(self, resource, reply, params):
        if reply:
            if resource == 'context':
                if 'keep' in reply:
                    reply.remove('keep')
                if 'keep_impl' in reply:
                    reply.remove('keep_impl')
            params['reply'] = ','.join(reply)
        return params
