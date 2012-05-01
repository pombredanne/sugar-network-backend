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

from active_document import util, principal, SingleFolder, enforce
from local_document import cache, sugar, http


_logger = logging.getLogger('local_document.commands')


class Mounts(object):

    def __init__(self, resources_path):
        principal.user = sugar.uid()

        self._folder = SingleFolder(resources_path)
        self._mounts = {
                '/': _OnlineMount(self._folder),
                '~': _OfflineMount(self._folder),
                }

    def __getitem__(self, mountpoint):
        enforce(mountpoint in self._mounts,
                _('Unknown mountpoint %r'), mountpoint)
        return self._mounts[mountpoint]

    def close(self):
        self._folder.close()

    def ping(self, socket, mountpoint, hello=None):
        return 'pong: %s' % hello

    def __getattr__(self, name):

        def functor(socket, mountpoint, **kwargs):
            mount = self[mountpoint]
            enforce(hasattr(mount, name), _('Unknown %r command'), name)
            attr = getattr(mount, name)
            enforce(hasattr(attr, 'is_command'), _('Unknown %r command'), name)
            return attr(**kwargs)

        return functor


def _command(func):
    func.is_command = True
    return func


class _Mount(object):

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

    def __init__(self, resources):
        self.resources = resources

    @_command
    def create(self, resource, props):
        obj = self.resources[resource].create(props)
        return {'guid': obj.guid}

    @_command
    def update(self, resource, guid, props):
        self.resources[resource].update(guid, props)

    @_command
    def get(self, resource, guid, reply=None):
        if reply and 'keep' in reply:
            reply.remove('keep')
        props = self.resources[resource](guid).properties(reply)
        props['keep'] = True
        return props

    @_command
    def find(self, resource, reply=None, **params):
        if reply and 'keep' in reply:
            reply.remove('keep')

        items, total = self.resources[resource].find(reply=reply, **params)

        result = []
        for obj in items:
            props = obj.properties(reply)
            props['keep'] = True
            result.append(props)

        return {'total': total.value, 'result': result}

    @_command
    def delete(self, resource, guid):
        self.resources[resource].delete(guid)

    @_command
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

    def __init__(self, resources):
        self.resources = resources

    @_command
    def create(self, resource, props):
        keep = props.get('keep')
        if keep is not None:
            del props['keep']

        response = http.request('POST', [resource], data=props,
                headers={'Content-Type': 'application/json'})
        guid = response['guid']

        if keep:
            self.resources[resource].create_with_guid(guid, props)

        return {'guid': guid}

    @_command
    def update(self, resource, guid, props):
        keep = props.get('keep')
        if keep is not None:
            del props['keep']

        if props:
            http.request('PUT', [resource, guid], data=props,
                    headers={'Content-Type': 'application/json'})

        if keep is not None:
            cls = self.resources[resource]
            if keep != cls(guid).exists:
                if keep:
                    props = http.request('GET', [resource, guid])
                    props.pop('guid')
                    cls.create_with_guid(guid, props)
                else:
                    cls.delete(guid)

    @_command
    def delete(self, resource, guid):
        http.request('DELETE', [resource, guid])

        cls = self.resources[resource]
        if cls(guid).exists:
            cls.delete(guid)

    @_command
    def get(self, resource, guid, reply=None):
        params = {}
        if reply:
            if 'keep' in reply:
                reply.remove('keep')
            params['reply'] = ','.join(reply)
        response = http.request('GET', [resource, guid], params=params)
        response['keep'] = self.resources[resource](guid).exists
        return response

    @_command
    def find(self, resource, reply=None, **params):
        if reply:
            if 'keep' in reply:
                reply.remove('keep')
            params['reply'] = ','.join(reply)
        try:
            response = http.request('GET', [resource], params=params)
            for props in response['result']:
                props['keep'] = self.resources[resource](props['guid']).exists
            return response
        except Exception:
            util.exception(_('Failed to query resources'))
            return {'total': 0, 'result': []}

    @_command
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
