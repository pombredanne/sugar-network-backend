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

    def create(self, socket, mountpoint, **kwargs):
        return self[mountpoint].create(**kwargs)

    def update(self, socket, mountpoint, **kwargs):
        self[mountpoint].update(**kwargs)

    def get(self, socket, mountpoint, **kwargs):
        return self[mountpoint].get(**kwargs)

    def find(self, socket, mountpoint, **kwargs):
        return self[mountpoint].find(**kwargs)

    def delete(self, socket, mountpoint, **kwargs):
        self[mountpoint].delete(**kwargs)

    def get_blob(self, socket, mountpoint, **kwargs):
        return self[mountpoint].get_blob(**kwargs)

    def set_blob(self, socket, mountpoint, **kwargs):
        self[mountpoint].set_blob(socket, **kwargs)


class _Mount(object):

    def get_blob(self, resource, guid, prop):
        path, mime_type = cache.get_blob(resource, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}

    def set_blob(self, socket, resource, guid, prop, files=None, url=None):
        raise NotImplementedError()


class _OfflineMount(_Mount):

    def __init__(self, resources):
        self._resources = resources

    def create(self, resource, props):
        obj = self._resources[resource].create(props)
        return {'guid': obj.guid}

    def update(self, resource, guid, props):
        self._resources[resource].update(guid, props)

    def get(self, resource, guid, reply=None):
        if reply and 'keep' in reply:
            reply.remove('keep')
        obj = self._resources[resource](guid)
        return obj.properties(reply)

    def find(self, resource, reply=None, **params):
        if reply and 'keep' in reply:
            reply.remove('keep')
        result, total = self._resources[resource].find(reply=reply, **params)
        return {'total': total.value,
                'result': [i.properties(reply) for i in result],
                }

    def delete(self, resource, guid):
        self._resources[resource].delete(guid)

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
        self._resources = resources

    def create(self, resource, props):
        keep = props.get('keep')
        if keep is not None:
            del props['keep']

        response = http.request('POST', [resource], data=props,
                headers={'Content-Type': 'application/json'})
        guid = response['guid']

        if keep:
            cls = self._resources[resource]
            document = cls(**props)
            document.set('guid', guid, raw=True)
            document.post()

        return {'guid': guid}

    def update(self, resource, guid, props):
        keep = props.get('keep')
        if keep is not None:
            del props['keep']

        if props:
            http.request('PUT', [resource, guid], data=props,
                    headers={'Content-Type': 'application/json'})

        if keep is not None:
            cls = self._resources[resource]
            if keep != cls(guid).exists:
                if keep:
                    props = http.request('GET', [resource, guid])
                    props.pop('guid')
                    document = cls(raw=True, **props)
                    document.set('guid', guid, raw=True)
                    document.post()
                else:
                    cls.delete(guid)

    def delete(self, resource, guid):
        http.request('DELETE', [resource, guid])

        cls = self._resources[resource]
        if cls(guid).exists:
            cls.delete(guid)

    def get(self, resource, guid, reply=None):
        params = {}
        if reply:
            if 'keep' in reply:
                reply.remove('keep')
            params['reply'] = ','.join(reply)
        response = http.request('GET', [resource, guid], params=params)
        response['keep'] = self._resources[resource](guid).exists
        return response

    def find(self, resource, reply=None, **params):
        if reply:
            if 'keep' in reply:
                reply.remove('keep')
            params['reply'] = ','.join(reply)
        try:
            response = http.request('GET', [resource], params=params)
            for props in response['result']:
                props['keep'] = self._resources[resource](props['guid']).exists
            return response
        except Exception:
            util.exception(_('Failed to query resources'))
            return {'total': 0, 'result': []}

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
