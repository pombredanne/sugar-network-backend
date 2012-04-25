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
from gettext import gettext as _

from active_document import util, principal

from local_document import storage, sugar, http


_logger = logging.getLogger('local_document.commands')


class _Commands(object):

    def ping(self, socket, hello=None):
        return 'pong: %s' % hello

    def get_blob(self, socket, resource, guid, prop):
        path, mime_type = storage.get_blob(resource, guid, prop)
        if path is None:
            return None
        else:
            return {'path': path, 'mime_type': mime_type}

    def set_blob(self, socket, resource, guid, prop, files=None,
            url=None):
        raise NotImplementedError()


class OfflineCommands(_Commands):

    def __init__(self, resources):
        self._resources = resources
        principal.user = sugar.uid()

    def create(self, socket, resource, props):
        obj = self._resources[resource].create(props)
        return {'guid': obj.guid}

    def update(self, socket, resource, guid, props):
        self._resources[resource].update(guid, props)

    def get(self, socket, resource, guid, reply=None):
        obj = self._resources[resource](guid)
        return obj.properties(reply)

    def find(self, socket, resource, reply=None, **params):
        result, total = self._resources[resource].find(reply=reply, **params)
        return {'total': total.value,
                'result': [i.properties(reply) for i in result],
                }

    def delete(self, socket, resource, guid):
        self._resources[resource].delete(guid)

    def set_blob(self, socket, resource, guid, prop, files=None,
            url=None):
        raise RuntimeError('Not supported')


class OnlineCommands(_Commands):

    def __init__(self, resources):
        self._resources = resources

    def create(self, socket, resource, props):
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

    def update(self, socket, resource, guid, props):
        keep = props.get('keep')
        if keep is not None:
            del props['keep']

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

    def delete(self, socket, resource, guid):
        http.request('DELETE', [resource, guid])

        cls = self._resources[resource]
        if cls(guid).exists:
            cls.delete(guid)

    def get(self, socket, resource, guid, reply=None):
        params = {}
        if reply:
            params['reply'] = ','.join(reply)
        response = http.request('GET', [resource, guid], params=params)
        response['keep'] = self._resources[resource](guid).exists
        return response

    def find(self, socket, resource, reply=None, **params):
        if reply:
            params['reply'] = ','.join(reply)
        try:
            response = http.request('GET', [resource], params=params)
            for props in response['result']:
                props['keep'] = self._resources[resource](props['guid']).exists
            return response
        except Exception:
            util.exception(_('Failed to query resources'))
            return {'total': 0, 'result': []}

    def set_blob(self, socket, resource, guid, prop, files=None,
            url=None):
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
