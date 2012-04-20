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

from local_document import storage, http


_logger = logging.getLogger('local_document.cp')


class CommandsProcessor(object):

    def ping(self, socket, hello=None):
        return 'pong: %s' % hello

    def create(self, socket, resource, props):
        reply = http.request('POST', [resource], data=props,
                headers={'Content-Type': 'application/json'})
        return {'guid': reply['guid']}

    def update(self, socket, resource, guid, props):
        http.request('PUT', [resource, guid], data=props,
                headers={'Content-Type': 'application/json'})

    def get(self, socket, resource, guid, reply=None):
        params = {}
        if reply:
            params['reply'] = ','.join(reply)
        reply = http.request('GET', [resource, guid], params=params)
        return reply

    def find(self, socket, resource, reply=None, **params):
        if reply:
            params['reply'] = ','.join(reply)
        reply = http.request('GET', [resource], params=params)
        return reply

    def delete(self, socket, resource, guid):
        http.request('DELETE', [resource, guid])

    def get_blob(self, socket, resource, guid, prop):
        path, mime_type = storage.get_blob(resource, guid, prop)
        return {'path': path, 'mime_type': mime_type}

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
