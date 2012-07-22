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

import logging
from cStringIO import StringIO
from os.path import isdir, abspath

from sugar_network.client.bus import Client
from sugar_network.toolkit import sugar
from active_toolkit import enforce


_logger = logging.getLogger('sugar_network.objects')


class Object(object):

    def __init__(self, mountpoint, document, reply, guid=None, props=None,
            offset=None, **kwargs):
        self.mountpoint = mountpoint
        self.document = document
        self._reply = reply or []
        self._guid = guid
        self._props = props or {}
        self._blobs = {}
        self._dirty = set()
        self.offset = offset

        for prop, value in kwargs.items():
            self[prop] = value

    @property
    def guid(self):
        return self._guid

    def get(self, prop):
        if prop == 'guid':
            return self._guid
        result = self._props.get(prop)
        if result is None:
            enforce(prop in self._reply,
                    'Access to not requested %r property in %r from %r mount',
                    prop, self.document, self.mountpoint)
            self.fetch()
            result = self._props.get(prop)
        return result

    def fetch(self, props=None):
        enforce(self._guid, 'Object needs to be posted first')

        to_fetch = []
        for prop in (props or self._reply):
            if prop not in self._props:
                to_fetch.append(prop)
        if not to_fetch:
            return

        response = Client.call('GET', mountpoint=self.mountpoint,
                document=self.document, guid=self._guid, reply=to_fetch)
        response.update(self._props)
        self._props = response

    def post(self):
        if not self._dirty:
            return

        props = {}
        for i in self._dirty:
            props[i] = self._props.get(i)

        if self._guid:
            Client.call('PUT', mountpoint=self.mountpoint,
                    document=self.document, guid=self._guid, content=props,
                    content_type='application/json')
        else:
            props['user'] = [sugar.uid()]
            self._guid = Client.call('POST', mountpoint=self.mountpoint,
                    document=self.document, content=props,
                    content_type='application/json')

        self._dirty.clear()
        return self._guid

    def get_blob_path(self, prop):
        blob, is_path = self._get_blob(prop)
        if is_path:
            return blob['path'], blob['mime_type']
        else:
            return None, None

    def get_blob(self, prop):
        blob, is_path = self._get_blob(prop)
        if is_path:
            path = blob['path']
            if path is None:
                return _empty_blob
            enforce(not isdir(path), 'Requested BLOB is a dictionary')
            return _Blob(path, blob['mime_type'])
        elif blob is not None:
            return _StringIO(blob.encode('utf8'))
        else:
            return _empty_blob

    def upload_blob(self, prop, path, pass_ownership=False):
        enforce(self._guid, 'Object needs to be posted first')
        Client.call('PUT', 'upload_blob', mountpoint=self.mountpoint,
                document=self.document, guid=self._guid, prop=prop,
                path=abspath(path), pass_ownership=pass_ownership)

    def _get_blob(self, prop):
        blob = self._blobs.get(prop)
        if blob is None:
            blob = Client.call('GET', 'get_blob', mountpoint=self.mountpoint,
                    document=self.document, guid=self._guid, prop=prop)
            self._blobs[prop] = blob
        return blob, type(blob) is dict and 'path' in blob

    def __getitem__(self, prop):
        result = self.get(prop)
        enforce(result is not None, KeyError,
                'Property %r is absent in %r from %r mount',
                prop, self.document, self.mountpoint)
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', 'Property "guid" is read-only')
        if self._props.get(prop) == value:
            return
        self._props[prop] = value
        self._dirty.add(prop)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.post()


class _Blob(file):

    def __init__(self, path, mime_type):
        file.__init__(self, path)
        self.mime_type = mime_type


class _EmptyBlob(object):

    closed = True
    mime_type = 'application/octet-stream'

    def read(self, size=None):
        return ''

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class _StringIO(object):

    def __init__(self, *args, **kwargs):
        self._stream = StringIO(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


_empty_blob = _EmptyBlob()
