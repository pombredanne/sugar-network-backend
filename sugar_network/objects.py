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
from os.path import isdir
from gettext import gettext as _

import sweets_recipe
from local_document import activities
from local_document.cache import get_cached_blob
from active_document import util, enforce


_logger = logging.getLogger('sugar_network')


class Object(object):

    def __init__(self, request, reply, guid=None, props=None, offset=None,
            **kwargs):
        self._request = request
        self._reply = reply or []
        self._guid = guid
        self._props = props or {}
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
                    _('Access to not requested %r property in \'%s\''),
                    prop, self._request)
            self.fetch()
            result = self._props.get(prop)
        return result

    def fetch(self, props=None):
        enforce(self._guid, _('Object needs to be posted first'))

        to_fetch = []
        for prop in (props or self._reply):
            if prop not in self._props:
                to_fetch.append(prop)
        if not to_fetch:
            return

        if self._request.online:
            response = self._request.send('GET', guid=self._guid,
                    reply=to_fetch)
        else:
            response = {}
            for prop in to_fetch:
                response[prop] = self._request.local_get(self._guid, prop)

        response.update(self._props)
        self._props = response

    def post(self):
        if not self._dirty:
            return
        props = {}
        for i in self._dirty:
            props[i] = self._props.get(i)
        self._do_post(props)
        self._dirty.clear()
        return self._guid

    def get_blob_path(self, prop):
        enforce(self._guid, _('Object needs to be posted first'))
        cached = get_cached_blob(self._request.document, self._guid, prop)
        if cached is not None:
            return cached
        response = self._request.send('get_blob', guid=self._guid, prop=prop)
        if not response:
            return None, None
        return response['path'], response['mime_type']

    def get_blob(self, prop):
        path, mime_type = self.get_blob_path(prop)
        if path is None:
            return _empty_blob
        enforce(not isdir(path), _('Requested BLOB is a dictionary'))
        return _Blob(path, mime_type)

    def set_blob(self, prop, data):
        enforce(self._guid, _('Object needs to be posted first'))
        self._request.send('PUT', guid=self._guid, prop=prop, content=data,
                content_type='application/octet-stream')

    def set_blob_by_url(self, prop, url):
        enforce(self._guid, _('Object needs to be posted first'))
        self._request.send('PUT', guid=self._guid, prop=prop, url=url)

    def _do_post(self, props):
        if self._guid:
            self._request.send('PUT', guid=self._guid, content=props,
                    content_type='application/json')
        else:
            self._guid = self._request.send('POST', content=props,
                    content_type='application/json')

    def __getitem__(self, prop):
        result = self.get(prop)
        enforce(result is not None, KeyError,
                _('Property %r is absent in \'%s\' resource'),
                prop, self._request)
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', _('Property "guid" is read-only'))
        if self._props.get(prop) == value:
            return
        self._props[prop] = value
        self._dirty.add(prop)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.post()


class Context(Object):

    def get(self, prop):
        if self._request.online:
            if prop == 'keep' and prop not in self._props:
                self._props['keep'] = \
                        self._request.local_get(self._guid, 'keep') or False
            elif prop == 'keep_impl' and prop not in self._props:
                self._props['keep_impl'] = \
                        self._request.local_get(self._guid, 'keep_impl') or \
                        False
        return Object.get(self, prop)

    @property
    def checkins(self):
        enforce(self._guid, _('Object needs to be posted first'))

        for path in activities.checkins(self._guid):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception, error:
                util.exception(_logger, _('Failed to read %r spec file: %s'),
                        path, error)
                continue
            yield spec

    def _do_post(self, props):
        if self._request.online and 'keep' in props:
            enforce(self._guid, _('Object needs to be posted first'))
            self._request.send('set_keep', guid=self._guid,
                    keep=props.pop('keep'))
            if not props:
                return
        Object._do_post(self, props)


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


_empty_blob = _EmptyBlob()
