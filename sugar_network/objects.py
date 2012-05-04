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
from active_document import util, enforce


_logger = logging.getLogger('sugar_network')


class Object(object):

    def __init__(self, request, reply, guid=None, props=None, offset=None):
        self._request = request
        self._reply = reply or []
        self._guid = guid
        self._props = props or {}
        self._dirty = set()
        self.offset = offset

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
        to_fetch = []
        for prop in (props or self._reply):
            if prop not in self._props:
                to_fetch.append(prop)
        if not to_fetch:
            return
        enforce(self._guid, _('Object needs to be posted first'))
        response = self._request.get_properties(self._guid, to_fetch)
        response.update(self._props)
        self._props = response

    def post(self):
        if not self._dirty:
            return
        props = {}
        for i in self._dirty:
            props[i] = self._props.get(i)
        if self._guid:
            self._request.send('update', guid=self._guid, props=props)
        else:
            self._guid = self._request.send('create', props=props)
        self._dirty.clear()

    def get_blob_path(self, prop):
        enforce(self._guid, _('Object needs to be posted first'))
        blob = self._request.get_blob(self._guid, prop)
        if blob is None:
            return None
        path, __ = blob
        return path

    def get_blob(self, prop):
        enforce(self._guid, _('Object needs to be posted first'))
        blob = self._request.get_blob(self._guid, prop)
        if blob is None:
            return _empty_blob
        path, mime_type = blob
        enforce(not isdir(path), _('Requested BLOB is a dictionary'))
        return _Blob(path, mime_type)

    def set_blob(self, prop, data):
        enforce(self._guid, _('Object needs to be posted first'))
        kwargs = {}
        if type(data) is dict:
            kwargs['files'] = data
            data = None
        self._request.send('set_blob', guid=self._guid, data=data, prop=prop,
                **kwargs)

    def set_blob_by_url(self, prop, url):
        enforce(self._guid, _('Object needs to be posted first'))
        self._request.send('set_blob', guid=self._guid, prop=prop, url=url)

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
        if prop == 'keep' and prop not in self._props:
            self._props['keep'] = \
                    self._request.local_get(self._guid, 'keep') or False
        elif prop == 'keep_impl' and prop not in self._props:
            self._props['keep_impl'] = \
                    self._request.local_get(self._guid, 'keep_impl') or False
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
