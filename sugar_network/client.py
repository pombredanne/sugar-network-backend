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

import json
import logging
import collections
from gettext import gettext as _

from sugar_network import sugar, cache, http
from sugar_network.util import enforce


_PAGE_SIZE = 16
_PAGE_NUMBER = 5
_CHUNK_SIZE = 1024 * 10

_logger = logging.getLogger('client')


def delete(resource, guid):
    http.request('DELETE', [resource, guid])


class Query(object):
    """Query resource objects."""

    def __init__(self, resource=None, query=None, order_by=None, reply=None,
            **filters):
        """
        :param resource:
            resource name to search in; if `None`, look for all resource types
        :param query:
            full text search query string in Xapian format
        :param order_by:
            name of property to sort by; might be prefixed by either `+` or `-`
            to change order's direction
        :param reply:
            list of property names to return for found objects;
            by default, only GUIDs will be returned; for missed properties,
            will be sent additional requests to a server on getting access
            to particular object.
        :param filters:
            a dictionary of properties to filter resulting list

        """
        self._path = []
        if resource:
            self._path.append(resource)
        self._resource = resource
        self._query = query
        self._order_by = order_by
        self._reply = reply or ['guid']
        self._filters = filters
        self._total = None
        self._page_access = collections.deque([], _PAGE_NUMBER)
        self._pages = {}
        self._offset = -1

        self._reset()

    # pylint: disable-msg=E1101,E0102,E0202
    @property
    def offset(self):
        """Current position in query results."""
        return self._offset

    @offset.setter
    def offset(self, value):
        """Change current position in query results."""
        self._offset = max(-1, value)

    @property
    def total(self):
        """Total number of objects."""
        if self._total is None:
            self._fetch_page(0)
        return self._total

    @property
    def order_by(self):
        """Current order of resulting list.

        Name of property to sort by. Might be prefixed by either `+` or `-`
        to change order's direction.

        """
        return self._order_by

    # pylint: disable-msg=E1101,E0102
    @order_by.setter
    def order_by(self, value):
        if self._order_by == value:
            return
        self._order_by = value
        self._reset()

    def __iter__(self):
        while self.offset + 1 < self.total:
            self.offset += 1
            obj = self.get(self.offset)
            if obj is None:
                break
            yield obj

    def filter(self, query=None, **filters):
        """Change query parameters.

        :param query:
            full text search query string in Xapian format
        :param filters:
            a dictionary of properties to filter resulting list

        """
        if query == self._query and filters == self._filters:
            return
        self._query = query
        self._filters = filters
        self._reset()

    def get(self, offset, default=None):
        """Get either object by offset or default value.

        :param offset:
            offset to get object for
        :param default:
            value to return if offset if not found
        :returns:
            `Object` value or `default`

        """
        if offset < 0 or self._total is not None and \
                (offset >= self._total):
            return default
        page = offset / _PAGE_SIZE
        offset -= page * _PAGE_SIZE
        if page not in self._pages:
            self._fetch_page(page)
        if offset >= len(self._pages[page]):
            total = page + len(self._pages[page])
            _logger.warning('Miscalculated total number, %s instead of %s',
                    total, self._total)
            self._total = total
            return default
        return self._pages[page][offset]

    def __getitem__(self, offset):
        """Get object by offset.

        :param offset:
            offset to get object for
        :returns:
            `Object` value or raise `KeyError` exception if offset is invalid

        """
        result = self.get(offset)
        enforce(result is not None, KeyError, _('Offset is out of range'))
        return result

    def _fetch_page(self, page):
        offset = page * _PAGE_SIZE

        params = {}
        if self._filters:
            params.update(self._filters)
        params['offset'] = offset
        params['limit'] = _PAGE_SIZE
        if self._query:
            params['query'] = self._query
        if self._order_by:
            params['order_by'] = self._order_by
        if self._reply:
            params['reply'] = ','.join(self._reply)

        reply = http.request('GET', self._path, params=params)
        self._total = reply['total']

        result = [None] * len(reply['result'])
        for i, props in enumerate(reply['result']):
            result[i] = Object(self._resource or props['document'],
                    props, offset + i)

        if not self._page_access or self._page_access[-1] != page:
            if len(self._page_access) == _PAGE_NUMBER:
                del self._pages[self._page_access[0]]
            self._page_access.append(page)
        self._pages[page] = result

    def _reset(self):
        self._page_access.clear()
        self._pages.clear()
        self._total = None


class Object(dict):

    #: Dictionary of `resource: {prop: (default, typecast)}` to cache
    #: properties in memory; it make sense for special cases like `vote`
    #: property that cannot be cached on server side
    memory_cache = {}

    __cache = {}

    def __init__(self, resource, props=None, offset=None, reply=None):
        dict.__init__(self, props or {})

        self._resource = resource
        self._path = None
        self._got = False
        self._dirty = set()
        self._reply = reply
        self._memory_cache = {}
        self._cache = None

        if resource in self.memory_cache:
            self._memory_cache = self.memory_cache[resource]
            self.__cache.setdefault(resource, {})
            self._cache = self.__cache[resource]

        self.offset = offset

        if 'guid' in self:
            self._path = [resource, self['guid']]

    @property
    def blobs(self):
        enforce(self._path is not None, _('Object needs to be posted first'))
        return _Blobs(self._path)

    def post(self):
        if not self._dirty:
            return

        data = {}
        for i in self._dirty:
            data[i] = self[i]
        if 'guid' in self:
            http.request('PUT', self._path, data=data,
                    headers={'Content-Type': 'application/json'})
        else:
            if 'author' in data:
                enforce(sugar.guid() in data['author'],
                        _('Current user should be in "author" property'))
            else:
                data['author'] = [sugar.guid()]
                dict.__setitem__(self, 'author', [sugar.guid()])
            reply = http.request('POST', [self._resource], data=data,
                    headers={'Content-Type': 'application/json'})
            self.update(reply)
            self._path = [self._resource, self['guid']]

        if self._memory_cache:
            self._update_cache()
        self._dirty.clear()

    def call(self, command, method='GET', **kwargs):
        enforce(self._path is not None, _('Object needs to be posted first'))
        kwargs['cmd'] = command
        return http.request(method, self._path, params=kwargs)

    def __getitem__(self, prop):
        result = self.get(prop)
        if result is not None:
            return result

        if self._path and not self._got:
            properties = self._fetch(prop)
            properties.update(self)
            self.update(properties)

        result = self.get(prop)
        enforce(result is not None, KeyError,
                _('Property "%s" is absent in "%s" resource'),
                prop, self._resource)
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', _('Property "guid" is read-only'))
        if self.get(prop) != value:
            self._dirty.add(prop)
        dict.__setitem__(self, prop, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.post()

    def _fetch(self, prop):
        if prop in self._memory_cache:
            cached = self._cache.get(self['guid'])
            if cached is not None:
                return cached

        params = None
        if self._reply and prop in self._reply:
            params = {'reply': ','.join(self._reply)}

        result = http.request('GET', self._path, params=params)
        self._got = True
        return result

    def _update_cache(self):
        cached = self._cache.get(self['guid'])
        if cached is None:
            cached = self._cache[self['guid']] = {}

        for prop, (default, typecast) in self._memory_cache.items():
            if prop not in self:
                value = default
            elif typecast is not None:
                value = typecast(self[prop])
            else:
                value = self[prop]
            cached[prop] = value


class Blob(object):

    def __init__(self, path):
        self._path = path

    @property
    def content(self):
        """Return entire BLOB value as a string."""
        path, mime_path = cache.get_blob(*self._path)
        with file(mime_path) as f:
            mime_type = f.read().strip()
        with file(path) as f:
            if mime_type == 'application/json':
                return json.load(f)
            else:
                return f.read()

    @property
    def path(self):
        """Return file-system path to file that contain BLOB value."""
        path, __ = cache.get_blob(*self._path)
        return path

    def iter_content(self):
        """Return BLOB value by poritons.

        :returns:
            generator that returns BLOB value by chunks

        """
        path, __ = cache.get_blob(*self._path)
        with file(path) as f:
            while True:
                chunk = f.read(_CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    def _set_url(self, url):
        http.request('PUT', self._path, params={'url': url})

    #: Set BLOB value by url
    url = property(None, _set_url)


class _Blobs(object):

    def __init__(self, path):
        self._path = path

    def __getitem__(self, prop):
        return Blob(self._path + [prop])

    def __setitem__(self, prop, data):
        headers = None
        if type(data) is dict:
            files = data
            data = None
        elif hasattr(data, 'read'):
            files = {prop: data}
            data = None
        else:
            files = None
            headers = {'Content-Type': 'application/octet-stream'}
        http.request('PUT', self._path + [prop], headers=headers,
                data=data, files=files)
