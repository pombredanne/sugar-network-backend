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
from gevent import socket
from cStringIO import StringIO
from gettext import gettext as _

from local_document import env, util, enforce


_QUERY_PAGE_SIZE = 16
_QUERY_PAGES_NUMBER = 5


_logger = logging.getLogger('local_document.ipc_client')


class Client(object):

    def __init__(self):
        self._conn = _Connection()

    def __getattr__(self, name):
        """Get class-like object to access to the server resource."""
        return _Resource(_Request(self._conn, resource=name.lower()))

    def close(self):
        self._conn.close()


class _Connection(dict):

    def __init__(self):
        self._file = None

    @property
    def socket_file(self):
        if self._file is None:
            env.rendezvous()

            # pylint: disable-msg=E1101
            socket_ = socket.socket(socket.AF_UNIX)
            try:
                socket_.connect(env.ipc_path('accept'))
                self._file = socket_.makefile()
            finally:
                socket_.close()

        return self._file

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None


class _Request(dict):

    def __init__(self, conn, **kwargs):
        dict.__init__(self, kwargs or {})
        self._conn = conn

    def dup(self, **kwargs):
        result = _Request(self._conn)
        result.update(self)
        result.update(kwargs)
        return result

    def __call__(self, cmd, stream=None, **kwargs):
        request = self.copy()
        request.update(kwargs)
        request['cmd'] = cmd

        _logger.debug('Make a call: %r', request)

        self._conn.socket_file.write(json.dumps(request) + '\n')

        if stream is not None:
            sent = 0
            while True:
                chunk = stream.read(env.BUFSIZE)
                if not chunk:
                    break
                self._conn.socket_file.write(chunk)
                sent += len(chunk)
            _logger.debug('Sent %s bytes of payload', sent)

        self._conn.socket_file.flush()
        reply = self._conn.socket_file.readline()

        _logger.debug('Got a reply: %r', reply)

        return json.loads(reply)


class _Resource(object):

    def __init__(self, request):
        self._request = request

    @property
    def name(self):
        return self._request['resource']

    def find(self, *args, **kwargs):
        """Query resource objects.

        Function accpets the same arguments as `_Query.__init__()`.

        """
        return _Query(self._request, *args, **kwargs)

    def delete(self, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        return self._request('delete', guid=guid)

    def __call__(self, guid=None, reply=None, **filters):
        if guid:
            return _Object(self._request, {'guid': guid}, reply=reply)
        elif not filters:
            return _Object(self._request, reply=reply)
        else:
            query = self.find(**filters)
            enforce(query.total, KeyError, _('No objects found'))
            enforce(query.total == 1, _('Found more than one object'))
            return _Object(self._request, query[0], reply=reply)


class _Query(object):

    def __init__(self, request, query=None, order_by=None, reply=None,
            **filters):
        """
        :param request:
            _Request object
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
        self._request = request
        self._query = query
        self._order_by = order_by
        self._reply = reply or ['guid']
        self._filters = filters
        self._total = None
        self._page_access = collections.deque([], _QUERY_PAGES_NUMBER)
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
            if not self._fetch_page(0):
                return 0
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
        page = offset / _QUERY_PAGE_SIZE
        offset -= page * _QUERY_PAGE_SIZE
        if page not in self._pages:
            if not self._fetch_page(page):
                return default
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
        offset = page * _QUERY_PAGE_SIZE

        params = {}
        if self._filters:
            params.update(self._filters)
        params['offset'] = offset
        params['limit'] = _QUERY_PAGE_SIZE
        if self._query:
            params['query'] = self._query
        if self._order_by:
            params['order_by'] = self._order_by
        if self._reply:
            params['reply'] = self._reply

        try:
            reply = self._request('find', **params)
            self._total = reply['total']
        except Exception:
            util.exception(_logger,
                    _('Failed to fetch query result: resource=%r query=%r'),
                    self._request['resource'], params)
            self._total = None
            return False

        result = [None] * len(reply['result'])
        for i, props in enumerate(reply['result']):
            result[i] = _Object(self._request, props, offset + i, self._reply)

        if not self._page_access or self._page_access[-1] != page:
            if len(self._page_access) == _QUERY_PAGES_NUMBER:
                del self._pages[self._page_access[0]]
            self._page_access.append(page)
        self._pages[page] = result

        return True

    def _reset(self):
        self._page_access.clear()
        self._pages.clear()
        self._total = None


class _Object(dict):

    def __init__(self, request, props=None, offset=None, reply=None):
        dict.__init__(self, props or {})

        self._request = request
        if 'guid' in self:
            self._request['guid'] = self['guid']
        self._got = False
        self._dirty = set()
        self._reply = reply

        self.offset = offset
        self._blobs = _Blobs(self._request)

    @property
    def blobs(self):
        enforce('guid' in self._request, _('Object needs to be posted first'))
        return self._blobs

    def post(self):
        if not self._dirty:
            return

        props = {}
        for i in self._dirty:
            props[i] = self[i]

        if 'guid' in self._request:
            self._request('update', props=props)
        else:
            reply = self._request('create', props=props)
            guid = reply['guid']
            dict.__setitem__(self, 'guid', guid)
            self._request['guid'] = guid

        self._dirty.clear()

    def __getitem__(self, prop):
        result = self.get(prop)
        if result is not None:
            return result

        if 'guid' in self._request and not self._got:
            kwargs = {}
            if self._reply and prop in self._reply:
                kwargs['reply'] = self._reply
            properties = self._request('get', **kwargs)

            self._got = True
            properties.update(self)
            self.update(properties)

        result = self.get(prop)
        enforce(result is not None, KeyError,
                _('Property "%s" is absent in "%s" resource'),
                prop, self._request['resource'])
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', _('Property "guid" is read-only'))
        if self.get(prop) != value:
            self._dirty.add(prop)
        dict.__setitem__(self, prop, value)

    def __getattr__(self, command):

        def call(**kwargs):
            return self._request(command, **kwargs)

        enforce('guid' in self._request, _('Object needs to be posted first'))
        return call

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.post()


class _Blobs(object):

    def __init__(self, request):
        self._request = request

    def __getitem__(self, prop):
        return _Blob(self._request.dup(prop=prop))

    def __setitem__(self, prop, data):
        stream = None
        kwargs = {}
        if type(data) is dict:
            kwargs['files'] = data
        elif hasattr(data, 'read'):
            stream = data
        else:
            stream = StringIO(data)
        self._request('set_blob', stream=stream, prop=prop, **kwargs)


class _Blob(object):

    def __init__(self, request):
        self._request = request

    @property
    def content(self):
        reply = self._request('get_blob')
        if 'path' not in reply:
            return None

        with file(reply['path']) as f:
            if reply['mime_type'] == 'application/json':
                return json.load(f)
            else:
                return f.read()

    @property
    def path(self):
        reply = self._request('get_blob')
        return reply.get('path')

    def iter_content(self):
        reply = self._request('get_blob')
        if 'path' not in reply:
            return

        with file(reply['path']) as f:
            while True:
                chunk = f.read(env.BUFSIZE)
                if not chunk:
                    break
                yield chunk

    def _set_url(self, url):
        self._request('set_blob', url=url)

    #: Set BLOB value by url
    url = property(None, _set_url)