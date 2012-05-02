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

import os
import json
import shutil
import logging
import collections
from gevent import socket
from contextlib import contextmanager
from os.path import join, dirname, exists, isdir
from gettext import gettext as _

from gevent.coros import Semaphore

import zerosugar
import sweets_recipe
from local_document import ipc, env, activities
from local_document.socket import SocketFile
from local_document.cache import get_cached_blob
from active_document import util, enforce


_QUERY_PAGE_SIZE = 16
_QUERY_PAGES_NUMBER = 5


_logger = logging.getLogger('local_document.client')


class ServerError(RuntimeError):
    pass


class Client(object):
    """IPC class to get access from a client side.

    See http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/Client
    for detailed information.

    """

    def __init__(self, mountpoint):
        self._conn = None
        self._mountpoint = mountpoint

    def close(self):
        if self._conn is not None:
            _logger.debug('Close connection')
            self._conn.close()
            self._conn = None

    def launch(self, context, command='activity', args=None):
        """Launch context implementation.

        Function will call fork at the beginning. In forked process,
        it will try to choose proper implementation to execute and launch it.

        Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

        :param context:
            context GUID to launch
        :param command:
            command that selected implementation should support
        :param args:
            optional list of arguments to pass to launching implementation
        :returns:
            child process pid

        """
        pid = os.fork()
        if pid:
            return pid

        cmd = ['sugar-network', '-C', command, 'launch', context] + \
                (args or [])

        cmd_path = join(dirname(__file__), '..', 'sugar-network')
        if exists(cmd_path):
            os.execv(cmd_path, cmd)
        else:
            os.execvp(cmd[0], cmd)

        exit(1)

    def checkin(self, context):
        solution = zerosugar.make(self, context)

        for sel, __, __ in solution.walk():
            try:
                spec = sweets_recipe.Spec(root=sel.local_path)
            except Exception, error:
                util.exception(_logger,
                        _('Cannot checkin %r, failed to read spec file: %s'),
                        self.interface, error)
                continue

            dst_path = util.unique_filename(
                    env.activities_root.value, spec['name'])
            _logger.info(_('Checkin %r implementation to %r'),
                    context, dst_path)
            util.cptree(sel.local_path, dst_path)

    def checkout(self, context):
        for path in activities.checkins(context):
            _logger.info(_('Checkout %r implementation from %r'),
                    context, path)
            shutil.rmtree(path)

    def __getattr__(self, name):
        """Class-like object to access to a resource or call a method.

        :param name:
            resource name (started with capital char) or method name
        :returns:
            if `name` starts with capital char, return a class-like
            resource object; otherwise, return a function-like object to call
            remote method that is not linked to specified resource

        """
        if self._conn is None:
            _logger.debug('Open connection')
            self._conn = _Connection()

        request = _Request(self._conn, mountpoint=self._mountpoint)

        if name[0].isupper():
            return _Resource(request.dup(resource=name.lower()))
        else:

            def call(**kwargs):
                return request(name, **kwargs)

            return call

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Connection(object):

    def __init__(self):
        self._socket_file = None
        self._lock = Semaphore()

    @contextmanager
    def socket_file(self):
        if self._socket_file is None:
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = socket.socket(socket.AF_UNIX)
            conn.connect(env.ensure_path('run', 'accept'))
            self._socket_file = SocketFile(conn)

        with self._lock:
            yield self._socket_file

    def close(self):
        if self._socket_file is None:
            return
        self._socket_file.close()
        self._socket_file = None


class _Request(dict):

    def __init__(self, conn, **kwargs):
        dict.__init__(self, kwargs or {})
        self._conn = conn

    def dup(self, **kwargs):
        result = _Request(self._conn)
        result.update(self)
        result.update(kwargs)
        return result

    def __call__(self, cmd, data=None, **kwargs):
        request = self.copy()
        request.update(kwargs)
        request['cmd'] = cmd

        with self._conn.socket_file() as socket_file:
            socket_file.write_message(request)
            if data is not None:
                socket_file.write(data)
            response = socket_file.read_message()

            _logger.debug('Made a call: request=%r response=%r',
                    request, response)

        if type(response) is dict and 'error' in response:
            raise ServerError(response['error'])

        return response


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

    def __call__(self, guid=None, **filters):
        if guid:
            return _Object(self._request, {'guid': guid})
        elif not filters:
            return _Object(self._request)
        else:
            query = self.find(**filters)
            enforce(query.total, KeyError, _('No objects found'))
            enforce(query.total == 1, _('Found more than one object'))
            return _Object(self._request, query[0])


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
        if request['mountpoint'] == '/':
            self._reply = reply
        else:
            self._reply = None
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
            response = self._request('find', **params)
            self._total = response['total']
        except Exception:
            util.exception(_logger,
                    _('Failed to fetch query result: resource=%r query=%r'),
                    self._request['resource'], params)
            self._total = None
            return False

        result = [None] * len(response['result'])
        for i, props in enumerate(response['result']):
            result[i] = _Object(self._request.dup(), props, offset + i)

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


class _Object(object):

    def __init__(self, request, props=None, offset=None):
        self._props = props or {}
        self._request = request
        if 'guid' in self._props:
            self._request['guid'] = props['guid']
        self._got = (request['mountpoint'] != '/')
        self._dirty = set()
        self._blobs = _Blobs(self._request)

        self.offset = offset

    @property
    def blobs(self):
        enforce('guid' in self._request, _('Object needs to be posted first'))
        return self._blobs

    def fetch(self):
        enforce('guid' in self._request, _('Object needs to be posted first'))
        if self._got:
            return

        props = self._request('get')
        props.update(self._props)
        self._props = props
        self._got = True

        if self._request['resource'] == 'context':
            guid = self._request['guid']
            self._props['keep'] = \
                    _local_get('~', 'context', guid, 'keep') or False
            self._props['keep_impl'] = \
                    _local_get('~', 'context', guid, 'keep_impl') or False

    def post(self):
        if not self._dirty:
            return

        props = {}
        for i in self._dirty:
            props[i] = self._props.get(i)

        if 'guid' in self._request:
            self._request('update', props=props)
        else:
            response = self._request('create', props=props)
            self._props['guid'] = self._request['guid'] = response['guid']

        self._dirty.clear()

    def get(self, prop):
        result = self._props.get(prop)
        if result is not None:
            return result

        enforce('guid' in self._request, _('Object needs to be posted first'))

        if self._request['mountpoint'] != '/':
            result = _local_get(prop=prop, **self._request)
            if result is not None:
                self._props[prop] = result
        else:
            self.fetch()
            result = self._props.get(prop)

        return result

    @property
    def checkins(self):
        enforce(self._request['resource'] == 'context')
        enforce('guid' in self._request, _('Object needs to be posted first'))

        for path in  activities.checkins(self['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception, error:
                util.exception(_logger, _('Failed to read %r spec file: %s'),
                        path, error)
                continue
            yield spec

    def __contains__(self, prop):
        return prop in self._props

    def __getitem__(self, prop):
        result = self.get(prop)
        enforce(result is not None, KeyError,
                _('Property %r is absent in %r resource in %r'),
                prop, self._request['resource'], self._request['guid'])
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', _('Property "guid" is read-only'))
        if self._props.get(prop) != value:
            self._dirty.add(prop)
        self._props[prop] = value

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
        cached = get_cached_blob(self._request['resource'],
                self._request['guid'], prop)
        if cached is not None:
            path, mime_type = cached
        else:
            response = self._request('get_blob', prop=prop)
            if not response:
                return None
            path = response['path']
            mime_type = response['mime_type']
        if isdir(path):
            return path
        else:
            return _Blob(path, mime_type)

    def __setitem__(self, prop, data):
        kwargs = {}
        if type(data) is dict:
            kwargs['files'] = data
            data = None
        self._request('set_blob', data=data, prop=prop, **kwargs)

    def set_by_url(self, prop, url):
        self._request('set_blob', prop=prop, url=url)


class _Blob(file):

    def __init__(self, path, mime_type):
        file.__init__(self, path)
        self.mime_type = mime_type


def _local_get(mountpoint, resource, guid, prop):
    enforce(mountpoint != '/', _('Remote mount is not directly accessible'))
    # XXX Will be broken if `mountpoint` ~= '~'
    path = join(env.local_root.value, 'local', resource, guid[:2], guid, prop)
    if exists(path):
        with file(path) as f:
            return json.load(f)
