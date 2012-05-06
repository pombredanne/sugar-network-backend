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
from gevent import socket
from contextlib import contextmanager
from os.path import join, exists, dirname
from gettext import gettext as _

from gevent.queue import Queue

import zerosugar
import sweets_recipe
from local_document import ipc, env, activities
from local_document.socket import SocketFile
from local_document.cache import get_cached_blob
from sugar_network.objects import Object, Context
from sugar_network.cursor import Cursor
from active_document import util


_CONNECTION_POOL = 6

_logger = logging.getLogger('sugar_network')


class ServerError(RuntimeError):
    pass


class Client(object):
    """IPC class to get access from a client side.

    See http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/Client
    for detailed information.

    """

    def __init__(self, mountpoint):
        self._mountpoint = mountpoint
        self._conn = _Connection()
        self._resources = {}

    def close(self):
        if self._conn is not None:
            _logger.debug('Close connection')
            self._conn.close()
            self._conn = None

    def checkin(self, context):
        solution = zerosugar.make(self, context)
        for sel, __, __ in solution.walk():
            try:
                spec = sweets_recipe.Spec(root=sel.local_path)
            except Exception, error:
                util.exception(_logger,
                        _('Cannot checkin %r, failed to read spec file: %s'),
                        sel.interface, error)
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

    def launch(self, context, command='activity', args=None):
        """Launch context implementation.

        Function will call fork at the beginning. In forked process,
        it will try to choose proper implementation to execute and launch it.

        Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

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

    def __getattr__(self, name):
        """Class-like object to access to a resource or call a method.

        :param name:
            resource name started with capital char
        :returns:
            a class-like resource object

        """
        resource = self._resources.get(name)
        if resource is None:
            request = _Request(self._conn, self._mountpoint, name.lower())
            object_class = Context if name == 'Context' else Object
            resource = _Resource(request, object_class)
            self._resources[name] = resource

        return resource

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Connection(object):

    def __init__(self):
        self._pool = Queue(maxsize=_CONNECTION_POOL)
        self._pool_size = 0

    @contextmanager
    def socket_file(self):
        if self._pool_size >= self._pool.maxsize:
            conn = self._pool.get()
        else:
            _logger.debug('Open new IPC connection')
            self._pool_size += 1
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = socket.socket(socket.AF_UNIX)
            conn.connect(env.ensure_path('run', 'accept'))

        try:
            yield SocketFile(conn)
        finally:
            self._pool.put(conn)

    def close(self):
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                conn.close()
            except Exception:
                util.exception(_logger, _('Cannot close IPC connection'))


class _Request(object):

    def __init__(self, conn, mountpoint, resource):
        self._conn = conn
        self._mountpoint = mountpoint
        self._resource = resource

    @property
    def online(self):
        return self._mountpoint == '/'

    def local_get(self, guid, prop):
        path = join(env.local_root.value, 'local', self._resource,
                guid[:2], guid, prop)
        if exists(path):
            with file(path) as f:
                return json.load(f)

    def send(self, cmd, data=None, **request):
        request['mountpoint'] = self._mountpoint
        request['resource'] = self._resource
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

    def get_properties(self, guid, reply):
        if self.online:
            result = self.send('get', guid=guid, reply=reply)
        else:
            result = {}
            for prop in reply:
                result[prop] = self.local_get(guid, prop)
        return result

    def get_blob(self, guid, prop):
        cached = get_cached_blob(self._resource, guid, prop)
        if cached is not None:
            return cached
        else:
            response = self.send('get_blob', guid=guid, prop=prop)
            if not response:
                return None
            return response['path'], response['mime_type']

    def __repr__(self):
        return self._resource


class _Resource(object):

    def __init__(self, request, object_class):
        self._request = request
        self._object_class = object_class

    def cursor(self, query=None, order_by=None, reply=None, page_size=18,
            **filters):
        """Query resource objects.

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
        :param page_size:
            number of items in one cached page, there are might be several
            (at least two) pages
        :param filters:
            a dictionary of properties to filter resulting list

        """
        if not self._request.online:
            # Offline properties will be gotten directly
            reply = None
        return Cursor(self._request, self._object_class, query, order_by,
                reply, page_size, **filters)

    def delete(self, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        return self._request.send('delete', guid=guid)

    def __call__(self, guid=None, reply=None):
        return self._object_class(self._request, reply or [], guid)
