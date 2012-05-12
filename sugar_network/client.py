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
import logging
from contextlib import contextmanager
from os.path import join, exists, dirname
from gettext import gettext as _

import gevent
from gevent import socket
from gevent.queue import Queue

from local_document import ipc, env
from local_document.socket import SocketFile
from sugar_network.objects import Object
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

    @property
    def connected(self):
        request = _Request(self._conn, self._mountpoint, None)
        return request.send('is_connected')

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
            resource = _Resource(request)
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
        self._subscribe_job = None
        self._subscriptions = {}

    @contextmanager
    def conn_file(self):
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

    @property
    def subscriptions(self):
        if self._subscribe_job is None:
            self._subscribe_job = gevent.spawn(self._subscribe)
        return self._subscriptions

    def close(self):
        if self._subscribe_job is not None:
            self._subscribe_job.kill()
            self._subscribe_job = None

        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                conn.close()
            except Exception:
                util.exception(_logger, _('Cannot close IPC connection'))

    def _subscribe(self):
        # pylint: disable-msg=E1101
        conn = SocketFile(socket.socket(socket.AF_UNIX))
        conn.connect(env.ensure_path('run', 'subscribe'))
        try:
            while True:
                socket.wait_read(conn.fileno())
                event = conn.read_message()
                for callback, (mountpoint, document) in \
                        self._subscriptions.items():
                    if event['mountpoint'] == mountpoint and \
                            event['document'] == document:
                        callback(event)
        finally:
            conn.close()


class _Request(object):

    def __init__(self, conn, mountpoint, document):
        self._conn = conn
        self.mountpoint = mountpoint
        self.document = document

    @property
    def online(self):
        return self.mountpoint == '/'

    def connect(self, callback):
        self._conn.subscriptions[callback] = (self.mountpoint, self.document)

    def disconnect(self, callback):
        if callback in self._conn.subscriptions:
            del self._conn.subscriptions[callback]

    def local_get(self, guid, prop):
        path = join(env.local_root.value, 'local', self.document,
                guid[:2], guid, prop)
        if exists(path):
            with file(path) as f:
                return json.load(f)

    def send(self, cmd, content=None, content_type=None, **request):
        request['mountpoint'] = self.mountpoint
        if self.document:
            request['document'] = self.document
        request['cmd'] = cmd
        request['content_type'] = content_type

        with self._conn.conn_file() as conn_file:
            conn_file.write_message(request)
            if content_type == 'application/json':
                content = json.dumps(content)
            conn_file.write(content)
            response = conn_file.read_message()

            _logger.debug('Made a call: request=%r response=%r',
                    request, response)

        if type(response) is dict and 'error' in response:
            raise ServerError(response['error'])

        return response

    def __repr__(self):
        return str((self.mountpoint, self.document))


class _Resource(object):

    def __init__(self, request):
        self._request = request

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
        return Cursor(self._request, query, order_by, reply, page_size,
                **filters)

    def delete(self, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        return self._request.send('DELETE', guid=guid)

    def __call__(self, guid=None, reply=None, **kwargs):
        return Object(self._request, reply or [], guid, **kwargs)
