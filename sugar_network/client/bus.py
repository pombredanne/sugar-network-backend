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

import json
import socket
import logging
from contextlib import contextmanager

import active_document as ad
from active_toolkit import util, coroutine, sockets
from sugar_network import local
from sugar_network.toolkit import ipc, sugar


_CONNECTION_POOL = 6

_logger = logging.getLogger('sugar_network')


class ServerError(RuntimeError):
    pass


class classproperty(property):

    def __get__(self, obj, type_):
        # pylint: disable-msg=E1101
        return self.fget.__get__(None, type_)()


class Client(object):
    """IPC class to get access from a client side.

    See http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/Client
    for detailed information.

    """
    _connection = None

    @classproperty
    @classmethod
    def connection(cls):
        if cls._connection is None:
            cls._connection = _Connection()
        return cls._connection

    @classmethod
    def close(cls):
        cls._connection = None

    @classmethod
    def call(cls, method, cmd=None, content=None, content_type=None, **kwargs):
        request = ad.Request(kwargs)
        request.access_level = ad.ACCESS_LOCAL
        request.principal = sugar.uid()
        request['method'] = method
        if cmd:
            request['cmd'] = cmd
        request.content = content
        request.content_type = content_type
        return cls.connection.call(request)

    @classmethod
    def publish(cls, event, **kwargs):
        kwargs['event'] = event
        cls.connection.publish(kwargs)

    @classmethod
    def connect(cls, callback, **condition):
        cls.connection.connect(callback, condition)

    @classmethod
    def disconnect(cls, callback):
        cls.connection.disconnect(callback)

    @classmethod
    def subscribe(cls):
        """Start subscription session.

        :returns:
            `SocketFile` object connected to IPC server to read events from

        """
        ipc.rendezvous()
        # pylint: disable-msg=E1101
        conn = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        conn.connect(local.ensure_path('run', 'subscribe'))
        return conn

    @classmethod
    def mounts(cls):
        return cls.call('GET', 'mounts')

    def __init__(self, mountpoint):
        self._mountpoint = mountpoint
        self._resources = {}

    @property
    def connected(self):
        _logger.warning('Client.connected is depecated, ' \
                'use Client.mounted instead')
        return self.call('GET', 'mounted', mountpoint=self._mountpoint)

    @property
    def mounted(self):
        return self.call('GET', 'mounted', mountpoint=self._mountpoint)

    def launch(self, context, command='activity', object_id=None, uri=None,
            args=None):
        """Launch context implementation.

        Function will call fork at the beginning. In forked process,
        it will try to choose proper implementation to execute and launch it.

        Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

        :param context:
            context GUID to look for implementations
        :param command:
            command that selected implementation should support
        :param object_id:
            optional id to restore Journal object
        :param uri:
            optional uri to open; if implementation supports it
        :param args:
            optional list of arguments to pass to launching implementation

        """
        # TODO Make a diference in launching from "~" and "/" mounts
        self.publish('launch', mountpoint=self._mountpoint, context=context,
                command=command, object_id=object_id, uri=uri, args=args)

    def __getattr__(self, name):
        """Class-like object to access to a resource or call a method.

        :param name:
            resource name started with capital char
        :returns:
            a class-like resource object

        """
        resource = self._resources.get(name)
        if resource is None:
            resource = _Resource(self._mountpoint, name.lower())
            self._resources[name] = resource
        return resource

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Connection(object):

    def __init__(self):
        self._pool = coroutine.Queue(maxsize=_CONNECTION_POOL)
        self._pool_size = 0
        self._subscribe_job = None
        self._subscriptions = {}

    def call(self, request, response=None):
        with self._pipe() as pipe:
            request['content_type'] = request.content_type
            pipe.write_message(request)
            if request.content_type == 'application/json':
                request.content = json.dumps(request.content)
            pipe.write(request.content)
            reply = pipe.read_message()

            _logger.debug('Made a call: request=%r reply=%r', request, reply)

        if type(reply) is dict and 'error' in reply:
            raise ServerError(reply['error'])

        return reply

    def connect(self, callback, condition=None):
        if self._subscribe_job is None:
            self._subscribe_job = coroutine.spawn(self._subscribe)
        self._subscriptions[callback] = condition or {}

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def publish(self, event):
        request = ad.Request()
        request['cmd'] = 'publish'
        request.content = event
        request.content_type = 'application/json'
        self.call(request)

    def close(self):
        if self._subscribe_job is not None:
            _logger.debug('Stop waiting for events')
            self._subscribe_job.kill()
            self._subscribe_job = None

        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                _logger.debug('Close IPC connection: %r', conn)
                conn.close()
            except Exception:
                util.exception(_logger, 'Cannot close IPC connection')

    @contextmanager
    def _pipe(self):
        if self._pool.qsize() or self._pool_size >= self._pool.maxsize:
            conn = self._pool.get()
        else:
            self._pool_size += 1
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = coroutine.socket(socket.AF_UNIX)
            conn.connect(local.ensure_path('run', 'accept'))
            _logger.debug('Open new IPC connection: %r', conn)
        try:
            yield sockets.SocketFile(conn)
        finally:
            self._pool.put(conn)

    def _subscribe(self):
        _logger.debug('Start waiting for events')

        conn = Client.subscribe()
        try:
            while True:
                coroutine.select([conn.fileno()], [], [])
                event = conn.read_message()
                if event is None:
                    break
                for callback, condition in self._subscriptions.items():
                    for key, value in condition.items():
                        if event.get(key) not in ('*', value):
                            break
                    else:
                        callback(event)
        finally:
            conn.close()


class _Resource(object):

    def __init__(self, mountpoint, name):
        self._request = {'mountpoint': mountpoint, 'document': name}

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
        from sugar_network.client.cursor import Cursor
        return Cursor(self._request, query, order_by, reply, page_size,
                **filters)

    def delete(self, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        return Client.call('DELETE', guid=guid, **self._request)

    def __call__(self, guid=None, reply=None, **kwargs):
        from sugar_network.client.objects import Object
        return Object(self._request, reply or [], guid, **kwargs)
