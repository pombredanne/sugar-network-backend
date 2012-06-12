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
from gettext import gettext as _

import active_document as ad
from local_document import ipc, env, sugar
from active_toolkit import util, coroutine, sockets


_CONNECTION_POOL = 6

_logger = logging.getLogger('sugar_network.bus')


class ServerError(RuntimeError):
    pass


class Request(dict):

    connection = None
    principal = None

    def __init__(self, mountpoint=None, document=None):
        dict.__init__(self)

        if mountpoint:
            self['mountpoint'] = mountpoint
        if document:
            self['document'] = document

        if Request.connection is None:
            Request.connection = _Client()
            Request.principal = sugar.uid()

    @property
    def online(self):
        return self.get('mountpoint') == '/'

    def call(self, method, cmd=None, content=None, content_type=None,
            **kwargs):
        request = ad.Request(kwargs)
        request.access_level = ad.ACCESS_LOCAL
        request.principal = self.principal
        request.update(self)
        request['method'] = method
        if cmd:
            request['cmd'] = cmd
        request.content = content
        request.content_type = content_type
        return self.connection.call(request)

    def publish(self, event, **kwargs):
        kwargs['event'] = event
        self.connection.publish(kwargs)

    def connect(self, callback):
        self.connection.connect(callback, self)

    def disconnect(self, callback):
        self.connection.disconnect(callback)


class _Client(object):

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
                util.exception(_logger, _('Cannot close IPC connection'))

    @staticmethod
    def subscribe():
        """Start subscription session.

        :returns:
            `SocketFile` object connected to IPC server to read events from

        """
        return _subscribe()

    @contextmanager
    def _pipe(self):
        if self._pool.qsize() or self._pool_size >= self._pool.maxsize:
            conn = self._pool.get()
        else:
            self._pool_size += 1
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = coroutine.socket(socket.AF_UNIX)
            conn.connect(env.ensure_path('run', 'accept'))
            _logger.debug('Open new IPC connection: %r', conn)
        try:
            yield sockets.SocketFile(conn)
        finally:
            self._pool.put(conn)

    def _subscribe(self):
        _logger.debug('Start waiting for events')

        conn = _subscribe()
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


def _subscribe():
    ipc.rendezvous()
    # pylint: disable-msg=E1101
    conn = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
    conn.connect(env.ensure_path('run', 'subscribe'))
    return conn
