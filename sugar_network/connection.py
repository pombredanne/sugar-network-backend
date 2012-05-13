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
from contextlib import contextmanager
from os.path import join, exists
from gettext import gettext as _

import gevent
from gevent import socket
from gevent.queue import Queue

from local_document import ipc, env
from local_document.socket import SocketFile
from active_document import util


_CONNECTION_POOL = 6

_logger = logging.getLogger('sugar_network')


class ServerError(RuntimeError):
    pass


class Connection(object):

    def __init__(self, mountpoint, document=None):
        self.mountpoint = mountpoint
        self.document = document

    @property
    def online(self):
        return self.mountpoint == '/'

    def connect(self, callback):
        conn = _Connection.get()
        conn.subscriptions[callback] = (self.mountpoint, self.document)

    def disconnect(self, callback):
        conn = _Connection.get()
        if callback in conn.subscriptions:
            del conn.subscriptions[callback]

    @staticmethod
    def subscribe():
        """Start subscription session.

        :returns:
            `SocketFile` object connected to IPC server to read events from

        """
        return _subscribe()

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

        conn = _Connection.get()
        with conn.socket_file() as socket_file:
            socket_file.write_message(request)
            if content_type == 'application/json':
                content = json.dumps(content)
            socket_file.write(content)
            response = socket_file.read_message()

            _logger.debug('Made a call: request=%r response=%r',
                    request, response)

        if type(response) is dict and 'error' in response:
            raise ServerError(response['error'])

        return response

    def __repr__(self):
        return str((self.mountpoint, self.document))


class _Connection(object):

    _instance = None

    def __init__(self):
        self._pool = Queue(maxsize=_CONNECTION_POOL)
        self._pool_size = 0
        self._subscribe_job = None
        self._subscriptions = {}

    def __del__(self):
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

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = _Connection()
        return cls._instance

    @contextmanager
    def socket_file(self):
        if self._pool.qsize() or self._pool_size >= self._pool.maxsize:
            conn = self._pool.get()
        else:
            self._pool_size += 1
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = socket.socket(socket.AF_UNIX)
            conn.connect(env.ensure_path('run', 'accept'))
            _logger.debug('Open new IPC connection: %r', conn)
        try:
            yield SocketFile(conn)
        finally:
            self._pool.put(conn)

    @property
    def subscriptions(self):
        if self._subscribe_job is None:
            self._subscribe_job = gevent.spawn(self._subscribe)
        return self._subscriptions

    def _subscribe(self):
        _logger.debug('Start waiting for events')

        conn = _subscribe()
        try:
            while True:
                socket.wait_read(conn.fileno())
                event = conn.read_message()
                if event is None:
                    break
                for callback, (mountpoint, document) in \
                        self._subscriptions.items():
                    if event['mountpoint'] == mountpoint and \
                            ('document' not in event or \
                            event['document'] == document):
                        callback(event)
        finally:
            conn.close()


def _subscribe():
    ipc.rendezvous()
    # pylint: disable-msg=E1101
    conn = SocketFile(socket.socket(socket.AF_UNIX))
    conn.connect(env.ensure_path('run', 'subscribe'))
    return conn
