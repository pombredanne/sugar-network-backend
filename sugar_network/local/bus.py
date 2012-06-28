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

import os
import json
import socket
import logging
from os.path import exists

import active_document as ad
from sugar_network import local
from sugar_network.toolkit import ipc, sugar
from active_toolkit import coroutine, sockets


_logger = logging.getLogger('local.bus')


class IPCServer(object):

    def __init__(self, mounts, delayed_start=None):
        self._subscriptions = []
        self._mounts = mounts
        self._delayed_start = delayed_start
        self._acceptor = _start_server('accept', self._serve_client)
        self._subscriber = _start_server('subscribe', self._serve_subscription)
        self._principal = sugar.uid()
        self._publish_lock = coroutine.Lock()

        self._mounts.connect(self._republish)

    def serve_forever(self):
        # Clients write to rendezvous named pipe, in block mode,
        # to make sure that server is started
        rendezvous = ipc.rendezvous(server=True)
        try:
            coroutine.joinall([
                coroutine.spawn(self._acceptor.serve_forever),
                coroutine.spawn(self._subscriber.serve_forever),
                ])
        except KeyboardInterrupt:
            pass
        finally:
            os.close(rendezvous)

    def stop(self):
        while self._subscriptions:
            self._subscriptions.pop().close()
        self._acceptor.stop()
        self._subscriber.stop()

    def _serve_client(self, conn_file):
        while True:
            message = conn_file.read_message()
            if message is None:
                break
            try:
                request = ad.Request(message)
                request.principal = self._principal
                request.access_level = ad.ACCESS_LOCAL

                content_type = request.pop('content_type')
                if content_type == 'application/json':
                    request.content = json.loads(conn_file.read())
                elif content_type:
                    request.content_stream = conn_file
                else:
                    request.content = conn_file.read() or None

                if request.get('cmd') == 'publish':
                    self._republish(request.content)
                    result = None
                else:
                    response = ad.Response()
                    result = self._mounts.call(request, response)

                conn_file.write_message(result)
            except Exception, error:
                conn_file.write_message({'error': str(error)})

    def _serve_subscription(self, conn_file):
        self._subscriptions.append(conn_file)
        return True

    def _republish(self, event):
        if event.get('event') == 'delayed-start':
            if self._delayed_start is not None:
                try:
                    self._delayed_start()
                finally:
                    self._delayed_start = None
            return

        _logger.debug('Send notification: %r', event)

        with self._publish_lock:
            for socket_file in self._subscriptions:
                socket_file.write_message(event)


def _start_server(name, serve_cb):
    accept_path = local.ensure_path('run', name)
    if exists(accept_path):
        os.unlink(accept_path)

    # pylint: disable-msg=E1101
    accept = coroutine.socket(socket.AF_UNIX)
    accept.bind(accept_path)
    accept.listen(5)

    def connection_cb(conn, address):
        conn_file = sockets.SocketFile(conn)
        _logger.debug('New %r connection: %r', name, conn_file)
        do_not_close = False
        try:
            do_not_close = serve_cb(conn_file)
        finally:
            _logger.debug('Quit %r connection: %r', name, conn_file)
            if not do_not_close:
                conn_file.close()

    return coroutine.Server(accept, connection_cb)
