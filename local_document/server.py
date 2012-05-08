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
from os.path import exists
from gettext import gettext as _

import gevent
from gevent import socket
from gevent.server import StreamServer

from local_document import ipc, env
from local_document.socket import SocketFile
from active_document import Request, Response, util


_logger = logging.getLogger('local_document.server')


class Server(object):

    def __init__(self, mounts):
        self._mounts = mounts
        self._acceptor = _start_server('accept', self._serve_client)
        self._subscriber = _start_server('subscribe', self._serve_subscription)
        self._subscriptions = []

        for mount in self._mounts.values():
            mount.connect(self.__event_cb)

    def serve_forever(self):
        # Clients write to rendezvous named pipe, in block mode,
        # to make sure that server is started
        rendezvous = ipc.rendezvous(server=True)
        try:
            gevent.joinall([
                gevent.spawn(self._acceptor.serve_forever),
                gevent.spawn(self._subscriber.serve_forever),
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
                request = Request(message)
                request.command = request.pop('cmd')

                content_type = request.pop('content_type')
                if content_type == 'application/json':
                    request.content = json.loads(conn_file.read())
                elif content_type:
                    request.content_stream = conn_file
                else:
                    request.content = conn_file.read() or None

                response = Response()
                result = self._mounts.call(request, response)

            except Exception, error:
                util.exception(_('Failed to process %r for %r connection: %s'),
                        request, conn_file, error)
                conn_file.write_message({'error': str(error)})
            else:
                _logger.debug('Processed %r for %r connection: %r',
                        request, conn_file, result)
                conn_file.write_message(result)

    def _serve_subscription(self, conn_file):
        self._subscriptions.append(conn_file)
        return True

    def __event_cb(self, mount, event, **message):
        message['event'] = event
        for socket_file in self._subscriptions:
            socket_file.write_message(message)


def _start_server(name, serve_cb):
    accept_path = env.ensure_path('run', name)
    if exists(accept_path):
        os.unlink(accept_path)

    # pylint: disable-msg=E1101
    accept = socket.socket(socket.AF_UNIX)
    accept.bind(accept_path)
    accept.listen(5)

    def connection_cb(conn, address):
        conn_file = SocketFile(conn)
        _logger.debug('New %s connection: %r', name, conn_file)
        do_not_close = False
        try:
            do_not_close = serve_cb(conn_file)
        finally:
            _logger.debug('Quit %s connection: %r', name, conn_file)
            if not do_not_close:
                conn_file.close()

    return StreamServer(accept, connection_cb)
