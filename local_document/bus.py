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
import socket
import logging
from os.path import exists
from gettext import gettext as _

from local_document import ipc, env, activities
from local_document.sockets import SocketFile
from local_document.mounts import Mounts
from active_document import Request, Response, util, coroutine


_logger = logging.getLogger('local_document.bus')


class Server(object):

    def __init__(self, root, resources):
        self._subscriptions = []
        self._mounts = Mounts(root, resources, self._publish_event)
        self._acceptor = _start_server('accept', self._serve_client)
        self._subscriber = _start_server('subscribe', self._serve_subscription)
        self._monitor = coroutine.spawn(activities.monitor, self._mounts)

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
            self._monitor.kill()
            self._mounts.close()

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
                request_repr = str(request)

                content_type = request.pop('content_type')
                if content_type == 'application/json':
                    request.content = json.loads(conn_file.read())
                elif content_type:
                    request.content_stream = conn_file
                else:
                    request.content = conn_file.read() or None

                if request.command == 'publish':
                    self._publish_event(request.content)
                    result = None
                else:
                    response = Response()
                    result = self._mounts.call(request, response)

            except Exception, error:
                if isinstance(error, env.Offline):
                    _logger.debug('Ignore %r request: %s', request, error)
                else:
                    util.exception(_logger,
                            _('Failed to process %s for %r connection: %s'),
                            request_repr, conn_file, error)
                conn_file.write_message({'error': str(error)})
            else:
                _logger.debug('Processed %s for %r connection: %r',
                        request_repr, conn_file, result)
                conn_file.write_message(result)

    def _serve_subscription(self, conn_file):
        self._subscriptions.append(conn_file)
        return True

    def _publish_event(self, event):
        _logger.debug('Send notification: %r', event)
        for socket_file in self._subscriptions:
            socket_file.write_message(event)


def _start_server(name, serve_cb):
    accept_path = env.ensure_path('run', name)
    if exists(accept_path):
        os.unlink(accept_path)

    # pylint: disable-msg=E1101
    accept = coroutine.socket(socket.AF_UNIX)
    accept.bind(accept_path)
    accept.listen(5)

    def connection_cb(conn, address):
        conn_file = SocketFile(conn)
        _logger.debug('New %r connection: %r', name, conn_file)
        do_not_close = False
        try:
            do_not_close = serve_cb(conn_file)
        finally:
            _logger.debug('Quit %r connection: %r', name, conn_file)
            if not do_not_close:
                conn_file.close()

    return coroutine.Server(accept, connection_cb)
