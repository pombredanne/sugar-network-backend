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
import socket
import logging
from gettext import gettext as _

from sugar_network import node
from active_toolkit import sockets, coroutine, util, enforce


_logger = logging.getLogger('node.subscribe_socket')


class SubscribeSocket(object):

    def __init__(self, volume, host, port):
        self._host = host
        self._port = port
        self._server = None
        self._tickets = set()
        self._subscribers = set()

        volume.connect(self.__signal_cb)

    def new_ticket(self):
        ticket = os.urandom(16).encode('hex')
        self._tickets.add(ticket)
        return {'host': self._host, 'port': self._port, 'ticket': ticket}

    def serve_forever(self):
        _logger.info(_('Listening for subscriptions on %s port'), self._port)

        conn = coroutine.socket(socket.AF_INET, socket.SOCK_STREAM)
        # pylint: disable-msg=E1101
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.bind((self._host, self._port))
        conn.listen(5)

        self._server = coroutine.Server(conn, self._serve_client)
        try:
            self._server.serve_forever()
        finally:
            self._server.stop()
            self._server = None

    def stop(self):
        if self._server is not None:
            self._server.stop()

    def _serve_client(self, conn, host):
        _logger.debug('Got request from %r, making a handshake', host)

        try:
            handshake = sockets.SocketFile(conn).read_message()
            ticket = handshake.get('ticket')
            enforce(ticket and ticket in self._tickets, _('Unknown request'))
            self._tickets.remove(ticket)
        except Exception, error:
            _logger.warning(_('Handshake failed, discard the request: %s'),
                    error)
            return

        _logger.debug('Accepted %r subscriber', host)
        self._subscribers.add(conn)
        try:
            data = conn.recv(sockets.BUFFER_SIZE)
            enforce(not data, _('Subscriber misused connection ' \
                    'by sending %s bytes, discard it'), len(data))
        except Exception:
            util.exception(_('Failed to handle subscription from %r'), host)
        finally:
            _logger.debug('Close subscription from %r', host)
            self._subscribers.remove(conn)

    def __signal_cb(self, event):
        if node.only_sync_notification.value:
            if event['event'] != 'commit':
                # Even "sync" event can be ignored,
                # passing only "commit" is enough
                return
            event['event'] = 'sync'
        else:
            if event['event'] == 'commit':
                # Subscribers already got update notifications enough
                return

        for conn in self._subscribers:
            sockets.SocketFile(conn).write_message(event)
