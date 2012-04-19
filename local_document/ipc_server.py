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
from gettext import gettext as _

import gevent
from gevent import socket

from local_document import env, util, enforce


_logger = logging.getLogger('local_document.ipc_server')


class Server(object):

    def __init__(self, commands_processor):
        self.commands_processor = commands_processor

    def serve_forever(self):
        accept_path = env.ipc_path('accept')
        # pylint: disable-msg=E1101
        accept = socket.socket(socket.AF_UNIX)
        accept.bind(accept_path)
        accept.listen(5)

        # Clients write to rendezvous named pipe, in block mode,
        # to make sure that server is started
        rendezvous = env.rendezvous(server=True)
        try:
            gevent.joinall([
                gevent.spawn(self._accept_clients, accept),
                ])
        except KeyboardInterrupt:
            pass
        finally:
            os.close(rendezvous)
            accept.close()
            os.unlink(accept_path)

    def _accept_clients(self, accept):
        while True:
            conn, __ = accept.accept()
            self._serve_client(conn)

    def _serve_client(self, conn):
        _logger.debug('Opened connection %r', conn.fileno())

        def read_message():
            cmd_line = env.recvline(conn)
            if not cmd_line:
                return False

            try:
                message = json.loads(cmd_line)
            except Exception:
                util.exception(_('Bad parse client\'s command, %r'), cmd_line)
                return True

            _logger.debug('Got a call: %r', message)

            enforce('cmd' in message, _('Argument "cmd" was not specified'))
            cmd = message.pop('cmd')
            enforce(hasattr(self.commands_processor, cmd),
                    _('Unknown %r command'), cmd)

            reply = getattr(self.commands_processor, cmd)(conn, **message)
            conn.send(json.dumps(reply) + '\n')

            _logger.debug('Send reply: %r', reply)
            return True

        try:
            while read_message():
                pass
        finally:
            _logger.debug('Closed connection %r', conn.fileno())
            conn.close()
