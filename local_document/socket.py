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

"""Utilities to work with sockets.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/socket.py$
$Data: 2012-04-26$

"""

import json
import errno
import struct
from gettext import gettext as _

from . import util
enforce = util.enforce


BUFFER_SIZE = 1024 * 10


class SocketFile(object):

    def __init__(self, socket):
        self._socket = socket
        self._message_buffer = bytearray('\0' * BUFFER_SIZE)

    @property
    def socket(self):
        return self._socket

    def write_message(self, message):
        try:
            message_str = json.dumps(message)
        except Exception, error:
            raise RuntimeError(_('Cannot encode %r message: %s') % \
                    (message, error))
        self._socket.send(message_str)
        self._socket.send('\n')

    def read_message(self):
        pos = 0

        chunk = None
        while chunk != '\n':
            chunk = self._recv(1)
            if not chunk:
                # Got disconnected
                return None
            self._message_buffer[pos] = chunk
            pos += 1
            enforce(pos < BUFFER_SIZE, _('Too long message'))

        message_str = buffer(self._message_buffer, 0, pos)
        try:
            # XXX Have to convert to str,
            # json from Python-2.6 doesn't treat buffer as a string
            message = json.loads(str(message_str))
        except Exception, error:
            raise RuntimeError(_('Cannot decode "%s" message: %s') % \
                    (message_str, error))

        return message

    def write(self, data):
        size_str = struct.pack('i', len(data))
        self._socket.send(size_str)
        self._socket.send(data)

    def read(self):
        size_str = self._recv(struct.calcsize('i'))
        size, = struct.unpack('i', size_str)
        # TODO Make sure that we got exactly `size` bytes
        return self._recv(size)

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def _recv(self, size):
        while True:
            try:
                chunk = self._socket.recv(size)
            except OSError, error:
                if error.errno == errno.EINTR:
                    continue
                raise
            return chunk

    def __repr__(self):
        return repr(self._socket)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, name):
        return getattr(self._socket, name)
