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
$Data: 2012-05-15$

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
        self._read_size = None

    @property
    def socket(self):
        return self._socket

    def write_message(self, message):
        try:
            message_str = json.dumps(message)
        except Exception, error:
            raise RuntimeError(_('Cannot encode %r message: %s') % \
                    (message, error))
        self.write(message_str)

    def read_message(self):
        message_str = self.read()
        if not message_str:
            return None
        try:
            message = json.loads(message_str)
        except Exception, error:
            raise RuntimeError(_('Cannot decode "%s" message: %s') % \
                    (message_str, error))
        return message

    def write(self, data):
        if data is None:
            data = ''
        size_str = struct.pack('i', len(data))
        self._socket.send(size_str)
        self._socket.send(data)

    def read(self, size=None):

        def read_size():
            size_str = self._recv(struct.calcsize('i'))
            if not size_str:
                return 0
            return struct.unpack('i', size_str)[0]

        if size is None:
            chunks = []
            size = read_size()
            while size:
                chunk = self._recv(min(size, BUFFER_SIZE))
                if not chunk:
                    break
                chunks.append(chunk)
                size -= len(chunk)
            return ''.join(chunks)
        else:
            if self._read_size is None:
                self._read_size = read_size()
            if self._read_size:
                chunk = self._recv(min(self._read_size, BUFFER_SIZE, size))
            else:
                chunk = ''
            if not chunk:
                self._read_size = None
            else:
                self._read_size -= len(chunk)
            return chunk

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
