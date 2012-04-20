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
import errno
import struct
from os.path import dirname, exists, join
from gettext import gettext as _

from local_document import env
from active_document import enforce


BUFSIZE = 1024 * 10


def path(*args):
    """Make sure that directory, starting from `ipc_root`, exists.

    :param args:
        arguments with path parts
    :returns:
        the final path, prefixed with `ipc_root` value, in one string

    """
    result = join(env.ipc_root.value, *args)
    if result.endswith(os.sep):
        path_dir = result
    else:
        path_dir = dirname(result)
    if not exists(path_dir):
        try:
            os.makedirs(path_dir)
        except OSError, error:
            # Different process might create directory
            if error.errno != errno.EEXIST:
                raise
    return result


def rendezvous(server=False):
    """Rendezvous barrier to synchronize one server and multiple clients.

    :param server:
        if caller is a server
    :returns:
        if `server` is `True`, file descriptor that should be closed
        on server shutting down

    """
    rendezvous_path = path('rendezvous')

    try:
        os.mkfifo(rendezvous_path)
    except OSError, error:
        if error.errno != errno.EEXIST:
            raise

    if server:
        return os.open(rendezvous_path, os.O_RDONLY | os.O_NONBLOCK)
    else:
        # Will be blocked until server will call `rendezvous(server=True)`
        fd = os.open(rendezvous_path, os.O_WRONLY)
        # No need in fd any more
        os.close(fd)


class SocketFile(object):

    def __init__(self, socket):
        self._socket = socket
        self._message_buffer = bytearray('\0' * BUFSIZE)

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
            enforce(pos < BUFSIZE, _('Too long message'))

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
        return str(self._socket.fileno())
