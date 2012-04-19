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
import errno
from os.path import expanduser, dirname, exists, join
from gettext import gettext as _

from local_document import util


BUFSIZE = 1024 * 16


ipc_root = util.Option(
        _('path to a directory with IPC sockets'),
        default=expanduser('~/.local/var/sugar-network'))


def ipc_path(*args):
    """Make sure that directory, starting from `ipc_root`, exists.

    :param args:
        arguments with path parts
    :returns:
        the final path, prefixed with `ipc_root` value, in one string

    """
    path = join(ipc_root.value, *args)
    if path.endswith(os.sep):
        path_dir = path
    else:
        path_dir = dirname(path)
    if not exists(path_dir):
        try:
            os.makedirs(path_dir)
        except OSError, error:
            # Different process might create directory
            if error.errno != errno.EEXIST:
                raise
    return path


def rendezvous(server=False):
    """Rendezvous barrier to synchronize one server and multiple clients.

    :param server:
        if caller is a server
    :returns:
        if `server` is `True`, file descriptor that should be closed
        on server shutting down

    """
    path = ipc_path('rendezvous')

    try:
        os.mkfifo(path)
    except OSError, error:
        if error.errno != errno.EEXIST:
            raise

    if server:
        return os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    else:
        # Will be blocked until server will call `rendezvous(server=True)`
        fd = os.open(path, os.O_WRONLY)
        # No need in fd any more
        os.close(fd)


def recvline(socket):
    buf = []
    data = None

    while True:
        try:
            while data != '\n':
                data = socket.recv(1)
                if not data:
                    break
                buf.append(data)
        except OSError, error:
            # The try..except to catch EINTR was moved outside the
            # recv loop to avoid the per byte overhead.
            if error.errno == errno.EINTR:
                continue
            raise
        break

    return ''.join(buf)
