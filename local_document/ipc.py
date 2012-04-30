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
import logging
from os.path import exists, join

from local_document import env, sugar


_logger = logging.getLogger('local_document.ipc')


def path(suffix):
    """Make sure that directory, starting from `ipc_root`, exists.

    :param suffix:
        filename suffix
    :returns:
        the final path, prefixed with `ipc_root` value, in one string

    """
    path_dir = env.ipc_root.value
    if not path_dir:
        path_dir = sugar.profile_path('run')

    if not exists(path_dir):
        try:
            os.makedirs(path_dir)
        except OSError, error:
            # Different process might create directory
            if error.errno != errno.EEXIST:
                raise

    return join(path_dir, 'sugar-network.' + suffix)


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
        _logger.debug('Start accepting clients')
        return os.open(rendezvous_path, os.O_RDONLY | os.O_NONBLOCK)
    else:
        _logger.debug('Connecting to the server')
        # Will be blocked until server will call `rendezvous(server=True)`
        fd = os.open(rendezvous_path, os.O_WRONLY)
        _logger.debug('Connected successfully')
        # No need in fd any more
        os.close(fd)
