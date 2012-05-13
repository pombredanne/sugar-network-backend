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
import logging
from os.path import join, exists, dirname

from local_document import activities
from sugar_network.objects import Object
from sugar_network.cursor import Cursor
from sugar_network.connection import Connection


_logger = logging.getLogger('sugar_network')


def checkins(context):
    """Iterate paths of checked in implementations."""
    return activities.checkins(context)


def launch(context, command='activity', args=None):
    """Launch context implementation.

    Function will call fork at the beginning. In forked process,
    it will try to choose proper implementation to execute and launch it.

    Execution log will be stored in `~/.sugar/PROFILE/logs` directory.

    :param command:
        command that selected implementation should support
    :param args:
        optional list of arguments to pass to launching implementation
    :returns:
        child process pid

    """
    pid = os.fork()
    if pid:
        return pid

    cmd = ['sugar-network', '-C', command, 'launch', context] + \
            (args or [])

    cmd_path = join(dirname(__file__), '..', 'sugar-network')
    if exists(cmd_path):
        os.execv(cmd_path, cmd)
    else:
        os.execvp(cmd[0], cmd)

    exit(1)


class Client(object):
    """IPC class to get access from a client side.

    See http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/Client
    for detailed information.

    """

    def __init__(self, mountpoint):
        self._mountpoint = mountpoint
        self._resources = {}

    @property
    def connected(self):
        conn = Connection(self._mountpoint, None)
        return conn.send('is_connected')

    def launch(self, *args, **kwargs):
        # TODO Make a diference in launching from "~" and "/" mounts
        return launch(*args, **kwargs)

    def __getattr__(self, name):
        """Class-like object to access to a resource or call a method.

        :param name:
            resource name started with capital char
        :returns:
            a class-like resource object

        """
        resource = self._resources.get(name)
        if resource is None:
            resource = _Resource(self._mountpoint, name.lower())
            self._resources[name] = resource
        return resource

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Resource(object):

    def __init__(self, mountpoint, name):
        self._conn = Connection(mountpoint, name)

    def cursor(self, query=None, order_by=None, reply=None, page_size=18,
            **filters):
        """Query resource objects.

        :param query:
            full text search query string in Xapian format
        :param order_by:
            name of property to sort by; might be prefixed by either `+` or `-`
            to change order's direction
        :param reply:
            list of property names to return for found objects;
            by default, only GUIDs will be returned; for missed properties,
            will be sent additional requests to a server on getting access
            to particular object.
        :param page_size:
            number of items in one cached page, there are might be several
            (at least two) pages
        :param filters:
            a dictionary of properties to filter resulting list

        """
        return Cursor(self._conn, query, order_by, reply, page_size, **filters)

    def delete(self, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        return self._conn.send('DELETE', guid=guid)

    def __call__(self, guid=None, reply=None, **kwargs):
        return Object(self._conn, reply or [], guid, **kwargs)
