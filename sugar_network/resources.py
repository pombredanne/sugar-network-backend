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

from gettext import gettext as _

from sugar_network import client


class Resource(object):
    """Common routines for Sugar Network resources."""

    #: Resource name
    resource = None
    #: List of properties to return from query to avoid
    # additional requests to fetch missed properties
    reply_properties = None

    @classmethod
    def new(cls):
        """Create empty resource object.

        To send create request to a server, call `post()` function
        for returned object.

        :returns:
            `sugar_network.Object` object

        """
        return client.Object(cls.resource)

    @classmethod
    def get(cls, guid):
        """Get access to resource object.

        :param guid:
            object's GUID to get
        :returns:
            `sugar_network.Object` object

        """
        return client.Object(cls.resource, {'guid': guid})

    @classmethod
    def find(cls, *args, **kwargs):
        """Query resource objects.

        Function accpet the same arguments as `sugar_network.Query()`.

        """
        if 'reply_properties' not in kwargs:
            kwargs['reply_properties'] = cls.reply_properties
        return client.Query(cls.resource, *args, **kwargs)

    @classmethod
    def delete(cls, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        client.delete(cls.resource, guid)

    def __init__(self, guid):
        self._guid = guid
        self._path = '/%s/%s' % (self.resource, guid)

    def call(self, command, **kwargs):
        kwargs['cmd'] = command
        return client.request('GET', self._path, params=kwargs)


class User(Resource):

    resource = 'user'
    reply_properties = ['guid', 'nickname', 'color']

    @classmethod
    def new(cls):
        raise RuntimeError(_('Users cannot be created explicitly'))

    @classmethod
    def delete(cls, guid):
        raise RuntimeError(_('Users cannot be deleted explicitly'))
