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
from sugar_network.util import enforce


class Resource(client.Object):
    """Common routines for Sugar Network resources."""

    #: Resource name
    resource = None
    #: List of properties to return from query to avoid
    # additional requests to fetch missed properties
    reply_properties = None

    def __init__(self, guid=None, **filters):
        """Get access to resource object.

        If no arguments given, new object will be assumed.

        :param guid:
            object's GUID to find
        :param filters:
            a dictionary of properties to find object if `guid`
            was not specified

        """
        if guid:
            client.Object.__init__(self, self.resource, {'guid': guid})
        elif not filters:
            client.Object.__init__(self, self.resource)
        else:
            query = self.find(**filters)
            enforce(query.total, KeyError, _('No objects found'))
            enforce(query.total == 1, _('Found more than one object'))
            client.Object.__init__(self, self.resource, query[0])

    def call(self, command, **kwargs):
        enforce('guid' in self, _('Object needs to be postet first'))
        kwargs['cmd'] = command
        return client.request('GET', [self.resource, self['guid']],
                headers={'Content-Type': 'application/json'},
                params=kwargs)

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


class User(Resource):

    resource = 'user'
    reply_properties = ['guid', 'nickname', 'color']

    @classmethod
    def new(cls):
        raise RuntimeError(_('Users cannot be created explicitly'))

    @classmethod
    def delete(cls, guid):
        raise RuntimeError(_('Users cannot be deleted explicitly'))


class Context(Resource):

    resource = 'context'
    reply_properties = ['guid', 'author', 'name', 'title', 'summary']


class Question(Resource):

    resource = 'question'
    reply_properties = ['guid', 'author', 'title']


class Idea(Resource):

    resource = 'idea'
    reply_properties = ['guid', 'author', 'title']


class Problem(Resource):

    resource = 'problem'
    reply_properties = ['guid', 'author', 'title']


class Review(Resource):

    resource = 'review'
    reply_properties = ['guid', 'author', 'title']


class Solution(Resource):

    resource = 'solution'
    reply_properties = ['guid', 'author', 'title']


class Artifact(Resource):

    resource = 'artifact'
    reply_properties = ['guid', 'author', 'title']


class Implementation(Resource):

    resource = 'implementation'
    reply_properties = ['guid', 'author', 'version', 'date', 'stability']


class Report(Resource):

    resource = 'report'
    reply_properties = ['guid', 'author', 'title']


class Notification(Resource):

    resource = 'notification'
    reply_properties = [
            'guid', 'author', 'type', 'object_type', 'object', 'to', 'message']


class Comment(Resource):

    resource = 'comment'
    reply_properties = ['guid', 'author', 'message']
