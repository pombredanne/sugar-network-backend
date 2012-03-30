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

import re
from gettext import gettext as _

from sugar_network import client, cache
from sugar_network.util import enforce


_GUID_RE = re.compile('[a-z0-9]{28}')


class Resource(client.Object):

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
            client.Object.__init__(self, self.name(), {'guid': guid})
        elif not filters:
            client.Object.__init__(self, self.name())
        else:
            query = self.find(**filters)
            enforce(query.total, KeyError, _('No objects found'))
            enforce(query.total == 1, _('Found more than one object'))
            client.Object.__init__(self, self.name(), query[0])

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @classmethod
    def find(cls, *args, **kwargs):
        """Query resource objects.

        Function accpet the same arguments as `sugar_network.Query()`.

        """
        return client.Query(cls.name(), *args, **kwargs)

    @classmethod
    def delete(cls, guid):
        """Delete resource object.

        :param guid:
            resource object's GUID

        """
        client.delete(cls.name(), guid)


class User(Resource):

    def __init__(self, guid, **filters):
        enforce(guid, _('Users cannot be created explicitly'))
        Resource.__init__(self, guid, **filters)

    @classmethod
    def delete(cls, guid):
        raise RuntimeError(_('Users cannot be deleted explicitly'))


class Context(Resource):

    def __init__(self, guid=None, **filters):
        if guid and _GUID_RE.match(guid) is None:
            guid = self.resolve(guid)
        Resource.__init__(self, guid, **filters)

    @property
    def implement(self):
        """Sugar Network name the context is implementing.

        It is the first, if there are more than one, entity from `implement`
        context property. If `implement` is empty, return `guid`.

        :returns:
            string value

        """
        if self['implement']:
            return self['implement'][0]
        else:
            return self['guid']

    def resolve(self, name):
        return cache.resolve_context(name)


class Question(Resource):
    pass


class Idea(Resource):
    pass


class Problem(Resource):
    pass


class Review(Resource):
    pass


class Solution(Resource):
    pass


class Artifact(Resource):
    pass


class Implementation(Resource):
    pass


class Report(Resource):
    pass


class Notification(Resource):
    pass


class Comment(Resource):
    pass


client.Object.memory_cache = {
        Context.name(): {
            'vote': (False, bool),
            },
        Question.name(): {
            'vote': (False, bool),
            },
        Idea.name(): {
            'vote': (False, bool),
            },
        Problem.name(): {
            'vote': (False, bool),
            },
        Review.name(): {
            'vote': (False, bool),
            },
        Solution.name(): {
            'vote': (False, bool),
            },
        Artifact.name(): {
            'vote': (False, bool),
            },
        }
