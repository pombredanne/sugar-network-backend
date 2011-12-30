# Copyright (C) 2011, Aleksey Lim
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

import uuid
import logging
from gettext import gettext as _

from active_document import env, database_raw
from active_document.properties import GuidProperty
from active_document.util import enforce


class Database(object):
    """High level class to get access to Xapian databases."""

    def __init__(self, properties, crawler):
        """
        :param properties:
            `Property` objects associated with the `Database`
        :param crawler:
            iterator function that should return (guid, props)
            for every existing document

        """
        if 'guid' not in properties:
            properties['guid'] = GuidProperty()
        self._properties = properties
        self._db = database_raw.get(self.name, properties, crawler)

    @property
    def name(self):
        """Xapian database name."""
        return self.__class__.__name__.lower()

    @property
    def properties(self):
        """`Property` objects associated with the `Database`."""
        return self._properties

    def create(self, props):
        """Create new document.

        :param props:
            document properties
        :returns:
            GUID of newly created document

        """
        guid = str(uuid.uuid1())
        self._db.replace(guid, props)
        return guid

    def update(self, guid, props):
        """Update properties of existing document.

        :param guid:
            document GUID to update
        :param props:
            properties to update, not necessary all document's properties

        """
        enforce('guid' not in props or props['guid'] == guid)
        entries, total = self._db.find(0, 1, {'guid': guid},
                None, None, None, None)
        enforce(total == 1 and entries,
                _('Cannot find "%s" in %s to update'), guid, self.name)
        entries[0].update(props)
        self._db.replace(guid, entries[0])

    def delete(self, guid):
        """Delete document.

        :props guid:
            document GUID to delete

        """
        self._db.delete(guid)

    def find(self, offset=0, limit=None, request=None, query='',
            reply=None, order_by=None, group_by=None):
        """Search documents.

        The result will be an array of dictionaries with found documents'
        properties.

        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size;
            the `--find-limit` will be used by default
        :param request:
            a dictionary with property values to restrict the search
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param reply:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            array of properties to sort resulting list; property names might be
            prefixed with ``+`` (or without any prefixes) for ascending order,
            and ``-`` for descending order
        :param group_by:
            a property name to group resulting list by; if was specified,
            every resulting list item will contain `grouped` with
            a number of entries that are represented by the current one;
            no groupping by default
        :returns:
            a tuple of (`entries`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        if limit is None:
            limit = env.find_limit.value
        elif limit > env.find_limit.value:
            logging.warning(_('The find limit for %s is restricted to %s'),
                    self.name, env.find_limit.value)
            limit = env.find_limit.value
        if request is None:
            request = {}
        if not reply:
            reply = ['guid']
        if order_by is None:
            order_by = ['+ctime']

        return self._db.find(offset, limit, request, query, reply, order_by,
                group_by)

    def connect(self, *args, **kwargs):
        """Connect to feedback signals sent by the database."""
        self._db.connect(*args, **kwargs)

    def get_property(self, guid, name):
        pass
