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

import logging
from gettext import gettext as _

from active_document import env, index_db
from active_document.properties import GuidProperty
from active_document.util import enforce


class Index(object):
    """High level class to get access to Xapian databases."""

    def __init__(self, metadata):
        """
        :param metadata:
            `Metadata` object that describes the document

        """
        self.metadata = metadata
        if 'guid' not in metadata:
            metadata['guid'] = GuidProperty()
        self._db = index_db.get(metadata)

    def store(self, guid, props, new):
        """Store properties for a document.

        :param guid:
            document GUID to store
        :param props:
            properties to store; for non new entities, not necessary
            all document's properties
        :param new:
            if it is a new entity

        """
        enforce('guid' not in props or props['guid'] == guid)

        if new:
            props['guid'] = guid
            for name, prop in self.metadata.items():
                value = props.get(name, prop.default)
                enforce(value is not None,
                        _('Property "%s" should be passed while creating ' \
                                'new %s document'),
                        name, self.metadata.name)
                props[name] = env.value(value)
        else:
            entries, __ = self._db.find(0, 1, {'guid': guid})
            enforce(len(entries) == 1, _('Cannot find "%s" in %s to store'),
                    guid, self.metadata.name)
            entries[0].update(props)
            props = entries[0]

        self._db.store(guid, props, new)

    def delete(self, guid):
        """Delete document.

        :props guid:
            document GUID to delete

        """
        self._db.delete(guid)

    def find(self, offset=0, limit=None, request=None, query='', reply=None,
            order_by=None, group_by=None):
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
            a tuple of (`guids`, `entries`, `total_count`);
            where the `total_count` is the total number of documents
            conforming the search parameters, i.e., not only documents that
            are included to the resulting list

        """
        if limit is None:
            limit = env.find_limit.value
        elif limit > env.find_limit.value:
            logging.warning(_('The find limit for %s is restricted to %s'),
                    self.metadata.name, env.find_limit.value)
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
