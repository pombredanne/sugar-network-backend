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

import re
import uuid
import time
import logging
from gettext import gettext as _

import xapian

from active_document import env, database_writer
from active_document.properties import GuidProperty


# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z]+):=(")?((?(2)[^"]+|\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10


class Database(object):
    """Read-only access to Xapian databases."""

    _writer = None

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
        self.__db = None
        self._need_to_reopen = False

        if self._writer is None:
            self.__class__._writer = database_writer.get_writer(
                    self.name, properties, crawler)

        self._writer.connect('changed', self.__changed_cb)

    @property
    def name(self):
        """Xapian database name."""
        return self.__class__.__name__

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
        self._writer.create(guid, props)
        return guid

    def update(self, guid, props):
        """Update properties of existing document.

        :param guid:
            document GUID to update
        :param props:
            properties to update, not necessary all document properties

        """
        self._writer.update(guid, props)

    def delete(self, guid):
        """Delete document.

        :props guid:
            document GUID to delete

        """
        self._writer.delete(guid)

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
        if self._db is None:
            return [], 0

        if self._need_to_reopen:
            self._db.reopen()
            self._need_to_reopen = False

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

        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        enquire = self._enquire(request, query, order_by, group_by)
        result = self._call_db(enquire.get_mset, offset, limit, check_at_least)
        total_count = result.get_matches_estimated()

        entries = []
        for hit in result:
            entry = {}
            guid = hit.document.get_value(0)
            for name in reply:
                prop = self.properties.get(name)
                if prop is not None and prop.slot is not None:
                    entry[name] = hit.document.get_value(prop.slot)
                else:
                    entry[name] = self.get_property(guid, name)
            if group_by:
                entry['grouped'] = hit.collapse_count + 1
            entries.append(entry)

        logging.debug('Find in %s: offset=%s limit=%s request=%r query=%r ' \
                'order_by=%r group_by=%r time=%s entries=%s total_count=%s ' \
                'parsed=%s',
                self.name, offset, limit, request, query, order_by, group_by,
                time.time() - start_timestamp, len(entries), total_count,
                enquire.get_query())

        return (entries, total_count)

    def connect(self, *args, **kwargs):
        """Connect to signals sent by database writer."""
        self._writer.connect(*args, **kwargs)

    def get_property(self, guid, name):
        pass

    @property
    def _db(self):
        if self.__db is None:
            self.__db = self._writer.get_reader()
            self._need_to_reopen = False
        return self.__db

    def _call_db(self, op, *args):
        tries = 0
        while True:
            try:
                return op(*args)
            except xapian.DatabaseError, error:
                if tries >= _REOPEN_LIMIT:
                    logging.warning(_('Cannot open %s database'), self.name)
                    raise
                logging.debug('Fail to %r %s database, will reopen it %sth ' \
                        'time: %s', op, self.name, tries, error)
                time.sleep(tries * .1)
                self._db.reopen()
                tries += 1

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        queries = []
        boolean_queries = []

        if query:
            query = _extract_exact_search_terms(query, request)

        if query:
            parser = xapian.QueryParser()
            parser.set_database(self._db)
            for name, prop in self.properties.items():
                if prop.prefix:
                    if prop.boolean:
                        parser.add_boolean_prefix(name, prop.prefix)
                    else:
                        parser.add_prefix(name, prop.prefix)
                    parser.add_prefix('', prop.prefix)
            query = parser.parse_query(query,
                    xapian.QueryParser.FLAG_PHRASE |
                    xapian.QueryParser.FLAG_BOOLEAN |
                    xapian.QueryParser.FLAG_LOVEHATE |
                    xapian.QueryParser.FLAG_PARTIAL |
                    xapian.QueryParser.FLAG_WILDCARD,
                    '')
            queries.append(query)

        for name, value in request.items():
            value = str(value).strip()
            prop = self.properties.get(name)
            if prop is not None and prop.prefix:
                query = xapian.Query(env.term(prop.prefix, value))
                if prop.boolean:
                    boolean_queries.append(query)
                else:
                    queries.append(query)
            else:
                logging.warning(
                        _('Unknow search term "%s" for %s'), name, self.name)

        final_query = None
        if queries:
            final_query = xapian.Query(xapian.Query.OP_AND, queries)
        if boolean_queries:
            query = xapian.Query(xapian.Query.OP_AND, boolean_queries)
            if final_query is None:
                final_query = query
            else:
                final_query = xapian.Query(xapian.Query.OP_FILTER,
                        [final_query, query])
        if final_query is None:
            final_query = xapian.Query('')
        enquire.set_query(final_query)

        if hasattr(xapian, 'MultiValueKeyMaker'):
            sorter = xapian.MultiValueKeyMaker()
            for order in order_by:
                if order.startswith('+'):
                    reverse = False
                    order = order[1:]
                elif order.startswith('-'):
                    reverse = True
                    order = order[1:]
                else:
                    reverse = False
                prop = self.properties.get(order)
                if prop is not None and prop.slot is not None:
                    sorter.add_value(prop.slot, reverse)
                else:
                    logging.warning(_('Cannot sort using "%s" property in %s'),
                            order, self.name)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            logging.warning(_('In order to support sorting, ' \
                    'Xapian should be at least 1.2.0'))

        if group_by:
            prop = self.properties.get(group_by)
            if prop is not None and prop.slot is not None:
                enquire.set_collapse_key(prop.slot)
            else:
                logging.warning(_('Cannot group by "%s" property in %s'),
                        group_by, self.name)

        return enquire

    def __changed_cb(self, sender):
        self._need_to_reopen = True


def _extract_exact_search_terms(query, props):
    while True:
        exact_term = _EXACT_QUERY_RE.search(query)
        if exact_term is None:
            break
        query = query[:exact_term.start()] + query[exact_term.end():]
        term, __, value = exact_term.groups()
        props[term] = value
    return query
