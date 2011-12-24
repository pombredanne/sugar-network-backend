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
import shutil
import logging
from os.path import exists, join, dirname
from gettext import gettext as _

import xapian
import gobject

from active_document import util, env
from active_document.util import enforce


# To invalidate existed Xapian db on stcuture changes in stored documents
_LAYOUT_VERSION = 1

# Additional prefix for exact search terms
_EXACT_PREFIX = 'X'
# Term prefix for GUID value
_GUID_PREFIX = 'I'

# Default Database flush values
_FLUSH_TIMEOUT = 5
_FLUSH_THRESHOLD = 512

# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z]+):=(")?((?(2)[^"]+|\S+))(?(2)")')


class Database(gobject.GObject):
    """Manage Xapian databases."""

    __gsignals__ = {
            'changed': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            }

    #: `Term` objects collected for a class inherited from the `Database`
    terms = {}

    def __init__(self,
            flush_timeout=_FLUSH_TIMEOUT, flush_threshold=_FLUSH_THRESHOLD):
        """
        Xapian database will be openned.

        :param flush_timeout:
            force a flush after `flush_timeout` seconds since
            the last change to the database
        :param flush_threshold:
            force a flush every `flush_threshold` changes to the database

        """
        gobject.GObject.__init__(self)

        self._flush_timeout = flush_timeout
        self._flush_threshold = flush_threshold
        self._db = None
        self._flush_timeout_hid = None
        self._pending_writes = 0

        if 'guid' not in self.terms:
            self.terms['guid'] = Term('guid', 0, _GUID_PREFIX)

        self._open(False)

    @property
    def name(self):
        """Xapian database name."""
        return self.__class__.__name__

    def close(self):
        """Close the database."""
        if self._db is None:
            return
        self._commit(True)
        self._db = None

    def create(self, props):
        """Create new document.

        :param props:
            document properties
        :returns:
            GUID of newly created document

        """
        guid = str(uuid.uuid1())
        self.update(guid, props)
        return guid

    def update(self, guid, props):
        """Update properties of existing document.

        :param guid:
            document GUID to update
        :param props:
            properties to update, not necessary all document properties

        """
        props['guid'] = guid
        for name, value in props.items():
            props[name] = _value(value)
        for term in self.terms.values():
            if term.default is not None and term.name not in props:
                props[term.name] = _value(term.default)

        logging.debug('Store %s object: guid=%s props=%r',
                self.name, guid, props)

        document = xapian.Document()

        for term in self.terms.values():
            enforce(term.name in props,
                    _('Property "%s" should be passed while creating new ' \
                            '%s document'), term.name, self.name)
            document.add_value(term.slot, props[term.name])

        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)
        for term in self.terms.values():
            if not term.prefix:
                continue
            for i in term.list_value(props[term.name]):
                document.add_term(_key(term.prefix, i))
                term_generator.index_text(i, 1, term.prefix)
                term_generator.increase_termpos()

        self._db.replace_document(_key(_GUID_PREFIX, guid), document)
        self._commit(False)

    def delete(self, guid):
        """Delete document.

        :props guid:
            document GUID to delete

        """
        logging.debug('Delete "%s" document from %s', guid, self.name)
        self._db.delete_document(_key(_GUID_PREFIX, guid))
        self._commit(False)

    def find(self, offset=0, limit=None, request=None, query='',
            properties=None, order_by=None, group_by=None):
        """Search documents.

        The result will be an array of dictionaries with found documents'
        properties.

        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size;
            all entries by default
        :param request:
            a dictionary with property values to restrict the search
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param properties:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            array of properties to sort resulting list;
            property names might be prefixed with:
                - `+`, field prefix or without any prefixes, ascending order;
                - `-` descending order.
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
            limit = self._db.get_doccount()
        if request is None:
            request = {}
        if not properties:
            properties = ['guid']
        if order_by is None:
            order_by = ['+ctime']

        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        try:
            enquire = self._enquire(request, query, order_by, group_by)
            result = enquire.get_mset(offset, limit, check_at_least)
            total_count = result.get_matches_estimated()
        except xapian.DatabaseError:
            util.exception(_('Xapian index search failed for %s, ' \
                    'will rebuild index'), self.name)
            self._open(True)
            return [], 0

        entries = []
        for hit in result:
            doc = {}
            for term in [self.terms[i] for i in properties]:
                doc[term.name] = hit.document.get_value(term.slot)
            if group_by:
                doc['grouped'] = hit.collapse_count + 1
            entries.append(doc)

        logging.debug('Find in %s: offset=%s limit=%s request=%r query=%r ' \
                'order_by=%r group_by=%r time=%s entries=%s total_count=%s ' \
                'parsed=%s',
                self.name, offset, limit, request, query, order_by, group_by,
                time.time() - start_timestamp, len(entries), total_count,
                enquire.get_query())

        return (entries, total_count)

    def scan_cb(self):
        """Scan for a document.

        This function will be called from internals when database
        needs to be populated. If function returns a found document,
        it will be called once more until it fails.

        :returns:
            `None` if there no documents;
            a tuple of (guid, properties) for found document

        """
        pass

    def _open(self, reset):
        self.close()
        index_path = env.path(self.name, 'index', '')

        if not reset and self._is_layout_stale():
            reset = True
        if reset:
            shutil.rmtree(index_path, ignore_errors=True)

        try:
            self._db = xapian.WritableDatabase(
                    index_path, xapian.DB_CREATE_OR_OPEN)
        except xapian.DatabaseError:
            if reset:
                util.exception(_('Unrecoverable error while opening %s ' \
                        'Xapian index'), index_path)
                raise
            else:
                util.exception(_('Cannot open Xapian index in %s, ' \
                        'will rebuild it'), index_path)
                self._open(True)
                return

        self._save_layout()

        gobject.idle_add(self._populate)

    def _populate(self):
        if self._db is None:
            return
        doc = self.scan_cb()
        if doc is None:
            return
        self.update(*doc)
        gobject.idle_add(self._populate)

    def _commit(self, flush):
        if self._flush_timeout_hid is not None:
            gobject.source_remove(self._flush_timeout_hid)
            self._flush_timeout_hid = None

        self._pending_writes += 1

        if flush or self._flush_threshold and \
                self._pending_writes >= self._flush_threshold:
            logging.debug('Commit %s: flush=%r _pending_writes=%r',
                    self.name, flush, self._pending_writes)
            if hasattr(self._db, 'commit'):
                self._db.commit()
            else:
                self._db.flush()
            self._pending_writes = 0
            self.emit('changed')
        elif self._flush_timeout:
            self._flush_timeout_hid = gobject.timeout_add_seconds(
                    self._flush_timeout, lambda: self._commit(True))

        return False

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        subqueries = []

        if query:
            query = _extract_exact_search_terms(query, request)

        if query:
            parser = xapian.QueryParser()
            parser.set_database(self._db)
            for term in self.terms.values():
                if term.prefix:
                    parser.add_prefix(term.name, term.prefix)
                    parser.add_prefix('', term.prefix)
            query = parser.parse_query(query,
                    xapian.QueryParser.FLAG_PHRASE |
                    xapian.QueryParser.FLAG_BOOLEAN |
                    xapian.QueryParser.FLAG_LOVEHATE |
                    xapian.QueryParser.FLAG_PARTIAL |
                    xapian.QueryParser.FLAG_WILDCARD,
                    '')
            subqueries.append(query)

        for term, value in request.items():
            value = str(value).strip()
            if term in self.terms:
                query = xapian.Query(_key(self.terms[term].prefix, value))
                subqueries.append(query)
            else:
                logging.warning(
                        _('Unknow search term "%s" for %s'), term, self.name)

        if subqueries:
            enquire.set_query(xapian.Query(xapian.Query.OP_AND, subqueries))
        else:
            enquire.set_query(xapian.Query(''))

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
                if order not in self.terms:
                    logging.warning(_('Cannot sort using "%s" property in %s'),
                            order, self.name)
                    continue
                sorter.add_value(self.terms[order].slot, reverse)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            logging.warning(_('In order to support sorting, ' \
                    'Xapian should be at least 1.2.0'))

        if group_by:
            if group_by in self.terms:
                enquire.set_collapse_key(self.terms[group_by].slot)
            else:
                logging.warning(_('Cannot group by "%s" property in %s'),
                        group_by, self.name)

        return enquire

    def _is_layout_stale(self):
        path = env.path(self.name, 'version')
        if not exists(path):
            return True
        version = file(path).read()
        return not version.isdigit() or int(version) != _LAYOUT_VERSION

    def _save_layout(self):
        version = file(env.path(self.name, 'version'), 'w')
        version.write(str(_LAYOUT_VERSION))
        version.close()


class Term(object):
    """Collect inforamtion for Xapian term."""

    def __init__(self, name, slot, prefix=None, default=None, is_list=False,
            list_separator=None):
        enforce(name == 'guid' or slot != 0,
                _('The slot "0" is reserved for internal needs'))
        enforce(name == 'guid' or prefix != _GUID_PREFIX,
                _('The prefix "I" is reserved for internal needs'))
        self._name = name
        self._slot = slot
        self._prefix = prefix
        self._default = default
        self._is_list = is_list
        self._list_separator = list_separator

    @property
    def name(self):
        """Term name."""
        return self._name

    @property
    def slot(self):
        """Xapian document slot to place this property value."""
        return self._slot

    @property
    def prefix(self):
        """Xapian term prefix or None for not using term the the property."""
        return self._prefix

    @property
    def default(self):
        """Default property value or None."""
        pass

    def list_value(self, value):
        if self._is_list:
            return [i.strip() for i in value.split(self._list_separator) \
                    if i.strip()]
        else:
            return [value]


def _key(prefix, value):
    return _EXACT_PREFIX + prefix + str(value).split('\n')[0][:243]


def _value(value):
    if isinstance(value, unicode):
        return value.encode('utf-8')
    elif isinstance(value, bool):
        return '1' if value else '0'
    elif not isinstance(value, basestring):
        return str(value)
    else:
        return value


def _extract_exact_search_terms(query, props):
    while True:
        exact_term = _EXACT_QUERY_RE.search(query)
        if exact_term is None:
            break
        query = query[:exact_term.start()] + query[exact_term.end():]
        term, __, value = exact_term.groups()
        props[term] = value
    return query
