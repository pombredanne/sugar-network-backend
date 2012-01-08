# Copyright (C) 2011-2012, Aleksey Lim
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
import re
import time
import shutil
import logging
from os.path import exists
from gettext import gettext as _

import xapian

from active_document import util, env
from active_document.metadata import IndexedProperty, CounterProperty
from active_document.util import enforce


# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z0-9_]+):=(")?((?(2)[^"]+|\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10


_logger = logging.getLogger('ad.index')


class IndexReader(object):
    """Read-only access to an index."""

    def __init__(self, metadata):
        self.metadata = metadata
        self._db = None
        self._props = {}

        for name, prop in self.metadata.items():
            if isinstance(prop, IndexedProperty):
                self._props[name] = prop

    def store(self, guid, properties, new, pre_cb=None, post_cb=None):
        """Store new document in the index.

        :param guid:
            document's GUID to store
        :param properties:
            document's properties to store; for non new entities,
            not necessary all document's properties
        :param new:
            is this initial store for the document
        :param pre_cb:
            callback to execute before storing;
            will be called with passing `guid` and `properties`
        :param post_cb:
            callback to execute after storing;
            will be called with passing `guid` and `properties`

        """
        raise NotImplementedError()

    def delete(self, guid, post_cb=None):
        """Delete a document from the index.

        :param guid:
            document's GUID to remove
        :param post_cb:
            callback to execute after deleting;
            will be called with passing `guid`

        """
        raise NotImplementedError()

    def find(self, offset, limit, request=None, query=None, reply=None,
            order_by=None, group_by=None):
        """Search documents within the index.

        Function interface is the same as for `active_document.Document.find`.

        """
        if self._db is None:
            _logger.warning(_('%s was called with not initialized db'),
                    self.find)
            return [], 0

        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        if request is None:
            request = {}

        enquire = self._enquire(request, query, order_by, group_by)
        result = self._call_db(enquire.get_mset, offset, limit, check_at_least)
        total_count = result.get_matches_estimated()

        documents = []
        for hit in result:
            props = {}
            for name in reply or self._props.keys():
                prop = self._props.get(name)
                if prop is None:
                    _logger.warning(_('Unknown property name "%s" for %s ' \
                            'to return from find'), name, self.metadata.name)
                    continue
                if prop.slot is not None and prop.slot != 0:
                    props[name] = hit.document.get_value(prop.slot)
            if group_by:
                props['grouped'] = hit.collapse_count + 1
            guid = hit.document.get_value(0)
            documents.append(self.metadata.to_document(guid, props))

        _logger.debug('Find in %s: offset=%s limit=%s request=%r query=%r ' \
                'order_by=%r group_by=%r time=%s documents=%s ' \
                'total_count=%s parsed=%s',
                self.metadata.name, offset, limit, request, query, order_by,
                group_by, time.time() - start_timestamp, len(documents),
                total_count, enquire.get_query())

        return documents, total_count

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        queries = []
        boolean_queries = []

        if query:
            query = self._extract_exact_search_terms(query, request)

        if query:
            parser = xapian.QueryParser()
            parser.set_database(self._db)
            for name, prop in self._props.items():
                if prop.prefix:
                    if prop.boolean:
                        parser.add_boolean_prefix(name, prop.prefix)
                    else:
                        parser.add_prefix(name, prop.prefix)
                    parser.add_prefix('', prop.prefix)
            parser.add_prefix('', '')
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
            prop = self._props.get(name)
            enforce(prop is not None and prop.prefix,
                    _('Unknow search term "%s" for %s'),
                    name, self.metadata.name)
            query = xapian.Query(env.term(prop.prefix, value))
            if prop.boolean:
                boolean_queries.append(query)
            else:
                queries.append(query)

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
            for order in order_by or []:
                if order.startswith('+'):
                    reverse = False
                    order = order[1:]
                elif order.startswith('-'):
                    reverse = True
                    order = order[1:]
                else:
                    reverse = False
                prop = self._props.get(order)
                enforce(prop is not None and prop.slot is not None,
                        _('Cannot sort using "%s" property of %s'),
                        order, self.metadata.name)
                sorter.add_value(prop.slot, reverse)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            _logger.warning(_('In order to support sorting, ' \
                    'Xapian should be at least 1.2.0'))

        if group_by:
            prop = self._props.get(group_by)
            enforce(prop is not None and prop.slot is not None,
                    _('Cannot group by "%s" property in %s'),
                    group_by, self.metadata.name)
            enquire.set_collapse_key(prop.slot)

        return enquire

    def _call_db(self, op, *args):
        tries = 0
        while True:
            try:
                return op(*args)
            except xapian.DatabaseError, error:
                if tries >= _REOPEN_LIMIT:
                    _logger.warning(_('Cannot open %s index'),
                            self.metadata.name)
                    raise
                _logger.debug('Fail to %r %s index, will reopen it %sth ' \
                        'time: %s', op, self.metadata.name, tries, error)
                time.sleep(tries * .1)
                self._db.reopen()
                tries += 1

    def _extract_exact_search_terms(self, query, props):
        while True:
            exact_term = _EXACT_QUERY_RE.search(query)
            if exact_term is None:
                break
            query = query[:exact_term.start()] + query[exact_term.end():]
            term, __, value = exact_term.groups()
            prop = self.metadata.get(term)
            if isinstance(prop, IndexedProperty) and prop.prefix:
                props[term] = value
        return query


class IndexWriter(IndexReader):
    """Write access to Xapian databases."""

    def __init__(self, metadata):
        IndexReader.__init__(self, metadata)
        self._pending_writes = 0
        self._open(False)

    @property
    def mtime(self):
        """UNIX seconds of the last `commit()` call."""
        path = self.metadata.path('stamp')
        if exists(path):
            return os.stat(path).st_mtime
        else:
            return 0

    def close(self):
        """Flush index write pending queue and close the index."""
        if self._db is None:
            return
        self.commit()
        self._db = None

    def store(self, guid, properties, new, pre_cb=None, post_cb=None):
        if pre_cb is not None:
            pre_cb(guid, properties)

        _logger.debug('Store %s object: %r', self.metadata.name, properties)

        for name, value in properties.items():
            prop = self.metadata[name]
            if not isinstance(prop, CounterProperty):
                continue
            try:
                int(value)
            except ValueError:
                raise RuntimeError(_('Counter property "%s" in %s ' \
                        'is not an integer value') % \
                        (name, self.metadata.name))

        if not new:
            documents, __ = self.find(0, 1, {'guid': guid})
            enforce(len(documents) == 1,
                    _('Cannot find "%s" in %s to store'),
                    guid, self.metadata.name)
            existing_doc = documents[0]
            for name, prop in self._props.items():
                if prop.slot is None:
                    continue
                if name not in properties:
                    properties[name] = existing_doc[name]
                elif isinstance(prop, CounterProperty):
                    properties[name] = str(
                            int(existing_doc[name] or '0') + \
                            int(properties[name]))

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self._props.items():
            value = guid if prop.slot == 0 else properties[name]
            if prop.slot is not None:
                document.add_value(prop.slot, value)
            if prop.prefix or prop.full_text:
                for value in prop.list_value(value):
                    if prop.prefix:
                        if prop.boolean:
                            document.add_boolean_term(
                                    env.term(prop.prefix, value))
                        else:
                            document.add_term(env.term(prop.prefix, value))
                    if prop.full_text:
                        term_generator.index_text(value, 1, prop.prefix or '')
                    term_generator.increase_termpos()

        self._db.replace_document(env.term(env.GUID_PREFIX, guid), document)
        self._commit()

        if post_cb is not None:
            post_cb(guid, properties)

    def delete(self, guid, post_cb=None):
        _logger.debug('Delete "%s" document from %s', guid, self.metadata.name)
        self._db.delete_document(env.term(env.GUID_PREFIX, guid))
        self._commit()
        if post_cb is not None:
            post_cb(guid)

    def commit(self):
        """Flush index changes to the disk."""
        ts = time.time()
        _logger.debug('Commiting %s to the disk', self.metadata.name)

        if hasattr(self._db, 'commit'):
            self._db.commit()
        else:
            self._db.flush()
        self._touch_stamp()

        _logger.debug('Commit %s changes took %s seconds',
                self.metadata.name, time.time() - ts)

    def _open(self, reset):
        if not reset and self._is_layout_stale():
            reset = True
        if reset:
            self._wipe_out()

        try:
            self._db = xapian.WritableDatabase(self.metadata.index_path(),
                    xapian.DB_CREATE_OR_OPEN)
        except xapian.DatabaseError:
            if reset:
                util.exception(_('Unrecoverable error while opening %s ' \
                        'Xapian index'), self.metadata.name)
                raise
            else:
                util.exception(_('Cannot open Xapian index in %s, ' \
                        'will rebuild it'), self.metadata.name)
                self._open(True)

        if reset:
            self._save_layout()

    def _commit(self):
        self._pending_writes += 1
        if env.index_flush_threshold.value and \
                self._pending_writes >= env.index_flush_threshold.value:
            self.commit()
            self._pending_writes = 0

    def _is_layout_stale(self):
        path = self.metadata.path('version')
        if not exists(path):
            return True
        layout = file(path)
        version = layout.read()
        layout.close()
        return not version.isdigit() or int(version) != env.LAYOUT_VERSION

    def _save_layout(self):
        version = file(self.metadata.path('version'), 'w')
        version.write(str(env.LAYOUT_VERSION))
        version.close()

    def _touch_stamp(self):
        stamp = file(self.metadata.path('stamp'), 'w')
        # Xapian's flush uses fsync
        # so, it is a good idea to do the same for stamp file
        os.fsync(stamp.fileno())
        stamp.close()

    def _wipe_out(self):
        shutil.rmtree(self.metadata.index_path(), ignore_errors=True)
        path = self.metadata.path('version')
        if exists(path):
            os.unlink(path)
        path = self.metadata.path('stamp')
        if exists(path):
            os.unlink(path)
