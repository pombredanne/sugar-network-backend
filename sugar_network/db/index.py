# Copyright (C) 2011-2012 Aleksey Lim
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
from os.path import exists, join

import xapian

from sugar_network.db.metadata import GUID_PREFIX
from sugar_network.toolkit import Option, coroutine, exception, enforce


index_flush_timeout = Option(
        'flush index index after specified seconds since the last change',
        default=60, type_cast=int)

index_flush_threshold = Option(
        'flush index every specified changes',
        default=1024, type_cast=int)

index_write_queue = Option(
        'if active-document is being used for the scheme with one writer '
            'process and multiple reader processes, this option specifies '
            'the writer\'s queue size',
        default=256, type_cast=int)

# Additional Xapian term prefix for exact search terms
_EXACT_PREFIX = 'X'

# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z0-9_]+):=(")?((?(2)[^"]+|\\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10

_logger = logging.getLogger('db.index')


class IndexReader(object):
    """Read-only access to an index."""

    def __init__(self, root, metadata, commit_cb=None):
        self.metadata = metadata
        self._db = None
        self._props = {}
        self._path = root
        self._mtime_path = join(self._path, 'mtime')
        self._commit_cb = commit_cb

        for name, prop in self.metadata.items():
            if prop.indexed:
                self._props[name] = prop

    @property
    def mtime(self):
        """UNIX seconds of the last `commit()` call."""
        return int(os.stat(self._mtime_path).st_mtime)

    def ensure_open(self):
        if not exists(self._mtime_path):
            with file(self._mtime_path, 'w'):
                pass
            # Outter code should understand the initial state
            os.utime(self._mtime_path, (0, 0))

    def get_cached(self, guid):
        """Return cached document.

        Only in case if index support caching updates.

        :param guid:
            document GUID to get cache for
        :returns:
            dictionary with cached properties or `None`

        """
        pass

    def store(self, guid, properties, pre_cb=None, post_cb=None, *args):
        """Store new document in the index.

        :param guid:
            document's GUID to store
        :param properties:
            document's properties to store; for non new entities,
            not necessary all document's properties
        :param pre_cb:
            callback to execute before storing;
            will be called with passing `guid` and `properties`
        :param post_cb:
            callback to execute after storing;
            will be called with passing `guid` and `properties`

        """
        raise NotImplementedError()

    def delete(self, guid, post_cb=None, *args):
        """Delete a document from the index.

        :param guid:
            document's GUID to remove
        :param post_cb:
            callback to execute after deleting;
            will be called with passing `guid`

        """
        raise NotImplementedError()

    def find(self, offset=0, limit=None, query='', reply=('guid',),
            order_by=None, no_cache=False, group_by=None, **request):
        """Search resources within the index.

        The result will be an array of dictionaries with found documents'
        properties.

        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param reply:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            property name to sort resulting list; might be prefixed with ``+``
            (or without any prefixes) for ascending order, and ``-`` for
            descending order
        :param group_by:
            property name to group resulting list by; no groupping by default
        :param request:
            a dictionary with property values to restrict the search
        :returns:
            a tuple of (`documents`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        self.ensure_open()

        start_timestamp = time.time()
        if limit is None:
            limit = self._db.get_doccount()
        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        enquire = self._enquire(request, query, order_by, group_by)
        mset = self._call_db(enquire.get_mset, offset, limit, check_at_least)

        _logger.debug('Found in %s: query=%r time=%s total=%s parsed=%s',
                self.metadata.name, query, time.time() - start_timestamp,
                mset.get_matches_estimated(), enquire.get_query())

        return mset

    def commit(self):
        """Flush index changes to the disk."""
        raise NotImplementedError()

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        all_queries = []
        and_not_queries = []
        boolean_queries = []

        if query:
            query = self._extract_exact_search_terms(query, request)

        if query:
            parser = xapian.QueryParser()
            parser.set_database(self._db)
            for name, prop in self._props.items():
                if not prop.prefix:
                    continue
                if prop.boolean:
                    parser.add_boolean_prefix(name, prop.prefix)
                else:
                    parser.add_prefix(name, prop.prefix)
                parser.add_prefix('', prop.prefix)
                if prop.slot is not None:
                    value_range = xapian.NumberValueRangeProcessor(
                            prop.slot, name + ':')
                    parser.add_valuerangeprocessor(value_range)
            parser.add_prefix('', '')
            query = parser.parse_query(query,
                    xapian.QueryParser.FLAG_PHRASE |
                    xapian.QueryParser.FLAG_BOOLEAN |
                    xapian.QueryParser.FLAG_LOVEHATE |
                    xapian.QueryParser.FLAG_PARTIAL |
                    xapian.QueryParser.FLAG_WILDCARD |
                    xapian.QueryParser.FLAG_PURE_NOT,
                    '')
            all_queries.append(query)

        for name, value in request.items():
            queries = sub_queries = []
            if name.startswith('!'):
                queries = and_not_queries
                name = name[1:]
            elif name.startswith('not_'):
                queries = and_not_queries
                name = name[4:]
            prop = self._props.get(name)
            if prop is None or not prop.prefix:
                continue
            for needle in value if type(value) in (tuple, list) else [value]:
                if needle is None:
                    continue
                needle = prop.decode(needle)
                queries.append(xapian.Query(_term(prop.prefix, needle)))
            if len(sub_queries) == 1:
                all_queries.append(sub_queries[0])
            elif sub_queries:
                all_queries.append(
                        xapian.Query(xapian.Query.OP_OR, sub_queries))

        final = None
        if len(all_queries) == 1:
            final = all_queries[0]
        elif all_queries:
            final = xapian.Query(xapian.Query.OP_AND, all_queries)
        if boolean_queries:
            query = xapian.Query(xapian.Query.OP_AND, boolean_queries)
            if final is None:
                final = query
            else:
                final = xapian.Query(xapian.Query.OP_FILTER, [final, query])
        if final is None:
            final = xapian.Query('')
        for i in and_not_queries:
            final = xapian.Query(xapian.Query.OP_AND_NOT, [final, i])
        enquire.set_query(final)

        if hasattr(xapian, 'MultiValueKeyMaker'):
            sorter = xapian.MultiValueKeyMaker()
            if order_by:
                if order_by.startswith('+'):
                    reverse = False
                    order_by = order_by[1:]
                elif order_by.startswith('-'):
                    reverse = True
                    order_by = order_by[1:]
                else:
                    reverse = False
                prop = self._props.get(order_by)
                enforce(prop is not None and prop.slot is not None,
                        'Cannot sort using %r property of %r',
                        order_by, self.metadata.name)
                sorter.add_value(prop.slot, reverse)
            # Sort by ascending GUID to make order predictable all time
            sorter.add_value(0, False)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            _logger.warning('In order to support sorting, '
                    'Xapian should be at least 1.2.0')

        if group_by:
            prop = self._props.get(group_by)
            enforce(prop is not None and prop.slot is not None,
                    'Cannot group by %r property of %r',
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
                    _logger.warning('Cannot open %r index',
                            self.metadata.name)
                    raise
                _logger.debug('Fail to %r %r index, will reopen it %sth '
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
            if prop.indexed and prop.prefix:
                props[term] = value
        return query


class IndexWriter(IndexReader):
    """Write access to Xapian databases."""

    def __init__(self, root, metadata, commit_cb=None):
        IndexReader.__init__(self, root, metadata, commit_cb)

        self._pending_updates = 0
        self._commit_cond = coroutine.Event()
        self._commit_job = coroutine.spawn(self._commit_handler)

        # Let `_commit_handler()` call `wait()` to not miss immediate commit
        coroutine.dispatch()
        self.ensure_open()

    def close(self):
        """Flush index write pending queue and close the index."""
        if self._db is None:
            return
        self._commit()
        self._commit_job.kill()
        self._commit_job = None
        self._db = None

    def store(self, guid, properties, pre_cb=None, post_cb=None, *args):
        self.ensure_open()

        if pre_cb is not None:
            properties = pre_cb(guid, properties, *args)

        _logger.debug('Index %r object: %r', self.metadata.name, properties)

        doc = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(doc)

        for name, prop in self._props.items():
            value = guid \
                    if prop.slot == 0 \
                    else properties.get(name, prop.default)

            if prop.slot is not None:
                doc.add_value(prop.slot, prop.slotting(value))

            if prop.prefix or prop.full_text:
                for value_ in prop.encode(value):
                    if prop.prefix:
                        if prop.boolean:
                            doc.add_boolean_term(_term(prop.prefix, value_))
                        else:
                            doc.add_term(_term(prop.prefix, value_))
                    if prop.full_text:
                        term_generator.index_text(value_, 1, prop.prefix or '')
                    term_generator.increase_termpos()

        self._db.replace_document(_term(GUID_PREFIX, guid), doc)
        self._pending_updates += 1

        if post_cb is not None:
            post_cb(*args)

        self._check_for_commit()

    def delete(self, guid, post_cb=None, *args):
        self.ensure_open()

        _logger.debug('Delete %r document from %r',
                guid, self.metadata.name)

        self._db.delete_document(_term(GUID_PREFIX, guid))
        self._pending_updates += 1

        if post_cb is not None:
            post_cb(*args)

        self._check_for_commit()

    def commit(self):
        if self._db is None:
            return
        self._commit()
        # Trigger condition to reset waiting for `index_flush_timeout` timeout
        self._commit_cond.set()

    def ensure_open(self):
        if self._db is None:
            try:
                self._db = xapian.WritableDatabase(self._path,
                        xapian.DB_CREATE_OR_OPEN)
            except xapian.DatabaseError:
                exception('Cannot open Xapian index in %r, will rebuild it',
                        self.metadata.name)
                shutil.rmtree(self._path, ignore_errors=True)
                self._db = xapian.WritableDatabase(self._path,
                        xapian.DB_CREATE_OR_OPEN)
        IndexReader.ensure_open(self)

    def _commit(self):
        if self._pending_updates <= 0:
            return

        _logger.debug('Commiting %s changes of %r index to the disk',
                self._pending_updates, self.metadata.name)
        ts = time.time()

        if hasattr(self._db, 'commit'):
            self._db.commit()
        else:
            self._db.flush()

        checkpoint = time.time()
        os.utime(self._mtime_path, (checkpoint, checkpoint))
        self._pending_updates = 0

        _logger.debug('Commit to %r took %s seconds',
                self.metadata.name, checkpoint - ts)

        if self._commit_cb is not None:
            self._commit_cb()

    def _check_for_commit(self):
        if index_flush_threshold.value > 0 and \
                self._pending_updates >= index_flush_threshold.value:
            # Avoid processing heavy commits in the same coroutine
            self._commit_cond.set()

    def _commit_handler(self):
        if index_flush_timeout.value > 0:
            timeout = index_flush_timeout.value
        else:
            timeout = None

        while True:
            self._commit_cond.wait(timeout)
            self._commit()
            self._commit_cond.clear()


def _term(prefix, value):
    return _EXACT_PREFIX + prefix + str(value).split('\n')[0][:243]
