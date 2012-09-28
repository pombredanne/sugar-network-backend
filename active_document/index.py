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

from active_document import env
from active_document.metadata import ActiveProperty
from active_toolkit import util, coroutine, enforce


# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z0-9_]+):=(")?((?(2)[^"]+|\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10

_logger = logging.getLogger('active_document.index')


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
            if isinstance(prop, ActiveProperty):
                self._props[name] = prop

    @property
    def mtime(self):
        """UNIX seconds of the last `commit()` call."""
        if exists(self._mtime_path):
            return os.stat(self._mtime_path).st_mtime
        else:
            return 0

    def get_cached(self, guid):
        """Return cached document.

        Only in case if index support caching updates.

        :param guid:
            document GUID to get cache for
        :returns:
            dictionary with cached properties or `None`

        """
        pass

    def store(self, guid, properties, new, pre_cb=None, post_cb=None, *args):
        """Store new document in the index.

        :param guid:
            document's GUID to store
        :param properties:
            document's properties to store; for non new entities,
            not necessary all document's properties
        :param new:
            initial store for the document; `None` for merging from other nodes
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

    def find(self, query):
        """Search documents within the index.

        Function interface is the same as for `active_document.Document.find`.

        """
        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = query.offset + query.limit + 1

        enquire = self._enquire(query.request, query.query, query.order_by,
                query.group_by)
        mset = self._call_db(enquire.get_mset, query.offset, query.limit,
                check_at_least)

        _logger.debug('Found in %s: %s time=%s total=%s parsed=%s',
                self.metadata.name, query, time.time() - start_timestamp,
                mset.get_matches_estimated(), enquire.get_query())

        return mset

    def commit(self):
        """Flush index changes to the disk."""
        raise NotImplementedError()

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        queries = []
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
                if prop.slot is not None and \
                        prop.typecast in [int, float, bool]:
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
            queries.append(query)

        for name, value in request.items():
            prop = self._props.get(name)
            if prop is None or not prop.prefix:
                continue

            sub_queries = []
            not_queries = []
            for needle in value if type(value) in (tuple, list) else [value]:
                needle = prop.to_string(needle)[0]
                if needle.startswith('!'):
                    term = _term(prop.prefix, needle[1:])
                    not_queries.append(xapian.Query(term))
                elif needle.startswith('-'):
                    term = _term(prop.prefix, needle[1:])
                    and_not_queries.append(xapian.Query(term))
                else:
                    term = _term(prop.prefix, needle)
                    sub_queries.append(xapian.Query(term))

            if not_queries:
                not_query = xapian.Query(xapian.Query.OP_AND_NOT,
                        [xapian.Query(''),
                            xapian.Query(xapian.Query.OP_OR, not_queries)])
                sub_queries.append(not_query)

            if sub_queries:
                if len(sub_queries) == 1:
                    query = sub_queries[0]
                else:
                    query = xapian.Query(xapian.Query.OP_OR, sub_queries)
                if prop.boolean:
                    boolean_queries.append(query)
                else:
                    queries.append(query)

        final = None
        if queries:
            final = xapian.Query(xapian.Query.OP_AND, queries)
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
            if isinstance(prop, ActiveProperty) and prop.prefix:
                props[term] = value
        return query


class IndexWriter(IndexReader):
    """Write access to Xapian databases."""

    def __init__(self, root, metadata, commit_cb=None):
        IndexReader.__init__(self, root, metadata, commit_cb)

        self._lang = env.default_lang()
        self._pending_updates = 0
        self._commit_cond = coroutine.Event()
        self._commit_job = coroutine.spawn(self._commit_handler)

        # Let `_commit_handler()` call `wait()` to not miss immediate commit
        coroutine.dispatch()

        self._do_open()

    def close(self):
        """Flush index write pending queue and close the index."""
        if self._db is None:
            return
        self._commit()
        self._commit_job.kill()
        self._commit_job = None
        self._db = None

    def find(self, query):
        if self._db is None:
            self._do_open()
        return IndexReader.find(self, query)

    def store(self, guid, properties, new, pre_cb=None, post_cb=None, *args):
        if self._db is None:
            self._do_open()

        if pre_cb is not None:
            pre_cb(guid, properties, *args)

        _logger.debug('Index %r object: %r', self.metadata.name, properties)

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self._props.items():
            value = guid if prop.slot == 0 else properties[name]

            if prop.slot is not None:
                if prop.typecast in [int, float, bool]:
                    add_value = xapian.sortable_serialise(value)
                else:
                    if prop.localized:
                        value = value.get(self._lang) or \
                                value.get(env.DEFAULT_LANG) or ''
                    add_value = prop.to_string(value)[0]
                document.add_value(prop.slot, add_value)

            if prop.prefix or prop.full_text:
                for value in prop.to_string(value):
                    if prop.prefix:
                        if prop.boolean:
                            document.add_boolean_term(
                                    _term(prop.prefix, value))
                        else:
                            document.add_term(_term(prop.prefix, value))
                    if prop.full_text:
                        term_generator.index_text(value, 1, prop.prefix or '')
                    term_generator.increase_termpos()

        self._db.replace_document(_term(env.GUID_PREFIX, guid), document)
        self._pending_updates += 1

        if post_cb is not None:
            post_cb(guid, properties, *args)

        self._check_for_commit()

    def delete(self, guid, post_cb=None, *args):
        if self._db is None:
            self._do_open()

        _logger.debug('Delete %r document from %r',
                guid, self.metadata.name)

        self._db.delete_document(_term(env.GUID_PREFIX, guid))
        self._pending_updates += 1

        if post_cb is not None:
            post_cb(guid, *args)

        self._check_for_commit()

    def commit(self):
        if self._db is None:
            return
        self._commit()
        # Trigger condition to reset waiting for `index_flush_timeout` timeout
        self._commit_cond.set()

    def _do_open(self):
        try:
            self._db = xapian.WritableDatabase(self._path,
                    xapian.DB_CREATE_OR_OPEN)
        except xapian.DatabaseError:
            util.exception('Cannot open Xapian index in %r, will rebuild it',
                    self.metadata.name)
            shutil.rmtree(self._path, ignore_errors=True)
            self._db = xapian.WritableDatabase(self._path,
                    xapian.DB_CREATE_OR_OPEN)

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
        with file(self._mtime_path, 'w'):
            pass
        self._pending_updates = 0

        _logger.debug('Commit %r changes took %s seconds',
                self.metadata.name, time.time() - ts)

        if self._commit_cb is not None:
            self._commit_cb()

    def _check_for_commit(self):
        if env.index_flush_threshold.value > 0 and \
                self._pending_updates >= env.index_flush_threshold.value:
            # Avoid processing heavy commits in the same coroutine
            self._commit_cond.set()

    def _commit_handler(self):
        if env.index_flush_timeout.value > 0:
            timeout = env.index_flush_timeout.value
        else:
            timeout = None

        while True:
            self._commit_cond.wait(timeout)
            self._commit()
            self._commit_cond.clear()


def _term(prefix, value):
    return env.EXACT_PREFIX + prefix + str(value).split('\n')[0][:243]
