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
from os.path import exists, join
from gettext import gettext as _

import gevent
import gevent.event
import xapian

from active_document import util, env, gthread
from active_document.metadata import ActiveProperty
from active_document.util import enforce


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
        self._root = root
        self._path = join(root, 'index')
        self._layout_path = join(self._path, 'layout')
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
        if self._db is None:
            _logger.warning(_('%s was called with not initialized db'),
                    self.find)
            return [], Total(0)

        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = query.offset + query.limit + 1

        enquire = self._enquire(query.request, query.query, query.order_by)
        result = self._call_db(enquire.get_mset, query.offset, query.limit,
                check_at_least)
        total = Total(result.get_matches_estimated())

        _logger.debug('Found in %s: %s time=%s total_count=%s parsed=%s',
                self.metadata.name, query, time.time() - start_timestamp,
                total.value, enquire.get_query())

        def iterate():
            for hit in result:
                props = {}
                for name in query.reply or self._props.keys():
                    prop = self._props.get(name)
                    if prop is None:
                        continue
                    if prop.slot is not None and prop.slot != 0:
                        value = hit.document.get_value(prop.slot)
                        if prop.typecast is int:
                            value = int(xapian.sortable_unserialise(value))
                        elif prop.typecast is float:
                            value = xapian.sortable_unserialise(value)
                        elif prop.typecast is bool:
                            value = bool(xapian.sortable_unserialise(value))
                        props[name] = value
                guid = hit.document.get_value(0)
                yield guid, props

        return iterate(), total

    def commit(self):
        """Flush index changes to the disk."""
        raise NotImplementedError()

    def _enquire(self, request, query, order_by):
        enquire = xapian.Enquire(self._db)
        queries = []
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
                    xapian.QueryParser.FLAG_WILDCARD,
                    '')
            queries.append(query)

        for name, value in request.items():
            value = str(value).strip()
            prop = self._props.get(name)
            enforce(prop is not None and prop.prefix,
                    _('Unknow search term %r for %r'),
                    name, self.metadata.name)
            query = xapian.Query(_term(prop.prefix, value))
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
                        _('Cannot sort using %r property of %r'),
                        order_by, self.metadata.name)
                sorter.add_value(prop.slot, reverse)
            # Sort by ascending GUID to make order predictable all time
            sorter.add_value(0, False)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            _logger.warning(_('In order to support sorting, ' \
                    'Xapian should be at least 1.2.0'))

        return enquire

    def _call_db(self, op, *args):
        tries = 0
        while True:
            try:
                return op(*args)
            except xapian.DatabaseError, error:
                if tries >= _REOPEN_LIMIT:
                    _logger.warning(_('Cannot open %r index'),
                            self.metadata.name)
                    raise
                _logger.debug('Fail to %r %r index, will reopen it %sth ' \
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

        self._pending_updates = 0
        self._commit_cond = gthread.Condition()
        self._commit_job = None

        self._open(False)
        self._commit_job = gevent.spawn(self._commit_handler)

    def close(self):
        """Flush index write pending queue and close the index."""
        if self._db is None:
            return
        self._commit()
        self._commit_job.kill()
        self._commit_job = None
        self._db = None

    def store(self, guid, properties, new, pre_cb=None, post_cb=None, *args):
        if pre_cb is not None:
            pre_cb(guid, properties, new)

        _logger.debug('Store %r object: %r', self.metadata.name, properties)

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self._props.items():
            value = guid if prop.slot == 0 else properties[name]

            if prop.slot is not None:
                if prop.typecast in [int, float, bool]:
                    add_value = xapian.sortable_serialise(value)
                else:
                    add_value = value
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
            post_cb(guid, properties, new, *args)

        self._check_for_commit()

    def delete(self, guid, post_cb=None, *args):
        _logger.debug('Delete %r document from %r',
                guid, self.metadata.name)

        self._db.delete_document(_term(env.GUID_PREFIX, guid))
        self._pending_updates += 1

        if post_cb is not None:
            post_cb(guid, *args)

        self._check_for_commit()

    def commit(self):
        self._commit()
        # Trigger condition to reset waiting for `index_flush_timeout` timeout
        self._commit_cond.notify(False)

    def _open(self, reset):
        if not reset and self._is_layout_stale():
            reset = True

        if reset:
            shutil.rmtree(self._path, ignore_errors=True)

        try:
            self._db = xapian.WritableDatabase(self._path,
                    xapian.DB_CREATE_OR_OPEN)
        except xapian.DatabaseError:
            if reset:
                util.exception(_('Unrecoverable error while opening %r ' \
                        'Xapian index'), self.metadata.name)
                raise
            else:
                util.exception(_('Cannot open Xapian index in %r, ' \
                        'will rebuild it'), self.metadata.name)
                self._open(True)

        if reset:
            self._save_layout()

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

    def _is_layout_stale(self):
        if not exists(self._layout_path):
            return True
        with file(self._layout_path) as f:
            version = f.read()
        return not version.isdigit() or int(version) != env.LAYOUT_VERSION

    def _save_layout(self):
        with file(self._layout_path, 'w') as f:
            f.write(str(env.LAYOUT_VERSION))

    def _check_for_commit(self):
        if env.index_flush_threshold.value > 0 and \
                self._pending_updates >= env.index_flush_threshold.value:
            # Avoid processing heavy commits in the same greenlet
            self._commit_cond.notify(True)

    def _commit_handler(self):
        if env.index_flush_timeout.value > 0:
            timeout = env.index_flush_timeout.value
        else:
            timeout = None

        while True:
            if self._commit_cond.wait(timeout) is not False:
                self._commit()


class Total(object):

    def __init__(self, value):
        self.value = value

    def __cmp__(self, other):
        if isinstance(other, Total):
            return cmp(self.value, other.value)
        else:
            return cmp(self.value, int(other))


def _term(prefix, value):
    return env.EXACT_PREFIX + prefix + str(value).split('\n')[0][:243]
