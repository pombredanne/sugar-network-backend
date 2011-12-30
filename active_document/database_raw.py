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
import time
import shutil
import thread
import logging
import threading
from os.path import exists
from gettext import gettext as _

import xapian
import gobject

from active_document import util, env
from active_document.database_queue import DatabaseQueue
from active_document.util import enforce


# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z]+):=(")?((?(2)[^"]+|\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10

_writers = {}


def get(name, properties, crawler):
    """Get a `Writer` object to write to the database.

    :param name:
        database name
    :param properties:
        `Property` objects associated with the `Database`
    :param crawler:
        iterator function that should return (guid, props)
        for every existing document

    """
    if env.threading.value:
        writer = _writers.get(name)
        if writer is None:
            logging.debug('Create database writer for %s', name)
            writer = _writers[name] = _ThreadWriter(name, properties, crawler)
            write_thread = threading.Thread(target=writer.serve_forever)
            write_thread.daemon = True
            write_thread.start()
        return _ThreadReader(writer)
    else:
        writer = _writers[name] = _Writer(name, properties, crawler)
        writer.open()
        return writer


def shutdown():
    """Flush all write pending queues and close all databases."""
    while _writers:
        __, db = _writers.popitem()
        db.shutdown()


class _Reader(object):

    def __init__(self, name, properties):
        self._name = name
        self._properties = properties
        self._db = None

    @property
    def name(self):
        return self._name

    @property
    def properties(self):
        return self._properties

    def find(self, offset, limit, request, query, reply, order_by, group_by):
        if self._db is None:
            return [], 0

        start_timestamp = time.time()
        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        enquire = self._enquire(request, query, order_by, group_by)
        result = self._call_db(enquire.get_mset, offset, limit, check_at_least)
        total_count = result.get_matches_estimated()

        entries = []
        for hit in result:
            entry = {}
            #guid = hit.document.get_value(0)
            for name in reply or self.properties.keys():
                prop = self.properties.get(name)
                if prop is not None and prop.slot is not None:
                    entry[name] = hit.document.get_value(prop.slot)
                else:
                    # TODO
                    #entry[name] = self.get_property(guid, name)
                    pass
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

    def _enquire(self, request, query, order_by, group_by):
        enquire = xapian.Enquire(self._db)
        queries = []
        boolean_queries = []

        if query:
            query = self._extract_exact_search_terms(query, request)

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
            for order in order_by or []:
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

    def _extract_exact_search_terms(self, query, props):
        while True:
            exact_term = _EXACT_QUERY_RE.search(query)
            if exact_term is None:
                break
            query = query[:exact_term.start()] + query[exact_term.end():]
            term, __, value = exact_term.groups()
            props[term] = value
        return query


class _Writer(gobject.GObject, _Reader):
    """Write access to Xapian databases."""

    __gsignals__ = {
            #: Content of the database was changed
            'changed': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            #: Database was openned
            'openned': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            }

    def __init__(self, name, properties, crawler):
        gobject.GObject.__init__(self)
        _Reader.__init__(self, name, properties)

        self._crawler = crawler
        self._flush_timeout_hid = None
        self._pending_writes = 0

    def open(self):
        self._open(False)

    def shutdown(self):
        if self._db is None:
            return
        self._flush(True)
        self._db = None

    def replace(self, guid, props):
        props['guid'] = guid
        for name, prop in self.properties.items():
            value = props.get(name, prop.default)
            enforce(value is not None,
                    _('Property "%s" should be passed while creating new %s ' \
                            'document'),
                    name, self.name)
            props[name] = env.value(value)

        logging.debug('Store %s object: %r', self.name, props)

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self.properties.items():
            if prop.slot is not None:
                document.add_value(prop.slot, props[name])
            if prop.prefix:
                for value in prop.list_value(props[name]):
                    if prop.boolean:
                        document.add_boolean_term(env.term(prop.prefix, value))
                    else:
                        document.add_term(env.term(prop.prefix, value))
                    term_generator.index_text(value, 1, prop.prefix)
                    term_generator.increase_termpos()

        self._db.replace_document(env.term(env.GUID_PREFIX, guid), document)
        self._commit(False)

    def delete(self, guid):
        logging.debug('Delete "%s" document from %s', guid, self.name)
        self._db.delete_document(env.term(env.GUID_PREFIX, guid))
        self._commit(False)

    def _open(self, reset):
        index_path = env.index_path(self.name)

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

        if reset:
            self._save_layout()
            gobject.idle_add(self._populate, self._crawler())

        # Emit from idle_add to send signal in the main loop thread
        gobject.idle_add(self.emit, 'openned')

    def _populate(self, i):
        try:
            guid, props = i.next()
            self.replace(guid, props)
        except StopIteration:
            pass
        else:
            gobject.idle_add(self._populate, i)

    def _commit(self, flush):
        self._pending_writes += 1
        self._flush(flush)

    def _flush(self, flush):
        if self._flush_timeout_hid is not None:
            gobject.source_remove(self._flush_timeout_hid)
            self._flush_timeout_hid = None

        if flush or env.flush_threshold.value and \
                self._pending_writes >= env.flush_threshold.value:
            logging.debug('Flush %s: flush=%r _pending_writes=%r',
                    self.name, flush, self._pending_writes)
            if hasattr(self._db, 'commit'):
                self._db.commit()
            else:
                self._db.flush()
            self._pending_writes = 0

            # Emit from idle_add to send signal in the main loop thread
            gobject.idle_add(self.emit, 'changed')

        elif env.flush_timeout.value:
            self._flush_timeout_hid = gobject.timeout_add_seconds(
                    env.flush_timeout.value, lambda: self._flush(True))

        return False

    def _is_layout_stale(self):
        path = env.path(self.name, 'version')
        if not exists(path):
            return True
        version = file(path).read()
        return not version.isdigit() or int(version) != env.LAYOUT_VERSION

    def _save_layout(self):
        version = file(env.path(self.name, 'version'), 'w')
        version.write(str(env.LAYOUT_VERSION))
        version.close()


class _ThreadWriter(_Writer):

    def __init__(self, name, properties, crawler):
        _Writer.__init__(self, name, properties, crawler)
        self.queue = DatabaseQueue()

    def serve_forever(self):
        try:
            self.open()
            logging.debug('Start serving writes to %s', self.name)
            while True:
                self.queue.iteration(self)
        except Exception:
            util.exception(_('Database %s write thread died, ' \
                    'will abort the whole application'), self.name)
            thread.interrupt_main()

    def shutdown(self):
        self.queue.shutdown()
        _Writer.shutdown(self)

    def _flush(self, flush):
        _Writer._flush(self, flush)
        if self._pending_writes == 0:
            self.queue.flush()
        return False


class _ThreadReader(_Reader):

    def __init__(self, writer):
        _Reader.__init__(self, writer.name, writer.properties)
        self._writer = writer
        self._queue = writer.queue
        self._last_flush = 0

    def replace(self, guid, props):
        logging.debug('Push replace request to %s\'s queue for %s',
                self.name, guid)
        self._queue.put(_Writer.replace, guid, props)

    def delete(self, guid):
        logging.debug('Push delete request to %s\'s queue for %s',
                self.name, guid)
        self._queue.put(_Writer.delete, guid)

    def find(self, offset, limit, request, query, reply, order_by, group_by):
        if self._db is None:
            try:
                self._db = xapian.Database(env.index_path(self.name))
                self._last_flush = time.time()
            except xapian.DatabaseOpeningError:
                logging.debug('Cannot open RO database for %s', self.name)
                return [], 0
            else:
                logging.debug('Open read-only database for %s', self.name)

        result, last_flush, reopen = self._queue.put_wait(self._last_flush,
                _Reader.find, offset, limit, request, query, reply, order_by,
                group_by)

        if result is None:
            self._last_flush = last_flush
            if reopen:
                self._db.reopen()
            result = _Reader.find(self, offset, limit, request, query, reply,
                    order_by, group_by)

        return result

    def connect(self, *args, **kwargs):
        self._writer.connect(*args, **kwargs)
