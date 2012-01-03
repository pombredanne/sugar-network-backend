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
from active_document.index_queue import IndexQueue, NoPut
from active_document.util import enforce


# The regexp to extract exact search terms from a query string
_EXACT_QUERY_RE = re.compile('([a-zA-Z0-9_]+):=(")?((?(2)[^"]+|\S+))(?(2)")')

# How many times to call Xapian database reopen() before fail
_REOPEN_LIMIT = 10

_writers = {}


def get_index(metadata):
    """Get access to an index.

    :param metadata:
        `Metadata` object, that describes the document, to get index for
    :returns:
        `Index` object

    """
    writer = _get_writer(metadata)
    if isinstance(writer, _ThreadWriter):
        return _ThreadReader(writer)
    else:
        return writer


def connect_to_index(metadata, cb, *args):
    """Connect to changes in the index.

    Callback function will be triggered on GObject signals when something
    was changed in the index and clients need to retry requests.

    :param metadata:
        `Metadata` object, that describes the document
    :param cb:
        callback to call on index changes
    :param args:
        optional arguments to pass to `cb`

    """
    _get_writer(metadata).connect('changed', cb, *args)


def close_indexes():
    """Flush all write pending queues and close all closes."""
    while _writers:
        __, db = _writers.popitem()
        db.close()


class Index(object):
    """Public interface for indexes."""

    def store(self, guid, properties, new):
        """Store new document in the index.

        :param guid:
            document's GUID to store
        :param properties:
            document's properties to store; for non new entities,
            not necessary all document's properties
        :param new:
            is this initial store for the document

        """
        raise NotImplementedError()

    def delete(self, guid):
        """Delete a document from the index.

        :param guid:
            document's GUID to remove

        """
        raise NotImplementedError()

    def find(self, offset, limit, request=None, query=None, reply=None,
            order_by=None, group_by=None):
        """Search documents within the index.

        Function interface is the same as for `active_document.Document.find`.

        """
        raise NotImplementedError()


class _Reader(Index):

    def __init__(self, metadata):
        self.metadata = metadata
        self._db = None

    def store(self, guid, properties, new):
        raise NotImplementedError()

    def delete(self, guid):
        raise NotImplementedError()

    def find(self, offset, limit, request=None, query=None, reply=None,
            order_by=None, group_by=None):
        if self._db is None:
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
            for name in reply or self.metadata.keys():
                prop = self.metadata.get(name)
                if prop is None:
                    logging.warning(_('Unknown property name "%s" for %s ' \
                            'to return from find'), name, self.metadata.name)
                    continue
                if prop.slot is not None and prop.slot != 0:
                    props[name] = hit.document.get_value(prop.slot)
            if group_by:
                props['grouped'] = hit.collapse_count + 1
            guid = hit.document.get_value(0)
            documents.append(self.metadata.to_document(guid, props))

        logging.debug('Find in %s: offset=%s limit=%s request=%r query=%r ' \
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
            for name, prop in self.metadata.items():
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
            prop = self.metadata.get(name)
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
                prop = self.metadata.get(order)
                enforce(prop is not None and prop.slot is not None,
                        _('Cannot sort using "%s" property of %s'),
                        order, self.metadata.name)
                sorter.add_value(prop.slot, reverse)
            enquire.set_sort_by_key(sorter, reverse=False)
        else:
            logging.warning(_('In order to support sorting, ' \
                    'Xapian should be at least 1.2.0'))

        if group_by:
            prop = self.metadata.get(group_by)
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
                    logging.warning(_('Cannot open %s index'),
                            self.metadata.name)
                    raise
                logging.debug('Fail to %r %s index, will reopen it %sth ' \
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
            if term in self.metadata and self.metadata[term].prefix:
                props[term] = value
        return query


class _Writer(gobject.GObject, _Reader):
    """Write access to Xapian databases."""

    __gsignals__ = {
            #: Content of the index was changed
            'changed': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            #: Index was openned
            'openned': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            }

    def __init__(self, metadata):
        gobject.GObject.__init__(self)
        _Reader.__init__(self, metadata)

        self._flush_timeout_hid = None
        self._pending_writes = 0

    def open(self):
        self._open(False)

    def close(self):
        if self._db is None:
            return
        self._flush(True)
        self._db = None

    def store(self, guid, properties, new):
        logging.debug('Store %s object: %r', self.metadata.name, properties)

        if not new:
            documents, __ = self.find(0, 1, {'guid': guid})
            enforce(len(documents) == 1,
                    _('Cannot find "%s" in %s to store'),
                    guid, self.metadata.name)
            existing_doc = documents[0]
            for name in self.metadata.keys():
                if name not in properties:
                    properties[name] = existing_doc[name]

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self.metadata.items():
            if not prop.indexed:
                continue
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
        self._commit(False)

    def delete(self, guid):
        logging.debug('Delete "%s" document from %s', guid, self.metadata.name)
        self._db.delete_document(env.term(env.GUID_PREFIX, guid))
        self._commit(False)

    def _open(self, reset):
        index_path = env.index_path(self.metadata.name)

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
            gobject.idle_add(self._populate, self.metadata.crawler())

        # Emit from idle_add to send signal in the main loop thread
        gobject.idle_add(self.emit, 'openned')

    def _populate(self, i):
        try:
            guid, props = i.next()
            self.store(guid, props, True)
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

        if flush or env.index_flush_threshold.value and \
                self._pending_writes >= env.index_flush_threshold.value:
            logging.debug('Flush %s: flush=%r _pending_writes=%r',
                    self.metadata.name, flush, self._pending_writes)
            if hasattr(self._db, 'commit'):
                self._db.commit()
            else:
                self._db.flush()
            self._pending_writes = 0

            # Emit from idle_add to send signal in the main loop thread
            gobject.idle_add(self.emit, 'changed')

        elif env.index_flush_timeout.value:
            self._flush_timeout_hid = gobject.timeout_add_seconds(
                    env.index_flush_timeout.value, lambda: self._flush(True))

        return False

    def _is_layout_stale(self):
        path = env.path(self.metadata.name, 'version')
        if not exists(path):
            return True
        version = file(path).read()
        return not version.isdigit() or int(version) != env.LAYOUT_VERSION

    def _save_layout(self):
        version = file(env.path(self.metadata.name, 'version'), 'w')
        version.write(str(env.LAYOUT_VERSION))
        version.close()


class _ThreadWriter(_Writer):

    def __init__(self, metadata):
        _Writer.__init__(self, metadata)
        self.queue = IndexQueue()

    def serve_forever(self):
        try:
            self.open()
            logging.debug('Start serving writes to %s', self.metadata.name)
            while True:
                self.queue.iteration(self)
        except Exception:
            util.exception(_('Index %s write thread died, ' \
                    'will abort the whole application'), self.metadata.name)
            thread.interrupt_main()

    def close(self):
        self.queue.close()
        _Writer.close(self)

    def _flush(self, flush):
        _Writer._flush(self, flush)
        if self._pending_writes == 0:
            self.queue.flush()
        return False


class _ThreadReader(_Reader):

    def __init__(self, writer):
        _Reader.__init__(self, writer.metadata)
        self._writer = writer
        self._queue = writer.queue
        self._last_flush = 0

    def store(self, guid, props, new):
        logging.debug('Push store request to %s\'s queue for %s',
                self.metadata.name, guid)
        self._queue.put(_Writer.store, guid, props, new)

    def delete(self, guid):
        logging.debug('Push delete request to %s\'s queue for %s',
                self.metadata.name, guid)
        self._queue.put(_Writer.delete, guid)

    def find(self, offset, limit, request=None, query=None, reply=None,
            order_by=None, group_by=None):
        if self._db is None:
            try:
                self._db = xapian.Database(env.index_path(self.metadata.name))
                self._last_flush = time.time()
            except xapian.DatabaseOpeningError:
                logging.debug('Cannot open RO index for %s',
                        self.metadata.name)
                return [], 0
            else:
                logging.debug('Open read-only index for %s',
                        self.metadata.name)

        try:
            return self._queue.put_wait(_Reader.find,
                    offset, limit, request, query, reply, order_by, group_by)
        except NoPut, error:
            if error.last_flush > self._last_flush:
                self._last_flush = error.last_flush
                self._db.reopen()
            return _Reader.find(self,
                    offset, limit, request, query, reply, order_by, group_by)

    def connect(self, *args, **kwargs):
        self._writer.connect(*args, **kwargs)


def _get_writer(metadata):
    writer = _writers.get(metadata.name)

    if writer is None:
        if env.index_pool.value > 0:
            logging.debug('Create index writer for %s', metadata.name)
            writer = _writers[metadata.name] = _ThreadWriter(metadata)
            write_thread = threading.Thread(target=writer.serve_forever)
            write_thread.daemon = True
            write_thread.start()
        else:
            writer = _Writer(metadata)
            writer.open()
        _writers[metadata.name] = writer

    return writer
