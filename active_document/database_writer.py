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

import shutil
import logging
import threading
from os.path import exists
from gettext import gettext as _

import xapian
import gobject

from active_document import util, env
from active_document.database_proxy import DatabaseProxy
from active_document.util import enforce


_writers = []


def get_writer(name, properties, crawler):
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
        logging.debug('Create database writer for %s', name)
        proxy = DatabaseProxy(Writer(name, properties, crawler))
        write_thread = threading.Thread(target=proxy.serve_forever)
        write_thread.daemon = True
        write_thread.start()
        _writers.append(proxy)
        return proxy
    else:
        writer = Writer(name, properties, crawler)
        writer.open()
        _writers.append(writer)
        return writer


def shutdown():
    """Flush all write pending queues and close all databases."""
    while _writers:
        _writers.pop().shutdown()


class Writer(gobject.GObject):
    """Write access to Xapian databases."""

    __gsignals__ = {
            #: Content of the database was changed
            'changed': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            #: Database was openned
            'openned': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            }

    def __init__(self, name, properties, crawler):
        gobject.GObject.__init__(self)

        self._name = name
        self._properties = properties
        self._crawler = crawler
        self._db = None
        self._flush_timeout_hid = None
        self._pending_writes = 0

    @property
    def name(self):
        """Xapian database name."""
        return self._name

    def open(self):
        """Open the database."""
        self._open(False)

    def shutdown(self):
        """Close the database and flush all pending changes."""
        if self._db is None:
            return
        self._commit(True)
        self._db = None

    def create(self, guid, props):
        """Create new document.

        :param guid:
            document GUID to create
        :param props:
            document properties

        """
        self.update(guid, props)

    def update(self, guid, props):
        """Update properties of existing document.

        :param guid:
            document GUID to update
        :param props:
            properties to update, not necessary all document properties

        """
        props['guid'] = guid
        for name, prop in self._properties.items():
            value = props.get(name, prop.default)
            enforce(value is not None,
                    _('Property "%s" should be passed while creating new %s ' \
                            'document'),
                    name, self._name)
            props[name] = env.value(value)

        logging.debug('Store %s object: %r', self._name, props)

        document = xapian.Document()
        term_generator = xapian.TermGenerator()
        term_generator.set_document(document)

        for name, prop in self._properties.items():
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
        """Delete document.

        :props guid:
            document GUID to delete

        """
        logging.debug('Delete "%s" document from %s', guid, self._name)
        self._db.delete_document(env.term(env.GUID_PREFIX, guid))
        self._commit(False)

    def get_reader(self):
        """Open the same database for reading only."""
        return self._db

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
            self.update(guid, props)
        except StopIteration:
            pass
        else:
            gobject.idle_add(self._populate, i)

    def _commit(self, flush):
        if self._flush_timeout_hid is not None:
            gobject.source_remove(self._flush_timeout_hid)
            self._flush_timeout_hid = None

        self._pending_writes += 1

        if flush or env.flush_threshold.value and \
                self._pending_writes >= env.flush_threshold.value:
            logging.debug('Commit %s: flush=%r _pending_writes=%r',
                    self._name, flush, self._pending_writes)
            if hasattr(self._db, 'commit'):
                self._db.commit()
            else:
                self._db.flush()
            self._pending_writes = 0

            # Emit from idle_add to send signal in the main loop thread
            gobject.idle_add(self.emit, 'changed')

        elif env.flush_timeout.value:
            self._flush_timeout_hid = gobject.timeout_add_seconds(
                    env.flush_timeout.value, lambda: self._commit(True))

        return False

    def _is_layout_stale(self):
        path = env.path(self._name, 'version')
        if not exists(path):
            return True
        version = file(path).read()
        return not version.isdigit() or int(version) != env.LAYOUT_VERSION

    def _save_layout(self):
        version = file(env.path(self._name, 'version'), 'w')
        version.write(str(env.LAYOUT_VERSION))
        version.close()
