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

import uuid
import shutil
import logging
from os.path import exists
from gettext import gettext as _

import xapian
import gobject

from active_document import util, env
from active_document.util import enforce


def get_writer(name, properties, crawler):
    """Open a database for writing.

    Function might be called several times for the same database,
    the real opening will happen only once.

    """
    return _Writer(name, properties, crawler)


class _BaseWriter(gobject.GObject):

    __gsignals__ = {
            'changed': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, []),
            }

    def __init__(self, name, properties, crawler):
        gobject.GObject.__init__(self)

        self._name = name
        self._properties = properties
        self._crawler = crawler
        self._db = None
        self._flush_timeout_hid = None
        self._pending_writes = 0
        self._index_path = env.path(self._name, 'index', '')

        self._open(False)

    def close(self):
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
        raise NotImplementedError()

    def _open(self, reset):
        if not reset and self._is_layout_stale():
            reset = True
        if reset:
            shutil.rmtree(self._index_path, ignore_errors=True)

        try:
            self._db = xapian.WritableDatabase(
                    self._index_path, xapian.DB_CREATE_OR_OPEN)
        except xapian.DatabaseError:
            if reset:
                util.exception(_('Unrecoverable error while opening %s ' \
                        'Xapian index'), self._index_path)
                raise
            else:
                util.exception(_('Cannot open Xapian index in %s, ' \
                        'will rebuild it'), self._index_path)
                self._open(True)
                return

        if reset:
            self._save_layout()
            gobject.idle_add(self._populate, self._crawler())

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
            self.emit('changed')
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


class _Writer(_BaseWriter):

    def get_reader(self):
        return self._db


class _ThreadWriter(_BaseWriter):

    def get_reader(self):
        return xapian.Database(self._index_path)
