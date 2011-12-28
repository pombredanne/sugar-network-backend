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

import thread
import logging
from Queue import Queue
from gettext import gettext as _

import xapian

from active_document import util, env


class DatabaseProxy(object):

    def __init__(self, writer):
        self._writer = writer
        self._queue = Queue(env.write_queue.value)

    @property
    def name(self):
        """Xapian database name."""
        return self._writer.name

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
        logging.debug('Push update request to %s\'s queue for %s',
                self.name, guid)
        self._queue.put([self._writer.__class__.update, guid, props], True)

    def delete(self, guid):
        """Delete document.

        :props guid:
            document GUID to delete

        """
        logging.debug('Push delete request to %s\'s queue for %s',
                self.name, guid)
        self._queue.put([self._writer.__class__.delete, guid], True)

    def get_reader(self):
        try:
            return xapian.Database(env.index_path(self.name))
        except xapian.DatabaseOpeningError:
            logging.debug('Cannot open read-only database for %s', self.name)
            return None
        else:
            logging.debug('Open read-only database for %s', self.name)

    def connect(self, *args, **kwargs):
        self._writer.connect(*args, **kwargs)

    def serve_forever(self):
        try:
            self._writer.open()

            logging.debug('Start serving writes to %s', self.name)

            while True:
                args = self._queue.get(True)
                op = args.pop(0)
                try:
                    op(self._writer, *args)
                except Exception:
                    util.exception(_('Cannot process "%s" operation for %s'),
                            op.__func__.__name__, self.name)
                finally:
                    self._queue.task_done()

        except Exception:
            util.exception(_('Database %s write thread died, ' \
                    'will abort the whole application'), self.name)
            thread.interrupt_main()

    def shutdown(self):
        """Flush all write pending queues and close all databases."""
        self._queue.join()
        self._writer.shutdown()
