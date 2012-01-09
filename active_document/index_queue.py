# Copyright (C) 2012, Aleksey Lim
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

import time
import thread
import logging
import threading
import Queue as queue
from gettext import gettext as _

import gevent
from gevent.event import Event

from active_document import env, util
from active_document.index import IndexWriter
from active_document.util import enforce


_queue = None
_queue_async = None
_queue_event = None
_write_thread = None
_flush_async = {}

_logger = logging.getLogger('ad.index_queue')


def init(document_classes):
    """Initialize the queue.

    Function will start index writing thread.

    :param document_classes:
        `active_document.Document` classes that queue should serve
        index writes for

    """
    global _queue, _queue_async, _queue_event, _write_thread

    _queue = queue.Queue(env.index_write_queue.value)
    _queue_async = gevent.get_hub().loop.async()
    _queue_event = Event()
    gevent.spawn(_wakeup_put)

    for cls in document_classes:
        _flush_async[cls.metadata.name] = gevent.get_hub().loop.async()

    _write_thread = _WriteThread(document_classes)
    _write_thread.start()


def put(document_name, op, *args):
    """Put new index change operation to the queue.

    Function migh be stuck in green wait if queue is full.

    :param document_name:
        document index name
    :param op:
        arbitrary function
    :param args:
        optional arguments to pass to `op`

    """
    while True:
        try:
            _queue.put((document_name, op, args), False)
        except queue.Full:
            _logger.debug('Postpone %r operation for %s index',
                    op, document_name)
            # This is potential race (we released `_queue`'s mitex),
            # but we need to avoid locking greenlets in `_queue`'s mutex.
            # The race might be avoided by using big enough `_queue`'s size
            _queue_event.wait()
        else:
            break


def wait(document_name):
    """Wait for changes in the specified document index.

    Function will be stuck in green wait until specified document index
    won't be flushed to the disk.

    :param document_name:
        document index name

    """
    enforce(document_name in _flush_async,
            _('Document %s is not registered in `init()` call'),
            document_name)
    gevent.get_hub().wait(_flush_async[document_name])


def close():
    """Flush all pending changes."""
    put(None, None)
    _write_thread.join()


def _wakeup_put():
    while True:
        gevent.get_hub().wait(_queue_async)
        _queue_event.set()
        _queue_event.clear()


class _IndexWriter(IndexWriter):

    def commit(self):
        IndexWriter.commit(self)
        _flush_async[self.metadata.name].send()


class _WriteThread(threading.Thread):

    class _Closing(Exception):
        pass

    def __init__(self, document_classes):
        threading.Thread.__init__(self)
        self.daemon = True
        self._document_classes = document_classes
        self._writers = {}

    def run(self):
        try:
            self._run()
        except _WriteThread._Closing:
            self._close()
        except Exception:
            util.exception(
                    _('Write queue died, will abort the whole application'))
            thread.interrupt_main()

    def _run(self):
        for cls in self._document_classes:
            _logger.info(_('Open %s index'), cls.metadata.name)
            self._writers[cls.metadata.name] = _IndexWriter(cls.metadata)

        for cls in self._document_classes:
            populating = False
            for __ in cls.populate():
                if not populating:
                    _logger.info(_('Start populating %s index'),
                            cls.metadata.name)
                    populating = True
                try:
                    # Try to server requests in parallel with populating
                    self._serve_put(*_queue.get(False))
                except queue.Empty:
                    pass

        next_commit = 0
        if env.index_flush_timeout.value:
            next_commit = time.time() + env.index_flush_timeout.value

        while True:
            try:
                timeout = None
                if next_commit:
                    timeout = max(1, next_commit - time.time())
                request = _queue.get(timeout=timeout)
                self._serve_put(*request)
            except queue.Empty:
                for writer in self._writers.values():
                    writer.commit()
                next_commit = time.time() + env.index_flush_timeout.value

    def _serve_put(self, document_name, op, args):
        if document_name is None:
            raise _WriteThread._Closing

        # Wakeup greenlets stuck in `put()`
        _queue_async.send()

        try:
            op(self._writers[document_name], *args)
        except Exception:
            util.exception(_logger,
                    _('Cannot process %r operation for %s index'),
                    op, document_name)

    def _close(self):
        while self._writers:
            name, writer = self._writers.popitem()
            _logger.info(_('Closing %s index'), name)
            try:
                writer.close()
            except Exception:
                util.exception(_logger, _('Fail to close %s index'), name)
