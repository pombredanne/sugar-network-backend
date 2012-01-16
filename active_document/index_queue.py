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


_COMMIT = 1

_queue = None
_queue_async = None
_write_thread = None
_flush_async = {}
_put_seqno = 0
_commit_seqno = {}

_logger = logging.getLogger('ad.index_queue')


def init(document_classes):
    """Initialize the queue.

    Function will start index writing thread.

    :param document_classes:
        `active_document.Document` classes that queue should serve
        index writes for

    """
    global _queue, _queue_async, _write_thread

    _queue = queue.Queue(env.index_write_queue.value)
    _queue_async = _AsyncEvent()

    for cls in document_classes:
        _flush_async[cls.metadata.name] = _AsyncEvent()
        _commit_seqno[cls.metadata.name] = 0

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
    :returns:
        put seqno

    """
    while True:
        global _put_seqno
        try:
            _queue.put((_put_seqno + 1, document_name, op, args), False)
            _put_seqno += 1
            return _put_seqno
        except queue.Full:
            _logger.debug('Postpone %r operation for "%s" index',
                    op, document_name)
            # This is potential race (we released `_queue`'s mitex),
            # but we need to avoid locking greenlets in `_queue`'s mutex.
            # The race might be avoided by using big enough `_queue`'s size
            _queue_async.wait()


def wait_commit(document_name):
    """Wait for changes in the specified document index.

    Function will be stuck in green wait until specified document index
    won't be flushed to the disk.

    :param document_name:
        document index name
    :returns:
        seqno for the last put that was flushed

    """
    enforce(document_name in _flush_async,
            _('Document "%s" is not registered in `init()` call'),
            document_name)
    _flush_async[document_name].wait()
    return _commit_seqno[document_name]


def commit(document):
    """Flush all pending changes."""
    put(document, _COMMIT)


def close():
    """Flush all pending changes."""
    put(None, None)
    _write_thread.join()
    _queue_async.close()
    while _flush_async:
        __, async = _flush_async.popitem()
        async.close()
    _commit_seqno.clear()


class _IndexWriter(IndexWriter):

    put_seqno = 0

    def commit(self):
        IndexWriter.commit(self)
        _commit_seqno[self.metadata.name] = self.put_seqno
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
            _logger.info(_('Open "%s" index'), cls.metadata.name)
            self._writers[cls.metadata.name] = _IndexWriter(cls.metadata)

        for cls in self._document_classes:
            populating = False
            for __ in cls.populate():
                if not populating:
                    _logger.info(_('Start populating "%s" index'),
                            cls.metadata.name)
                    populating = True
                try:
                    # Try to server requests in parallel with populating
                    self._serve_put(*_queue.get(False))
                except queue.Empty:
                    pass

        _logger.debug('Start processing "%s" queue', cls.metadata.name)

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

    def _serve_put(self, put_seqno, document_name, op, args):
        if document_name is None:
            raise _WriteThread._Closing

        # Wakeup greenlets stuck in `put()`
        _queue_async.send()

        writer = self._writers[document_name]
        writer.put_seqno = put_seqno

        try:
            if op is _COMMIT:
                writer.commit()
            else:
                op(writer, *args)
        except Exception:
            util.exception(_logger,
                    _('Cannot process %r operation for "%s" index'),
                    op, document_name)

    def _close(self):
        while self._writers:
            name, writer = self._writers.popitem()
            _logger.info(_('Closing "%s" index'), name)
            try:
                writer.close()
            except Exception:
                util.exception(_logger, _('Fail to close "%s" index'), name)


class _AsyncEvent(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()
        self._event = Event()
        self._wakeup_job = gevent.spawn(self._wakeup)

    def close(self):
        self._wakeup_job.kill()

    def send(self):
        self._async.send()

    def wait(self):
        self._event.wait()

    def _wakeup(self):
        while True:
            gevent.get_hub().wait(self._async)
            self._event.set()
            self._event.clear()
