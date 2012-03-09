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


errnum = 0

_COMMIT = 1

_queue = None
_queue_async = None
_write_thread = None
_commit_async = {}
_put_seqno = 0

_logger = logging.getLogger('ad.index_queue')


def init(document_classes):
    """Initialize the queue.

    Function will start index writing thread.

    :param document_classes:
        `active_document.Document` classes that queue should serve
        index writes for

    """
    global _queue, _queue_async, _write_thread

    if _queue is not None:
        return

    _queue = queue.Queue(env.index_write_queue.value)
    _queue_async = _AsyncEvent()

    classes = []
    for cls in document_classes:
        _commit_async[cls.metadata.name] = _AsyncEvent()
        classes.append(cls)

    _write_thread = _WriteThread(classes)
    _write_thread.start()

    for cls in classes:
        populating = False
        for __ in cls.populate():
            if not populating:
                _logger.info(_('Start populating "%s" index'),
                        cls.metadata.name)
                populating = True


def put(document, op, *args):
    """Put new index change operation to the queue.

    Function migh be stuck in green wait if queue is full.

    :param document:
        document index name
    :param op:
        arbitrary function
    :param args:
        optional arguments to pass to `op`

    """
    _put(document, op, *args)


def wait_commit(document):
    """Wait for changes in the specified document index.

    Function will be stuck in green wait until specified document index
    won't be flushed to the disk.

    :param document:
        document index name

    """
    enforce(document in _commit_async,
            _('Document "%s" is not registered in `init()` call'), document)
    _commit_async[document].wait()


def commit(document):
    """Flush all pending changes.

    :param document:
        document index name

    """
    put(document, _COMMIT)


def commit_and_wait(document):
    """Flush all pending changes.

    The function is different to `commit()` because it waits for
    commit finishing.

    :param document:
        document index name

    """
    seqno = _put(document, _COMMIT)
    while _commit_async[document].wait() != seqno:
        pass


def close():
    """Flush all pending changes."""
    global _queue
    if _queue is None:
        return
    put(None, None)
    _write_thread.join()
    _queue_async.close()
    while _commit_async:
        __, async = _commit_async.popitem()
        async.close()
    _queue = None


class _IndexWriter(IndexWriter):

    def commit(self):
        self.commit_with_send(None)

    def commit_with_send(self, seqno):
        IndexWriter.commit(self)
        _commit_async[self.metadata.name].send(seqno)


class _WriteThread(threading.Thread):

    class _Closing(Exception):
        pass

    def __init__(self, document_classes):
        threading.Thread.__init__(self)
        self.daemon = True
        self._document_classes = document_classes
        self._writers = {}

    def run(self):
        _logger.debug('Start processing queue')
        try:
            self._run()
        except _WriteThread._Closing:
            self._close()
        except Exception:
            global errnum
            errnum += 1
            util.exception(
                    _('Write queue died, will abort the whole application'))
            thread.interrupt_main()
        finally:
            _logger.debug('Stop processing queue')

    def _run(self):
        for cls in self._document_classes:
            _logger.info(_('Open "%s" index'), cls.metadata.name)
            self._writers[cls.metadata.name] = _IndexWriter(cls.metadata)

        next_commit = 0
        while True:
            if env.index_flush_timeout.value and not next_commit:
                next_commit = time.time() + env.index_flush_timeout.value
            try:
                timeout = None
                if next_commit:
                    timeout = max(1, next_commit - time.time())
                self._serve_put(*_queue.get(timeout=timeout))
            except queue.Empty:
                for writer in self._writers.values():
                    writer.commit()
                next_commit = 0

    def _serve_put(self, seqno, document, op, args):
        if document is None:
            raise _WriteThread._Closing

        _logger.debug('Start processing %r(%r) operation for "%s" index',
                op, args, document)

        # Wakeup greenlets stuck in `put()`
        _queue_async.send()

        writer = self._writers[document]
        try:
            if op is _COMMIT:
                writer.commit_with_send(seqno)
            else:
                op(writer, *args)
        except Exception:
            global errnum
            errnum += 1
            util.exception(_logger,
                    _('Cannot process %r(%r) operation for "%s" index'),
                    op, args, document)

    def _close(self):
        while self._writers:
            name, writer = self._writers.popitem()
            _logger.info(_('Closing "%s" index'), name)
            try:
                writer.close()
            except Exception:
                global errnum
                errnum += 1
                util.exception(_logger, _('Fail to close "%s" index'), name)


class _AsyncEvent(object):

    def __init__(self):
        self._async = gevent.get_hub().loop.async()
        self._event = Event()
        self._wakeup_job = gevent.spawn(self._wakeup)
        self._arg = None

    def close(self):
        self._wakeup_job.kill()

    def send(self, arg=None):
        self._arg = arg
        self._async.send()

    def wait(self):
        self._event.wait()
        return self._arg

    def _wakeup(self):
        while True:
            gevent.get_hub().wait(self._async)
            self._event.set()
            self._event.clear()


def _put(document, op, *args):
    while True:
        global _put_seqno
        try:
            _queue.put((_put_seqno + 1, document, op, args), False)
            _put_seqno += 1
            return _put_seqno
        except queue.Full:
            _logger.debug('Postpone %r operation for "%s" index',
                    op, document)
            # This is potential race (we released `_queue`'s mitex),
            # but we need to avoid locking greenlets in `_queue`'s mutex.
            # The race might be avoided by using big enough `_queue`'s size
            _queue_async.wait()
