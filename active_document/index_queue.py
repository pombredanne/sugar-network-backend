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
import collections
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

    _queue = _Queue()
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
    _queue.push(document, op, *args)


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
    seqno = _queue.push(document, _COMMIT)
    _commit_async[document].wait(seqno)


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

        while True:
            try:
                self._serve_put(*_queue.pop())
            except _Queue.Commit, pending:
                for i in pending:
                    self._serve_put(*i)
                for writer in self._writers.values():
                    writer.commit()

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
        self._sent_seqno = None

    def close(self):
        self._wakeup_job.kill()

    def send(self, arg=None):
        self._sent_seqno = max(self._sent_seqno, arg)
        self._async.send()

    def wait(self, seqno=None):
        if not seqno:
            self._event.wait()
        else:
            while seqno > self._sent_seqno:
                self._event.wait()

    def _wakeup(self):
        while True:
            gevent.get_hub().wait(self._async)
            self._event.set()
            self._event.clear()


class _Queue(object):

    class Commit(Exception):

        def __init__(self, queue):
            self._queue = queue
            Exception.__init__(self)

        def __iter__(self):
            while self._queue:
                yield self._queue.popleft()

    def __init__(self):
        self._maxsize = env.index_write_queue.value
        self._queue = collections.deque([], self._maxsize)
        self._mutex = threading.Lock()
        self._push_cond = threading.Condition(self._mutex)
        self._seqno = 0
        self._endtime = None
        self._pushes = 0

    def push(self, document, op, *args):
        self._mutex.acquire()
        try:
            while len(self._queue) >= self._maxsize:
                self._mutex.release()
                try:
                    # This is potential race but we need it to avoid using
                    # gevent.monkey patching (less not obvious code,
                    # less not obvious behaviour). The race might be avoided
                    # by using big enough `env.index_write_queue.value`
                    _logger.debug('Postpone %r for "%s" index', op, document)
                    _queue_async.wait()
                finally:
                    self._mutex.acquire()
            self._queue.append((self._seqno + 1, document, op, args))
            self._pushes += 1
            self._seqno += 1
            self._push_cond.notify()
            return self._seqno
        finally:
            self._mutex.release()

    def pop(self):

        def flush():
            self._pushes = 0
            if self._queue:
                pending = self._queue
                self._queue = collections.deque([], self._maxsize)
            else:
                pending = []
            raise _Queue.Commit(pending)

        self._mutex.acquire()
        try:
            if env.index_flush_threshold.value:
                if self._pushes >= env.index_flush_threshold.value:
                    flush()
            if env.index_flush_timeout.value:
                if not self._endtime:
                    self._endtime = time.time() + env.index_flush_timeout.value
                while True:
                    remaining = self._endtime - time.time()
                    if remaining <= 0.0:
                        self._endtime = None
                        if self._pushes:
                            flush()
                        else:
                            break
                    if self._queue:
                        break
                    self._push_cond.wait(remaining)
            while not self._queue:
                self._push_cond.wait()
            return self._queue.popleft()
        finally:
            self._mutex.release()
