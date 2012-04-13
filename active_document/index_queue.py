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

from active_document import env, util
from active_document.index import IndexWriter


errnum = 0

_queue = None
_write_thread = None

_logger = logging.getLogger('ad.index_queue')


def init(document_classes):
    """Initialize the queue.

    Function will start index writing thread.

    :param document_classes:
        `active_document.Document` classes that queue should serve
        index writes for

    """
    global _queue, _write_thread

    if _queue is not None:
        return

    _queue = _Queue()
    _write_thread = _WriteThread(document_classes)
    _write_thread.start()


def put(document, op, *args):
    """Put new index change operation to the queue.

    Function migh be stuck in green wait if queue is full.

    :param document:
        document index name
    :param op:
        arbitrary function
    :param args:
        optional arguments to pass to `op`
    :returns:
        commit seqno that will be used for the next `put()` call

    """
    return _queue.push(False, document, op, *args)


def commit(document):
    """Flush all pending changes.

    :param document:
        document index name

    """
    _queue.push(True, document)


def commit_and_wait(document):
    """Flush all pending changes.

    The function is different to `commit()` because it waits for
    commit finishing.

    :param document:
        document index name

    """
    _queue.push(True, document)
    _queue.wait()


def commit_seqno(document):
    """Last commit seqno."""
    return _queue.commit_seqno(document)


def close():
    """Flush all pending changes."""
    global _queue
    if _queue is None:
        return
    put(None, None)
    _write_thread.join()
    _queue = None


class _WriteThread(threading.Thread):

    def __init__(self, document_classes):
        threading.Thread.__init__(self)
        self.daemon = True
        self._document_classes = document_classes
        self._writers = {}

    def run(self):
        _logger.debug('Start processing queue')
        try:
            self._run()
        except _EmptyQueue:
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
            self._writers[cls.metadata.name] = IndexWriter(cls.metadata)

        closing = False

        while True:
            document, op, args, to_commit = _queue.pop_start(not closing)
            if document is None:
                _queue.pop_done(document, to_commit)
                closing = True
                continue

            writer = self._writers[document]

            if op is not None:
                _logger.debug('Start processing %r(%r) for "%s" index',
                        op, args, document)
                try:
                    op(writer, *args)
                except Exception:
                    global errnum
                    errnum += 1
                    util.exception(_logger,
                            _('Cannot process %r(%r) for "%s" index'),
                            op, args, document)
            if to_commit:
                writer.commit()

            _queue.pop_done(document, to_commit)

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


class _Queue(object):

    class _Seqno(object):

        def __init__(self):
            self.pending_seqno = 1
            self.commit_seqno = 0
            self.changes = 0
            self.endtime = time.time() + env.index_flush_timeout.value

    def __init__(self):
        self._queue = collections.deque()
        self._mutex = threading.Lock()
        self._push_cond = threading.Condition(self._mutex)
        self._done_cond = threading.Condition(self._mutex)
        self._done_async = gevent.get_hub().loop.async()
        self._endtime = time.time() + env.index_flush_timeout.value
        self._seqno = {}

    def push(self, to_commit, document, op=None, *args):
        self._mutex.acquire()
        try:
            while len(self._queue) >= env.index_write_queue.value:
                self._mutex.release()
                try:
                    # This is potential race but we need it to avoid using
                    # gevent.monkey patching (less not obvious code,
                    # less not obvious behaviour). The race might be avoided
                    # by using big enough `env.index_write_queue.value`
                    _logger.debug('Postpone %r for "%s" index', op, document)
                    gevent.get_hub().wait(self._done_async)
                finally:
                    self._mutex.acquire()
            return self._push(to_commit, document, op, args)
        finally:
            self._mutex.release()

    def pop_start(self, blocking=True):
        self._mutex.acquire()
        try:
            while True:
                remaining = None
                if env.index_flush_timeout.value:
                    ts = time.time()
                    remaining = self._endtime - ts
                    if remaining <= 0.0:
                        for document, seqno in self._seqno.items():
                            if seqno.endtime <= ts:
                                self._push(True, document, None, None)
                        remaining = env.index_flush_timeout.value
                        self._endtime = ts + remaining
                if not self._queue:
                    if not blocking:
                        raise _EmptyQueue()
                    self._push_cond.wait(remaining)
                if self._queue:
                    return self._queue[0]
        finally:
            self._mutex.release()

    def pop_done(self, document, to_commit):
        self._mutex.acquire()
        try:
            self._queue.popleft()
            if to_commit:
                self._seqno[document].commit_seqno += 1
            self._done_cond.notify()
        finally:
            self._mutex.release()
        self._done_async.send()

    def wait(self):
        self._mutex.acquire()
        try:
            while self._queue:
                self._done_cond.wait()
        finally:
            self._mutex.release()

    def commit_seqno(self, document):
        self._mutex.acquire()
        try:
            seqno = self._seqno.get(document)
            return 0 if seqno is None else seqno.commit_seqno
        finally:
            self._mutex.release()

    def _push(self, to_commit, document, op, args):
        seqno = self._seqno.get(document)
        if seqno is None:
            seqno = self._seqno[document] = _Queue._Seqno()
        if op is not None:
            seqno.changes += 1

        if env.index_flush_threshold.value:
            if seqno.changes >= env.index_flush_threshold.value:
                to_commit = True
        if env.index_flush_timeout.value:
            ts = time.time()
            if seqno.endtime <= ts:
                to_commit = True
                seqno.endtime = ts + env.index_flush_timeout.value

        if to_commit:
            if seqno.changes:
                seqno.pending_seqno += 1
                seqno.changes = 0
            else:
                to_commit = False

        self._queue.append((document, op, args, to_commit))
        self._push_cond.notify()

        return seqno.pending_seqno


class _EmptyQueue(Exception):
    pass
