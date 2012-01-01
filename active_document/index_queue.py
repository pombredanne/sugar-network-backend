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

import time
import logging
import threading
import collections
from gettext import gettext as _

from active_document import env, util
from active_document.util import enforce


class NoPut(Exception):
    """If `IndexQueue.put_wait` didn't put an operation."""

    #: A seqno of the last `IndexQueue.flush` call
    last_flush = 0

    def __init__(self, last_flush):
        Exception.__init__(self)
        self.last_flush = last_flush


class IndexQueue(object):
    """Index requests queue to keep writer only in one thread."""

    def __init__(self):
        self._maxlen = env.index_write_queue.value
        self._queue = collections.deque([], self._maxlen or None)

        self._lock = threading.Lock()
        self._iteration_cond = threading.Condition(self._lock)
        self._put_cond = threading.Condition(self._lock)
        self._op_cond = threading.Condition(self._lock)

        self._last_flush = 0
        self._pending_puts = 0
        self._pending_flush = False
        self._shutting_down = False
        self._got_tid = None
        self._got = None

    def put(self, op, *args):
        """Put new operation to the queue.

        :param op:
            arbitrary function
        :param args:
            optional arguments to pass to `op`

        """
        self._lock.acquire()
        try:
            self._pending_puts += 1
            self._pending_flush = True
            self._put(None, op, *args)
        finally:
            self._lock.release()

    def put_wait(self, op, *args):
        """Try to put new operation to the queue with waiting for result.

        Put operation to the queue only if there were succesful `iteration`
        calls without `flush`. If operation was placed to the queue,
        the function will wait until `iteration` will proces it and return
        the result.

        :param op:
            arbitrary function
        :param args:
            optional arguments to pass to `op`
        :returns:
            the result of `op` if put happened;
            otherwise raise an `NoPut`

        """
        self._lock.acquire()
        try:
            if self._pending_flush:
                logging.debug('Wait for %r from queue', op)
                tid = threading.current_thread().ident
                self._put(tid, op, *args)
                while self._got_tid != tid:
                    self._op_cond.wait()
                self._got_tid = None
                if isinstance(self._got, Exception):
                    # pylint: disable-msg=E0702
                    raise self._got
                else:
                    return self._got
            else:
                raise NoPut(self._last_flush)
        finally:
            self._lock.release()

    def iteration(self, obj):
        """Process the queue.

        If queue is empty, the function will wait for new `put` calls.

        :param obj:
            an object that will be used to call put operations

        """
        self._lock.acquire()
        try:
            while not len(self._queue):
                self._put_cond.wait()
            self._got_tid, op, args = self._queue.popleft()
            if self._got_tid is None:
                self._pending_puts -= 1
            self._iteration_cond.notify()
        finally:
            self._lock.release()

        try:
            got = op(obj, *args)
        except Exception, error:
            util.exception(_('Cannot process %r operation for %r'), op, obj)
            got = error

        self._lock.acquire()
        try:
            self._got = got
            self._op_cond.notify_all()
        finally:
            self._lock.release()

    def flush(self):
        """Flush the processed queue items.

        If there were succesful `iteration` calls, this function will
        "flush" them. This function makes sense only for `put_wait` calls.

        """
        last_flush = time.time()
        self._lock.acquire()
        try:
            if self._pending_puts == 0:
                self._pending_flush = False
            self._last_flush = last_flush
        finally:
            self._lock.release()

    def close(self):
        """Close the queue.

        The function will stop accepting new `put` calls and will wait until
        current queue will be processed.

        """
        self._lock.acquire()
        try:
            self._shutting_down = True
            while len(self._queue):
                self._iteration_cond.wait()
        finally:
            self._lock.release()

    def _put(self, tid, op, *args):
        enforce(not self._shutting_down, _('Index is being closed'))
        while self._maxlen and len(self._queue) >= self._maxlen:
            self._iteration_cond.wait()
        self._queue.append((tid, op, args))
        self._put_cond.notify()
