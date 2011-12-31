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
import threading
import collections
from gettext import gettext as _

from active_document import env, util
from active_document.util import enforce


class IndexQueue(object):
    """Index requests queue to keep writer only in one thread."""

    def __init__(self):
        self._maxlen = env.write_queue.value
        self._queue = collections.deque([], self._maxlen or None)

        self._lock = threading.Lock()
        self._iteration_cond = threading.Condition(self._lock)
        self._put_cond = threading.Condition(self._lock)
        self._op_cond = threading.Condition(self._lock)

        self._last_flush = 0
        self._pending_flush = False
        self._shutting_down = False
        self._seqno = 0
        self._got_seqno = None
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
            self._put(0, op, *args)
        finally:
            self._lock.release()

    def put_wait(self, last_flush, op, *args):
        """Try to put new operation to the queue with waiting for result.

        Put operation to the queue only if there were succesful `iteration`
        calls without `flush`. If operation was placed to the queue,
        the function will wait until `iteration` will proces it and return
        the result.

        :param last_flush:
            seqno of the last `flush` execution caller knows about
        :param op:
            arbitrary function
        :param args:
            optional arguments to pass to `op`
        :returns:
            (op_result, `None`, `None`) if put happened;
            (`None`, new_last_flush, reopen) if put didn't happen

        """
        reopen = False
        self._lock.acquire()
        try:
            if self._pending_flush:
                self._seqno += 1
                seqno = self._seqno
                self._put(seqno, op, *args)
                while seqno != self._got_seqno:
                    self._op_cond.wait()
                return self._got, None, None
            elif self._last_flush > last_flush:
                reopen = True
            last_flush = self._last_flush
        finally:
            self._lock.release()

        return None, last_flush, reopen

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
            self._got_seqno, op, args = self._queue.popleft()
            if not self._got_seqno:
                self._pending_flush = True
            self._iteration_cond.notify()
        finally:
            self._lock.release()

        try:
            got = op(obj, *args)
        except Exception:
            util.exception(_('Cannot process "%s" operation for %s'),
                    op.__func__.__name__, obj.name)
            got = None

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
            self._pending_flush = False
            self._last_flush = last_flush
        finally:
            self._lock.release()

    def shutdown(self):
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

    def _put(self, seqno, op, *args):
        enforce(not self._shutting_down, _('Index is being closed'))
        while self._maxlen and len(self._queue) >= self._maxlen:
            self._iteration_cond.wait()
        self._queue.append((seqno, op, args))
        self._put_cond.notify()
