#!/usr/bin/env python
# sugar-lint: disable

import time
import threading

from __init__ import tests

from active_document import env
from active_document.index_queue import IndexQueue


class IndexQueueTest(tests.Test):

    def test_put(self):
        queue = IndexQueue()

        queue.put(1, '1')
        self.assertEqual(
                [(0, 1, ('1',))],
                [i for i in queue._queue])
        self.assertEqual(0, queue._seqno)

        queue.put(2, '2')
        self.assertEqual(
                [(0, 1, ('1',)), (0, 2, ('2',))],
                [i for i in queue._queue])
        self.assertEqual(0, queue._seqno)

        queue.put(3, '3')
        self.assertEqual(
                [(0, 1, ('1',)), (0, 2, ('2',)), (0, 3, ('3',))],
                [i for i in queue._queue])
        self.assertEqual(0, queue._seqno)

    def test_iteration(self):
        queue = IndexQueue()

        queue.put(lambda x: x)
        queue.put(lambda x: x - 1)
        queue.put(lambda x: x - 2)

        queue.iteration(-1)
        self.assertEqual(0, queue._got_seqno)
        self.assertEqual(-1, queue._got)

        queue.iteration(-2)
        self.assertEqual(0, queue._got_seqno)
        self.assertEqual(-3, queue._got)

        queue.iteration(-3)
        self.assertEqual(0, queue._got_seqno)
        self.assertEqual(-5, queue._got)

    def test_put_MaxLen(self):
        env.index_write_queue.value = 1
        queue = IndexQueue()

        queue.put(lambda x: x)

        def iteration():
            time.sleep(1.5)
            queue.iteration(0)
        threading.Thread(target=iteration).start()

        ts = time.time()
        queue.put(lambda x: x)
        assert time.time() - ts > 1
        self.assertEqual(1, len(queue._queue))

    def test_put_wait(self):
        queue = IndexQueue()

        # No pending flush, return w/o putting
        result, last_flush, reopen = queue.put_wait(0, lambda x: x)
        self.assertEqual(0, queue._seqno)
        self.assertEqual(None, result)
        self.assertEqual(0, last_flush)
        self.assertEqual(False, reopen)

        queue.put(lambda x: x)

        # No pending flush (previous out was not processed), return w/o putting
        result, last_flush, reopen = queue.put_wait(0, lambda x: x)
        self.assertEqual(0, queue._seqno)
        self.assertEqual(None, result)
        self.assertEqual(0, last_flush)
        self.assertEqual(False, reopen)

        queue.iteration(-1)

        def iteration():
            queue.iteration(-2)
        threading.Thread(target=iteration).start()

        # There is pending flush, process put and wait for result
        result, last_flush, reopen = queue.put_wait(0, lambda x: x)
        self.assertEqual(1, queue._seqno)
        self.assertEqual(-2, result)
        self.assertEqual(None, last_flush)
        self.assertEqual(None, reopen)

        def iteration():
            queue.iteration(-3)
        threading.Thread(target=iteration).start()

        # There is pending flush, process put and wait for result
        result, last_flush, reopen = queue.put_wait(0, lambda x: x)
        self.assertEqual(2, queue._seqno)
        self.assertEqual(-3, result)
        self.assertEqual(None, last_flush)
        self.assertEqual(None, reopen)

        queue.flush()

        # Flush happened, return w/o putting but w/ reopen
        result, last_flush, reopen = queue.put_wait(0, lambda x: x)
        self.assertEqual(2, queue._seqno)
        self.assertEqual(None, result)
        self.assertNotEqual(0, last_flush)
        self.assertEqual(True, reopen)

        # Flush happened, return w/o putting but w/o reopen
        # since passed last_flush is the same as in queue
        result, new_last_flush, reopen = queue.put_wait(last_flush, lambda x: x)
        self.assertEqual(2, queue._seqno)
        self.assertEqual(None, result)
        self.assertEqual(last_flush, new_last_flush)
        self.assertEqual(False, reopen)

    def test_shutdown(self):
        queue = IndexQueue()

        def iteration():
            queue.iteration(0)
            time.sleep(1)
        thread = threading.Thread(target=iteration)

        queue.put(lambda x: x)
        self.assertEqual(1, len(queue._queue))
        thread.start()
        queue.shutdown()
        self.assertEqual(0, len(queue._queue))
        thread.join()

        self.assertRaises(RuntimeError, queue.put, lambda x: x)


if __name__ == '__main__':
    tests.main()
