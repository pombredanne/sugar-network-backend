#!/usr/bin/env python
# sugar-lint: disable

import time
import threading

from __init__ import tests

from active_document import env
from active_document.index_queue import IndexQueue, NoPut


class IndexQueueTest(tests.Test):

    def test_put(self):
        queue = IndexQueue()

        queue.put(1, '1')
        self.assertEqual(
                [(None, 1, ('1',))],
                [i for i in queue._queue])

        queue.put(2, '2')
        self.assertEqual(
                [(None, 1, ('1',)), (None, 2, ('2',))],
                [i for i in queue._queue])

        queue.put(3, '3')
        self.assertEqual(
                [(None, 1, ('1',)), (None, 2, ('2',)), (None, 3, ('3',))],
                [i for i in queue._queue])

    def test_iteration(self):
        queue = IndexQueue()

        queue.put(lambda x: x)
        queue.put(lambda x: x - 1)
        queue.put(lambda x: x - 2)

        queue.iteration(-1)
        self.assertEqual(-1, queue._got)

        queue.iteration(-2)
        self.assertEqual(-3, queue._got)

        queue.iteration(-3)
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
        try:
            [i for i in queue.put_wait(lambda x: [x])]
            assert False
        except NoPut, error:
            self.assertEqual(0, error.last_flush)

        queue.put(lambda x: x)
        queue.iteration(-1)

        def iteration():
            queue.iteration(-2)
        threading.Thread(target=iteration).start()

        # There is pending flush, process put and wait for result
        self.assertEqual(
                [-2],
                [i for i in queue.put_wait(lambda x: [x])])

        def iteration():
            queue.iteration(-3)
        threading.Thread(target=iteration).start()

        # There is pending flush, process put and wait for result
        self.assertEqual(
                [-3],
                [i for i in queue.put_wait(lambda x: [x])])

        queue.flush()

        # Flush happened, return w/o putting but w/ reopen
        try:
            [i for i in queue.put_wait(lambda x: [x])]
            assert False
        except NoPut, error:
            self.assertNotEqual(0, error.last_flush)
            last_flush = error.last_flush

        # Flush happened, return w/o putting but w/o reopen
        # since passed last_flush is the same as in queue
        try:
            [i for i in queue.put_wait(lambda x: [x])]
            assert False
        except NoPut, error:
            self.assertEqual(last_flush, error.last_flush)

    def test_put_wait_Exception(self):
        queue = IndexQueue()

        # Create pending flush
        queue.put(lambda x: x)
        queue.iteration(None)

        def iteration():
            queue.iteration(None)
        threading.Thread(target=iteration).start()

        def cb(*args):
            raise NotImplementedError()
        def put_wait():
            [i for i in queue.put_wait(cb)]
        self.assertRaises(NotImplementedError, put_wait)

    def test_close(self):
        queue = IndexQueue()

        def iteration():
            queue.iteration(0)
            time.sleep(1)
        thread = threading.Thread(target=iteration)

        queue.put(lambda x: x)
        self.assertEqual(1, len(queue._queue))
        thread.start()
        queue.close()
        self.assertEqual(0, len(queue._queue))
        thread.join()

        self.assertRaises(RuntimeError, queue.put, lambda x: x)


if __name__ == '__main__':
    tests.main()
