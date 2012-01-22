#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from active_document import sync



class SyncTest(tests.Test):

    def test_Timeline_flush(self):
        tl = sync._Timeline('tl')
        self.assertEqual(
                [[1, None]],
                tl)
        tl.append([2, 3])
        tl.flush()

        tl = sync._Timeline('tl')
        self.assertEqual(
                [[1, None], [2, 3]],
                tl)

    def test_Timeline_exclude(self):
        tl = sync._Timeline('1')
        tl.exclude(1, 10)
        self.assertEqual(
                [[11, None]],
                tl)

        tl = sync._Timeline('2')
        tl.exclude(5, 10)
        self.assertEqual(
                [[1, 4], [11, None]],
                tl)

        tl.exclude(2)
        self.assertEqual(
                [[1, 1], [3, 4], [11, None]],
                tl)

        tl.exclude(1)
        self.assertEqual(
                [[3, 4], [11, None]],
                tl)

        tl.exclude(3)
        self.assertEqual(
                [[4, 4], [11, None]],
                tl)

        tl.exclude(1, 20)
        self.assertEqual(
                [[21, None]],
                tl)


if __name__ == '__main__':
    tests.main()
