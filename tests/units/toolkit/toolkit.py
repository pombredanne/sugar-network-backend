#!/usr/bin/env python
# sugar-lint: disable

import copy
from os.path import exists
from cStringIO import StringIO

from __init__ import tests

from sugar_network import toolkit
from sugar_network.toolkit import Seqno


class ToolkitTest(tests.Test):

    def test_Seqno_commit(self):
        seqno = Seqno(tests.tmpdir + '/seqno')

        seqno.next()
        seqno.commit()
        seqno.next()

        seqno = Seqno(tests.tmpdir + '/seqno')
        self.assertEqual(1, seqno.value)

    def test_readline(self):

        def readlines(string):
            result = []
            stream = StringIO(string)
            while True:
                line = toolkit.readline(stream)
                if not line:
                    break
                result.append(line)
            return result

        self.assertEqual([], readlines(''))
        self.assertEqual([' '], readlines(' '))
        self.assertEqual([' a '], readlines(' a '))
        self.assertEqual(['\n'], readlines('\n'))
        self.assertEqual(['\n', 'b'], readlines('\nb'))
        self.assertEqual([' \n', ' b \n'], readlines(' \n b \n'))

    def test_Pool(self):
        stack = toolkit.Pool()

        stack.add('a')
        stack.add('b')
        stack.add('c')

        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('a'))
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('b'))
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                [('c', toolkit.Pool.ACTIVE), ('b', toolkit.Pool.ACTIVE), ('a', toolkit.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(
                [],
                [i for i in stack])
        self.assertEqual(toolkit.Pool.PASSED, stack.get_state('a'))
        self.assertEqual(toolkit.Pool.PASSED, stack.get_state('b'))
        self.assertEqual(toolkit.Pool.PASSED, stack.get_state('c'))

        stack.rewind()
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('a'))
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('b'))
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                ['c', 'b', 'a'],
                [i for i in stack])

        stack.add('c')
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                [('c', toolkit.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(toolkit.Pool.PASSED, stack.get_state('c'))

        stack.add('b')
        stack.add('a')
        self.assertEqual(
                ['a', 'b'],
                [i for i in stack])

        stack.rewind()
        self.assertEqual(
                ['a', 'b', 'c'],
                [i for i in stack])

        stack.add('d')
        self.assertEqual(toolkit.Pool.QUEUED, stack.get_state('d'))
        self.assertEqual(
                [('d', toolkit.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(toolkit.Pool.PASSED, stack.get_state('d'))

        stack.rewind()
        self.assertEqual(
                ['d', 'a', 'b', 'c'],
                [i for i in stack])


if __name__ == '__main__':
    tests.main()
