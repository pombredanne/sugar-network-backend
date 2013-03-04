#!/usr/bin/env python
# sugar-lint: disable

import copy
from os.path import exists
from cStringIO import StringIO

from __init__ import tests

from sugar_network.toolkit import util
from sugar_network.toolkit.util import Seqno, Sequence


class UtilTest(tests.Test):

    def test_Seqno_commit(self):
        seqno = Seqno('seqno')

        self.assertEqual(False, seqno.commit())
        assert not exists('seqno')

        seqno.next()
        self.assertEqual(True, seqno.commit())
        assert exists('seqno')
        self.assertEqual(False, seqno.commit())
        seqno.next()

        seqno = Seqno('seqno')
        self.assertEqual(1, seqno.value)
        self.assertEqual(False, seqno.commit())

    def test_Sequence_empty(self):
        scale = Sequence(empty_value=[1, None])
        self.assertEqual(
                [[1, None]],
                scale)
        assert scale.empty
        scale.exclude(1, 1)
        assert not scale.empty

        scale = Sequence()
        self.assertEqual(
                [],
                scale)
        assert scale.empty
        scale.include(1, None)
        assert not scale.empty

    def test_Sequence_exclude(self):
        scale = Sequence(empty_value=[1, None])
        scale.exclude(1, 10)
        self.assertEqual(
                [[11, None]],
                scale)
        scale = Sequence(empty_value=[1, None])
        scale.exclude(5, 10)
        self.assertEqual(
                [[1, 4], [11, None]],
                scale)
        scale.exclude(2, 2)
        self.assertEqual(
                [[1, 1], [3, 4], [11, None]],
                scale)
        scale.exclude(1, 1)
        self.assertEqual(
                [[3, 4], [11, None]],
                scale)
        scale.exclude(3, 3)
        self.assertEqual(
                [[4, 4], [11, None]],
                scale)
        scale.exclude(1, 20)
        self.assertEqual(
                [[21, None]],
                scale)
        scale.exclude(21, 21)
        self.assertEqual(
                [[22, None]],
                scale)

        seq = Sequence([[100, None]])
        seq.exclude([[1, 98]])
        self.assertEqual([[100, None]], seq)

        seq = Sequence([[1, 100]])
        seq.exclude([[200, 300]])
        self.assertEqual([[1, 100]], seq)

    def test_Sequence_include_JoinExistingItems(self):
        scale = Sequence()

        scale.include(1, None)
        self.assertEqual(
                [[1, None]],
                scale)

        scale.include(2, None)
        self.assertEqual(
                [[1, None]],
                scale)

        scale.include(4, 5)
        self.assertEqual(
                [[1, None]],
                scale)

        scale.exclude(2, 2)
        scale.exclude(4, 4)
        scale.exclude(6, 6)
        scale.exclude(9, 9)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, 8],
                    [10, None]],
                scale)

        scale.include(10, 20)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, 8],
                    [10, None]],
                scale)

        scale.include(8, 20)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, None]],
                scale)

        scale.include(5, None)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, None]],
                scale)

        scale.include(1, None)
        self.assertEqual(
                [[1, None]],
                scale)

    def test_Sequence_include_InsertNewItems(self):
        scale = Sequence()

        scale.include(8, 10)
        scale.include(3, 3)
        self.assertEqual(
                [[3, 3],
                    [8, 10]],
                scale)

        scale.include(9, 11)
        self.assertEqual(
                [[3, 3],
                    [8, 11]],
                scale)

        scale.include(7, 12)
        self.assertEqual(
                [[3, 3],
                    [7, 12]],
                scale)

        scale.include(5, 5)
        self.assertEqual(
                [[3, 3],
                    [5, 5],
                    [7, 12]],
                scale)

        scale.include(4, 4)
        self.assertEqual(
                [[3, 5],
                    [7, 12]],
                scale)

        scale.include(1, 1)
        self.assertEqual(
                [[1, 1],
                    [3, 5],
                    [7, 12]],
                scale)

        scale.include(2, None)
        self.assertEqual(
                [[1, None]],
                scale)

    def teste_Sequence_Invert(self):
        scale_1 = Sequence(empty_value=[1, None])
        scale_1.exclude(2, 2)
        scale_1.exclude(5, 10)

        scale_2 = copy.deepcopy(scale_1[:])
        scale_2[-1][1] = 20

        self.assertEqual(
                [
                    [1, 1],
                    [3, 4],
                    [11, None],
                    ],
                scale_1)
        scale_1.exclude(scale_2)
        self.assertEqual(
                [[21, None]],
                scale_1)

    def test_Sequence_contains(self):
        scale = Sequence(empty_value=[1, None])

        assert 1 in scale
        assert 4 in scale

        scale.exclude(2, 2)
        scale.exclude(5, 10)

        assert 1 in scale
        assert 2 not in scale
        assert 3 in scale
        assert 5 not in scale
        assert 10 not in scale
        assert 11 in scale
        assert 12 in scale

    def test_Sequence_stretch(self):
        seq = Sequence()
        seq.stretch()
        self.assertEqual([], seq)

        seq = Sequence([[1, None]])
        seq.stretch()
        self.assertEqual([[1, None]], seq)

        seq = Sequence([[1, 10]])
        seq.stretch()
        self.assertEqual([[1, 10]], seq)

        seq = Sequence([[1, 1], [3, 3], [5, None]])
        seq.stretch()
        self.assertEqual([[1, None]], seq)

        seq = Sequence([[3, 3], [5, 10]])
        seq.stretch()
        self.assertEqual([[3, 10]], seq)

    def test_Sequence_include(self):
        rng = Sequence()
        rng.include(2, 2)
        self.assertEqual(
                [[2, 2]],
                rng)
        rng.include(7, 10)
        self.assertEqual(
                [[2, 2], [7, 10]],
                rng)
        rng.include(5, 5)
        self.assertEqual(
                [[2, 2], [5, 5], [7, 10]],
                rng)
        rng.include(15, None)
        self.assertEqual(
                [[2, 2], [5, 5], [7, 10], [15, None]],
                rng)
        rng.include(3, 5)
        self.assertEqual(
                [[2, 5], [7, 10], [15, None]],
                rng)
        rng.include(11, 14)
        self.assertEqual(
                [[2, 5], [7, None]],
                rng)

        rng = Sequence()
        rng.include(10, None)
        self.assertEqual(
                [[10, None]],
                rng)
        rng.include(7, 8)
        self.assertEqual(
                [[7, 8], [10, None]],
                rng)
        rng.include(2, 2)
        self.assertEqual(
                [[2, 2], [7, 8], [10, None]],
                rng)

    def test_Sequence_Union(self):
        seq_1 = Sequence()
        seq_1.include(1, 2)
        seq_2 = Sequence()
        seq_2.include(3, 4)
        seq_1.include(seq_2)
        self.assertEqual(
                [[1, 4]],
                seq_1)

        seq_1 = Sequence()
        seq_1.include(1, None)
        seq_2 = Sequence()
        seq_2.include(3, 4)
        seq_1.include(seq_2)
        self.assertEqual(
                [[1, None]],
                seq_1)

        seq_2 = Sequence()
        seq_2.include(1, None)
        seq_1 = Sequence()
        seq_1.include(3, 4)
        seq_1.include(seq_2)
        self.assertEqual(
                [[1, None]],
                seq_1)

        seq_1 = Sequence()
        seq_1.include(1, None)
        seq_2 = Sequence()
        seq_2.include(2, None)
        seq_1.include(seq_2)
        self.assertEqual(
                [[1, None]],
                seq_1)

        seq_1 = Sequence()
        seq_2 = Sequence()
        seq_2.include(seq_1)
        self.assertEqual([], seq_2)

        seq_1 = Sequence()
        seq_2 = Sequence()
        seq_2.include(1, None)
        seq_2.include(seq_1)
        self.assertEqual([[1, None]], seq_2)

        seq = Sequence()
        seq.include(10, 11)
        seq.include(None)
        self.assertEqual([[10, 11]], seq)

    def test_readline(self):

        def readlines(string):
            result = []
            stream = StringIO(string)
            while True:
                line = util.readline(stream)
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
        stack = util.Pool()

        stack.add('a')
        stack.add('b')
        stack.add('c')

        self.assertEqual(util.Pool.QUEUED, stack.get_state('a'))
        self.assertEqual(util.Pool.QUEUED, stack.get_state('b'))
        self.assertEqual(util.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                [('c', util.Pool.ACTIVE), ('b', util.Pool.ACTIVE), ('a', util.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(
                [],
                [i for i in stack])
        self.assertEqual(util.Pool.PASSED, stack.get_state('a'))
        self.assertEqual(util.Pool.PASSED, stack.get_state('b'))
        self.assertEqual(util.Pool.PASSED, stack.get_state('c'))

        stack.rewind()
        self.assertEqual(util.Pool.QUEUED, stack.get_state('a'))
        self.assertEqual(util.Pool.QUEUED, stack.get_state('b'))
        self.assertEqual(util.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                ['c', 'b', 'a'],
                [i for i in stack])

        stack.add('c')
        self.assertEqual(util.Pool.QUEUED, stack.get_state('c'))
        self.assertEqual(
                [('c', util.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(util.Pool.PASSED, stack.get_state('c'))

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
        self.assertEqual(util.Pool.QUEUED, stack.get_state('d'))
        self.assertEqual(
                [('d', util.Pool.ACTIVE)],
                [(i, stack.get_state(i)) for i in stack])
        self.assertEqual(util.Pool.PASSED, stack.get_state('d'))

        stack.rewind()
        self.assertEqual(
                ['d', 'a', 'b', 'c'],
                [i for i in stack])


if __name__ == '__main__':
    tests.main()
