#!/usr/bin/env python
# sugar-lint: disable

import copy

from __init__ import tests

from sugar_network.toolkit.sequence import Sequence


class SequenceTest(tests.Test):

    def test_empty(self):
        scale = Sequence(tests.tmpdir, init_value=[1, None])
        self.assertEqual(
                [[1, None]],
                scale)
        assert scale.empty
        scale.exclude(1, 1)
        assert not scale.empty

        scale = Sequence(tests.tmpdir)
        self.assertEqual(
                [],
                scale)
        assert scale.empty
        scale.include(1, None)
        assert not scale.empty

    def test_commit(self):
        scale = Sequence(tests.tmpdir, 'scale')
        self.assertEqual(
                [],
                scale)
        scale.append([2, 3])
        scale.commit()

        scale = Sequence(tests.tmpdir, 'scale')
        self.assertEqual(
                [[2, 3]],
                scale)

    def test_exclude(self):
        scale = Sequence(tests.tmpdir, '1', [1, None])
        scale.exclude(1, 10)
        self.assertEqual(
                [[11, None]],
                scale)

        scale = Sequence(tests.tmpdir, '2', [1, None])
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

    def test_include_JoinExistingItems(self):
        scale = Sequence(tests.tmpdir)

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

    def test_include_InsertNewItems(self):
        scale = Sequence(tests.tmpdir)

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

    def teste_Invert(self):
        scale_1 = Sequence(tests.tmpdir, init_value=[1, None])
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

    def test_contains(self):
        scale = Sequence(tests.tmpdir, init_value=[1, None])
        scale.exclude(2, 2)
        scale.exclude(5, 10)

        assert 1 in scale
        assert 2 not in scale
        assert 3 in scale
        assert 5 not in scale
        assert 10 not in scale
        assert 11 in scale

    def test_first(self):
        scale = Sequence(tests.tmpdir)
        self.assertEqual(0, scale.first)

        scale = Sequence(tests.tmpdir, init_value=[1, None])
        self.assertEqual(1, scale.first)
        scale.exclude(1, 3)
        self.assertEqual(4, scale.first)

    def test_include(self):
        rng = Sequence(tests.tmpdir)
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

        rng = Sequence(tests.tmpdir)
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

    def test_floor(self):
        rng = Sequence(tests.tmpdir)
        rng.include(2, None)
        rng.floor(1)
        self.assertEqual([], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, None)
        rng.floor(2)
        self.assertEqual([[2, 2]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, None)
        rng.floor(10)
        self.assertEqual([[2, 10]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, 5)
        rng.include(10, 11)
        rng.floor(7)
        self.assertEqual([[2, 5]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, 5)
        rng.include(10, 11)
        rng.floor(5)
        self.assertEqual([[2, 5]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, 5)
        rng.include(10, 11)
        rng.floor(3)
        self.assertEqual([[2, 3]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, 5)
        rng.include(10, 11)
        rng.floor(2)
        self.assertEqual([[2, 2]], rng)

        rng = Sequence(tests.tmpdir)
        rng.include(2, 5)
        rng.include(10, 11)
        rng.floor(1)
        self.assertEqual([], rng)


if __name__ == '__main__':
    tests.main()
