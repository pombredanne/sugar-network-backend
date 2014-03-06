#!/usr/bin/env python
# sugar-lint: disable

import copy
from os.path import exists
from cStringIO import StringIO

from __init__ import tests

from sugar_network import toolkit
from sugar_network.toolkit import ranges


class RangesTest(tests.Test):

    def test_exclude(self):
        r = [[1, None]]
        ranges.exclude(r, 1, 10)
        self.assertEqual(
                [[11, None]],
                r)
        r = [[1, None]]
        ranges.exclude(r, 5, 10)
        self.assertEqual(
                [[1, 4], [11, None]],
                r)
        ranges.exclude(r, 2, 2)
        self.assertEqual(
                [[1, 1], [3, 4], [11, None]],
                r)
        ranges.exclude(r, 1, 1)
        self.assertEqual(
                [[3, 4], [11, None]],
                r)
        ranges.exclude(r, 3, 3)
        self.assertEqual(
                [[4, 4], [11, None]],
                r)
        ranges.exclude(r, 1, 20)
        self.assertEqual(
                [[21, None]],
                r)
        ranges.exclude(r, 21, 21)
        self.assertEqual(
                [[22, None]],
                r)

        r = [[100, None]]
        ranges.exclude(r, [[1, 98]])
        self.assertEqual([[100, None]], r)

        r = [[1, 100]]
        ranges.exclude(r, [[200, 300]])
        self.assertEqual([[1, 100]], r)

    def test_exclude_OpenStart(self):
        r = [[1, None]]
        self.assertRaises(RuntimeError, ranges.exclude, r, None, None)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 1)
        self.assertEqual([[10, 20], [30, None]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 10)
        self.assertEqual([[11, 20], [30, None]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 15)
        self.assertEqual([[16, 20], [30, None]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 20)
        self.assertEqual([[30, None]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 35)
        self.assertEqual([[36, None]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, None, 50)
        self.assertEqual([[51, None]], r)

        r = [[10, 20], [30, 40]]
        ranges.exclude(r, None, 50)
        self.assertEqual([], r)

        r = [[2, 2]]
        ranges.exclude(r, None, 2)
        self.assertEqual([], r)

    def test_exclude_OpenEnd(self):
        r = [[10, 20], [30, None]]
        ranges.exclude(r, 50, None)
        self.assertEqual([[10, 20], [30, 49]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 30, None)
        self.assertEqual([[10, 20]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 25, None)
        self.assertEqual([[10, 20]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 20, None)
        self.assertEqual([[10, 19]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 11, None)
        self.assertEqual([[10, 10]], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 10, None)
        self.assertEqual([], r)

        r = [[10, 20], [30, None]]
        ranges.exclude(r, 1, None)
        self.assertEqual([], r)

    def test_include_JoinExistingItems(self):
        r = []

        ranges.include(r, 1, None)
        self.assertEqual(
                [[1, None]],
                r)

        ranges.include(r, 2, None)
        self.assertEqual(
                [[1, None]],
                r)

        ranges.include(r, 4, 5)
        self.assertEqual(
                [[1, None]],
                r)

        ranges.exclude(r, 2, 2)
        ranges.exclude(r, 4, 4)
        ranges.exclude(r, 6, 6)
        ranges.exclude(r, 9, 9)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, 8],
                    [10, None]],
                r)

        ranges.include(r, 10, 20)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, 8],
                    [10, None]],
                r)

        ranges.include(r, 8, 20)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, 5],
                    [7, None]],
                r)

        ranges.include(r, 5, None)
        self.assertEqual(
                [[1, 1],
                    [3, 3],
                    [5, None]],
                r)

        ranges.include(r, 1, None)
        self.assertEqual(
                [[1, None]],
                r)

    def test_include_InsertNewItems(self):
        r = []

        ranges.include(r, 8, 10)
        ranges.include(r, 3, 3)
        self.assertEqual(
                [[3, 3],
                    [8, 10]],
                r)

        ranges.include(r, 9, 11)
        self.assertEqual(
                [[3, 3],
                    [8, 11]],
                r)

        ranges.include(r, 7, 12)
        self.assertEqual(
                [[3, 3],
                    [7, 12]],
                r)

        ranges.include(r, 5, 5)
        self.assertEqual(
                [[3, 3],
                    [5, 5],
                    [7, 12]],
                r)

        ranges.include(r, 4, 4)
        self.assertEqual(
                [[3, 5],
                    [7, 12]],
                r)

        ranges.include(r, 1, 1)
        self.assertEqual(
                [[1, 1],
                    [3, 5],
                    [7, 12]],
                r)

        ranges.include(r, 2, None)
        self.assertEqual(
                [[1, None]],
                r)

    def teste_Invert(self):
        r1 = [[1, None]]
        ranges.exclude(r1, 2, 2)
        ranges.exclude(r1, 5, 10)

        r2 = copy.deepcopy(r1)
        r2[-1][1] = 20

        self.assertEqual(
                [
                    [1, 1],
                    [3, 4],
                    [11, None],
                    ],
                r1)
        ranges.exclude(r1, r2)
        self.assertEqual(
                [[21, None]],
                r1)

    def test_contains(self):
        r = [[1, None]]

        assert ranges.contains(r, 1)
        assert ranges.contains(r, 4)

        ranges.exclude(r, 2, 2)
        ranges.exclude(r, 5, 10)

        assert ranges.contains(r, 1)
        assert not ranges.contains(r, 2)
        assert ranges.contains(r, 3)
        assert not ranges.contains(r, 5)
        assert not ranges.contains(r, 10)
        assert ranges.contains(r, 11)
        assert ranges.contains(r, 12)

    def test_stretch(self):
        r = []
        ranges.stretch(r)
        self.assertEqual([], r)

        r = [[1, None]]
        ranges.stretch(r)
        self.assertEqual([[1, None]], r)

        r = [[1, 10]]
        ranges.stretch(r)
        self.assertEqual([[1, 10]], r)

        r = [[1, 1], [3, 3], [5, None]]
        ranges.stretch(r)
        self.assertEqual([[1, None]], r)

        r = [[3, 3], [5, 10]]
        ranges.stretch(r)
        self.assertEqual([[3, 10]], r)

    def test_include(self):
        r = []
        ranges.include(r, 2, 2)
        self.assertEqual(
                [[2, 2]],
                r)
        ranges.include(r, 7, 10)
        self.assertEqual(
                [[2, 2], [7, 10]],
                r)
        ranges.include(r, 5, 5)
        self.assertEqual(
                [[2, 2], [5, 5], [7, 10]],
                r)
        ranges.include(r, 15, None)
        self.assertEqual(
                [[2, 2], [5, 5], [7, 10], [15, None]],
                r)
        ranges.include(r, 3, 5)
        self.assertEqual(
                [[2, 5], [7, 10], [15, None]],
                r)
        ranges.include(r, 11, 14)
        self.assertEqual(
                [[2, 5], [7, None]],
                r)

        r = []
        ranges.include(r, 10, None)
        self.assertEqual(
                [[10, None]],
                r)
        ranges.include(r, 7, 8)
        self.assertEqual(
                [[7, 8], [10, None]],
                r)
        ranges.include(r, 2, 2)
        self.assertEqual(
                [[2, 2], [7, 8], [10, None]],
                r)

    def test_Union(self):
        r1 = []
        ranges.include(r1, 1, 2)
        r2 = []
        ranges.include(r2, 3, 4)
        ranges.include(r1, r2)
        self.assertEqual(
                [[1, 4]],
                r1)

        r1 = []
        ranges.include(r1, 1, None)
        r2 = []
        ranges.include(r2, 3, 4)
        ranges.include(r1, r2)
        self.assertEqual(
                [[1, None]],
                r1)

        r2 = []
        ranges.include(r2, 1, None)
        r1 = []
        ranges.include(r1, 3, 4)
        ranges.include(r1, r2)
        self.assertEqual(
                [[1, None]],
                r1)

        r1 = []
        ranges.include(r1, 1, None)
        r2 = []
        ranges.include(r2, 2, None)
        ranges.include(r1, r2)
        self.assertEqual(
                [[1, None]],
                r1)

        r1 = []
        r2 = []
        ranges.include(r2, r1)
        self.assertEqual([], r2)

        r1 = []
        r2 = []
        ranges.include(r2, 1, None)
        ranges.include(r2, r1)
        self.assertEqual([[1, None]], r2)

        r = []
        ranges.include(r, 10, 11)
        ranges.include(r, None)
        self.assertEqual([[10, 11]], r)

    def test_intersect_Closed(self):
        self.assertEqual(
                [],
                ranges.intersect([], []))
        self.assertEqual(
                [],
                ranges.intersect([[1, 1]], []))
        self.assertEqual(
                [],
                ranges.intersect([], [[1, 1]]))

        self.assertEqual(
                [[1, 1]],
                ranges.intersect([[1, 1]], [[1, 1]]))
        self.assertEqual(
                [[1, 1]],
                ranges.intersect([[1, 10]], [[1, 1]]))
        self.assertEqual(
                [[1, 1]],
                ranges.intersect([[1, 1]], [[1, 10]]))

        self.assertEqual(
                [[2, 5]],
                ranges.intersect([[2, 10]], [[1, 5]]))
        self.assertEqual(
                [[2, 5]],
                ranges.intersect([[1, 5]], [[2, 10]]))

        self.assertEqual(
                [[2, 3], [5, 7]],
                ranges.intersect([[1, 10]], [[2, 3], [5, 7]]))
        self.assertEqual(
                [[2, 3], [5, 7]],
                ranges.intersect([[2, 3], [5, 7]], [[1, 10]]))

        self.assertEqual(
                [[2, 2], [4, 4], [7, 7]],
                ranges.intersect([[1, 2], [4, 5], [6, 8], [10, 11]], [[0, 0], [2, 4], [7, 7]]))
        self.assertEqual(
                [[2, 2], [4, 4], [7, 7]],
                ranges.intersect([[0, 0], [2, 4], [7, 7]], [[1, 2], [4, 5], [6, 8], [10, 11]]))

    def test_intersect_Open(self):
        self.assertEqual(
                [[1, None]],
                ranges.intersect([[1, None]], [[1, None]]))

        self.assertEqual(
                [[2, None]],
                ranges.intersect([[2, None]], [[1, None]]))
        self.assertEqual(
                [[2, None]],
                ranges.intersect([[1, None]], [[2, None]]))

        self.assertEqual(
                [[2, 3], [5, None]],
                ranges.intersect([[2, 3], [5, None]], [[1, None]]))
        self.assertEqual(
                [[2, 3], [5, None]],
                ranges.intersect([[1, None]], [[2, 3], [5, None]]))


if __name__ == '__main__':
    tests.main()
