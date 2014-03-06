# Copyright (C) 2011-2014 Aleksey Lim
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

"""Routines to treat lists of sorted and non-overlapping ranges.

List items are [`start`, `stop'] ranges. If `start` or `stop` is `None`,
it means the beginning or ending of the entire list.

"""
import sys
import collections

from sugar_network.toolkit import enforce


def contains(r, value):
    """Whether specified value included to one of ranges."""
    for start, end in r:
        if value >= start and (end is None or value <= end):
            return True
    else:
        return False


def stretch(r):
    """Remove all holes between the first and the last ranges."""
    if r:
        r[:] = [[r[0][0], r[-1][-1]]]


def include(r, start, end=None):
    """Insert specified range.

    :param start:
        either including range start or a list of
        (`start`, `end`) pairs
    :param end:
        including range end

    """
    if issubclass(type(start), collections.Iterable):
        for range_start, range_end in start:
            _include(r, range_start, range_end)
    elif start is not None:
        _include(r, start, end)


def exclude(r, start, end=None):
    """Remove specified range.

    :param start:
        either excluding range start or a list of
        (`start`, `end`) pairs
    :param end:
        excluding range end

    """
    if issubclass(type(start), collections.Iterable):
        for range_start, range_end in start:
            _exclude(r, range_start, range_end)
    else:
        _exclude(r, start, end)


def intersect(r1, r2):
    """Return an intersection between two range sets."""
    result = []
    for start1, end1 in r1:
        if end1 is None:
            end1 = sys.maxint
        for start2, end2 in r2:
            if end2 is None:
                end2 = sys.maxint
            start = max(start1, start2)
            end = min(end1, end2)
            if start > end:
                continue
            if end == sys.maxint:
                result.append([start, None])
                break
            result.append([start, end])
    return result


def _include(r, range_start, range_end):
    if range_start is None:
        range_start = 1

    range_start_new = None
    range_start_i = 0

    for range_start_i, (start, end) in enumerate(r):
        if range_end is not None and start - 1 > range_end:
            break
        if (range_end is None or start - 1 <= range_end) and \
                (end is None or end + 1 >= range_start):
            range_start_new = min(start, range_start)
            break
    else:
        range_start_i += 1

    if range_start_new is None:
        r.insert(range_start_i, [range_start, range_end])
        return

    range_end_new = range_end
    range_end_i = range_start_i
    for i, (start, end) in enumerate(r[range_start_i:]):
        if range_end is not None and start - 1 > range_end:
            break
        if range_end is None or end is None:
            range_end_new = None
        else:
            range_end_new = max(end, range_end)
        range_end_i = range_start_i + i

    del r[range_start_i:range_end_i]
    r[range_start_i] = [range_start_new, range_end_new]


def _exclude(r, range_start, range_end):
    enforce(range_start is not None or range_end is not None)

    if range_start is None:
        for i, interval in enumerate(r):
            start, end = interval
            if range_end < start:
                del r[:i]
                return
            if end is not None:
                if range_end == end:
                    del r[:i + 1]
                    return
                if range_end < end:
                    interval[0] = min(range_end + 1, end)
                    del r[:i]
                    return
        if r and r[-1][1] is None:
            r[:] = [[range_end + 1, None]]
        else:
            del r[:]
        return

    if range_end is None:
        for i, interval in enumerate(r):
            start, end = interval
            if end is None or range_start <= end:
                if range_start <= start:
                    del r[i:]
                else:
                    interval[1] = range_start - 1
                    del r[i + 1:]
                return
        return

    enforce(range_start <= range_end and range_start > 0,
            'Start value %r is less than 0 or not less than %r',
            range_start, range_end)

    for i, interval in enumerate(r):
        start, end = interval

        if end is not None and end < range_start:
            # Current `interval` is below new one
            continue

        if range_end is not None and range_end < start:
            # Current `interval` is above new one
            continue

        if end is None or end > range_end:
            # Current `interval` will exist after changing
            r[i] = [range_end + 1, end]
            if start < range_start:
                r.insert(i, [start, range_start - 1])
        else:
            if start < range_start:
                r[i] = [start, range_start - 1]
            else:
                del r[i]

        if end is not None:
            range_start = end + 1
            if range_start < range_end:
                exclude(r, range_start, range_end)
        break
