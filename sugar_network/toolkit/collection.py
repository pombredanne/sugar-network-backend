# Copyright (C) 2011-2012 Aleksey Lim
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

import os
import json
import collections
from os.path import exists
from gettext import gettext as _

from active_toolkit import util, enforce


class Sequence(list):
    """List of sorted and non-overlapping ranges.

    List items are ranges, [`start`, `stop']. If `start` or `stop`
    is `None`, it means the beginning or ending of the entire scale.

    """

    def __init__(self, value=None, empty_value=None):
        """
        :param value:
            default value to initialize range
        :param empty_value:
            if not `None`, the initial value for empty range

        """
        if empty_value is None:
            self._empty_value = []
        else:
            self._empty_value = [empty_value]

        if value:
            self.extend(value)
        else:
            self.clear()

    def __contains__(self, value):
        for start, end in self:
            if value >= start and (end is None or value <= end):
                return True
        else:
            return False

    @property
    def first(self):
        if self:
            return self[0][0]
        else:
            return 0

    @property
    def empty(self):
        """Is timeline in the initial state."""
        return self == self._empty_value

    def clear(self):
        """Reset range to the initial value."""
        self[:] = self._empty_value

    def include(self, start, end=None):
        """Include specified range.

        :param start:
            either including range start or a list of
            (`start`, `end`) pairs
        :param end:
            including range end

        """
        if issubclass(type(start), collections.Iterable):
            for range_start, range_end in start:
                self._include(range_start, range_end)
        else:
            self._include(start, end)

    def exclude(self, start, end=None):
        """Exclude specified range.

        :param start:
            either excluding range start or a list of
            (`start`, `end`) pairs
        :param end:
            excluding range end

        """
        if issubclass(type(start), collections.Iterable):
            for range_start, range_end in start:
                self._exclude(range_start, range_end)
        else:
            enforce(end is not None)
            self._exclude(start, end)

    def floor(self, end):
        """Make right limit as less as `end` is."""
        i = None
        for i, (self_start, self_end) in enumerate(self):
            if self_start > end:
                break
            elif self_end is None or self_end >= end:
                self[i][1] = end
                i += 1
                break
        else:
            return
        if i < len(self):
            del self[i:]

    def _include(self, range_start, range_end):
        if range_start is None:
            range_start = 1

        range_start_new = None
        range_start_i = 0

        for range_start_i, (start, end) in enumerate(self):
            if range_end is not None and start - 1 > range_end:
                break
            if (range_end is None or start - 1 <= range_end) and \
                    (end is None or end + 1 >= range_start):
                range_start_new = min(start, range_start)
                break
        else:
            range_start_i += 1

        if range_start_new is None:
            self.insert(range_start_i, [range_start, range_end])
            return

        range_end_new = range_end
        range_end_i = range_start_i
        for i, (start, end) in enumerate(self[range_start_i:]):
            if range_end is not None and start - 1 > range_end:
                break
            if range_end is None or end is None:
                range_end_new = None
            else:
                range_end_new = max(end, range_end)
            range_end_i = range_start_i + i

        del self[range_start_i:range_end_i]
        self[range_start_i] = [range_start_new, range_end_new]

    def _exclude(self, range_start, range_end):
        if range_start is None:
            range_start = 1
        enforce(range_end is not None)
        enforce(range_start <= range_end and range_start > 0,
                _('Start value %r is less than 0 or not less than %r'),
                range_start, range_end)

        for i, interval in enumerate(self):
            start, end = interval
            if end is not None and end < range_start:
                # Current `interval` is below than new one
                continue

            if end is None or end > range_end:
                # Current `interval` will exist after changing
                self[i] = [range_end + 1, end]
                if start < range_start:
                    self.insert(i, [start, range_start - 1])
            else:
                if start < range_start:
                    self[i] = [start, range_start - 1]
                else:
                    del self[i]

            if end is not None:
                range_start = end + 1
                if range_start < range_end:
                    self.exclude(range_start, range_end)
            break


class Sequences(dict):

    def __init__(self, value=None, **kwargs):
        dict.__init__(self)
        self._new_item_kwargs = kwargs
        if value:
            self.update(value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            value = self[key] = Sequence(**self._new_item_kwargs)
        return value

    def exclude(self, other):
        for key, seq in other.items():
            if key in self:
                self[key].exclude(seq)

    def update(self, other):
        for key, seq in other.items():
            self[key] = Sequence(seq)


class PersistentSequences(Sequences):

    def __init__(self, path, empty_value, keys):
        Sequences.__init__(self, empty_value=empty_value)
        self._path = path
        if exists(self._path):
            with file(self._path) as f:
                self.update(json.load(f))
        else:
            for i in keys:
                self[i].clear()

    def commit(self):
        with util.new_file(self._path) as f:
            json.dump(self, f)
            f.flush()
            os.fsync(f.fileno())


class MutableStack(object):
    """Stack that keeps its iterators correct after changing content."""

    def __init__(self):
        self._queue = collections.deque()

    def add(self, value):
        self.remove(value)
        self._queue.appendleft([False, value])

    def remove(self, value):
        for i, (__, existing) in enumerate(self._queue):
            if existing == value:
                del self._queue[i]
                break

    def rewind(self):
        for i in self._queue:
            i[0] = False

    def __len__(self):
        return len(self._queue)

    def __iter__(self):
        return _MutableStackIterator(self._queue)

    def __repr__(self):
        return str([i[1] for i in self._queue])


class _MutableStackIterator(object):

    def __init__(self, queue):
        self._queue = queue

    def next(self):
        for i in self._queue:
            processed, value = i
            if not processed:
                i[0] = True
                return value
        raise StopIteration()
