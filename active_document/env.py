# Copyright (C) 2011-2012, Aleksey Lim
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
import logging
import collections
from os.path import exists, join
from gettext import gettext as _

from active_document import util
from active_document.util import enforce


_logger = logging.getLogger('ad')


#: To invalidate existed Xapian db on stcuture changes in stored documents
LAYOUT_VERSION = 1

#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

#: Additional Xapian term prefix for exact search terms
EXACT_PREFIX = 'X'

ACCESS_CREATE = 1
ACCESS_WRITE = 2
ACCESS_READ = 4
ACCESS_DELETE = 8
ACCESS_FULL = 0xFFFF

ACCESS_NAMES = {
        ACCESS_CREATE: _('Create'),
        ACCESS_WRITE: _('Write'),
        ACCESS_READ: _('Read'),
        ACCESS_DELETE: _('Delete'),
        }


data_root = util.Option(
        _('path to the root directory for placing documents\' ' \
                'data and indexes'),
        default='/var/lib/sugar-network/db')

index_flush_timeout = util.Option(
        _('flush index index after specified seconds since the last change'),
        default=5, type_cast=int)

index_flush_threshold = util.Option(
        _('flush index every specified changes'),
        default=32, type_cast=int)

index_write_queue = util.Option(
        _('for concurent access, run index writer in separate thread; ' \
                'this option specifies the writer\'s queue size'),
        default=256, type_cast=int)

find_limit = util.Option(
        _('limit the resulting list for search requests'),
        default=32, type_cast=int)


class NotFound(Exception):
    """Document was not found."""
    pass


class Forbidden(Exception):
    """Caller does not have permissions to get access."""
    pass


class Range(list):
    """List of sorted, non-overlapping ranges on the same scale.

    List items are ranges, [`start`, `stop']. If `start` or `stop`
    is `None`, it means the beginning or ending of the enire scale.

    """

    def __init__(self, name=None, init_value=None):
        """
        :param name:
            if set, `Range` value will be restored on creation and
            can be stored by `flush()` function
        :param init_value:
            if not `None`, the initial value for the range

        """
        if not name:
            self._path = None
        else:
            self._path = join(data_root.value, name + '.range')
        if init_value is None:
            self._init_value = []
        else:
            self._init_value = [init_value]

        if self._path and exists(self._path):
            with file(self._path) as f:
                self.extend(json.load(f))
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
        return self == self._init_value

    def clear(self):
        """Reset range to the initial value."""
        self[:] = self._init_value

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

    def commit(self):
        """If timeline supports persistent saving, store current state."""
        enforce(self._path)
        with util.new_file(self._path) as f:
            json.dump(self, f)
            f.flush()
            os.fsync(f.fileno())

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


class Query(object):

    def __init__(self, offset=None, limit=None, request=None, query='',
            reply=None, order_by=None):
        self.query = query

        if offset is None:
            offset = 0
        self.offset = offset

        if limit is None:
            limit = find_limit.value
        elif limit > find_limit.value:
            _logger.warning(_('The find limit is restricted to %s'),
                    find_limit.value)
            limit = find_limit.value
        self.limit = limit

        if request is None:
            request = {}
        self.request = request

        if reply is None:
            reply = ['guid']
        self.reply = reply

        if order_by is None:
            order_by = 'ctime'
        self.order_by = order_by

    def __repr__(self):
        return 'offset=%s limit=%s request=%r query=%r order_by=%s' % \
                (self.offset, self.limit, self.request, self.query,
                        self.order_by)
