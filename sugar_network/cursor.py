# Copyright (C) 2012, Aleksey Lim
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

import logging
import collections
from gettext import gettext as _

from sugar_network.objects import Object
from active_document import coroutine, util, enforce


_QUERY_PAGES_NUMBER = 2


_logger = logging.getLogger('sugar_network.cursor')


class Cursor(object):

    def __init__(self, bus, query, order_by, reply, page_size, **filters):
        self._bus = bus
        self._query = query
        self._order_by = order_by
        self._reply = reply or ['guid']
        if 'guid' not in self._reply:
            self._reply.append('guid')
        self._page_size = page_size
        self._filters = filters
        self._total = None
        self._page_access = collections.deque([], _QUERY_PAGES_NUMBER)
        self._pages = {}
        self._offset = -1
        self._wait_session = None

        self._bus.connect(self.__event_cb)

    def __del__(self):
        self._bus.disconnect(self.__event_cb)

    # pylint: disable-msg=E1101,E0102,E0202
    @property
    def offset(self):
        """Current position in query results."""
        return self._offset

    @offset.setter
    def offset(self, value):
        """Change current position in query results."""
        self._offset = max(-1, value)

    @property
    def total(self):
        """Total number of objects."""
        if self._total is None:
            if not self._fetch_page(0):
                return 0
        return self._total

    @property
    def order_by(self):
        """Current order of resulting list.

        Name of property to sort by. Might be prefixed by either `+` or `-`
        to change order's direction.

        """
        return self._order_by

    # pylint: disable-msg=E1101,E0102
    @order_by.setter
    def order_by(self, value):
        if self._order_by == value:
            return
        self._order_by = value
        self._reset()

    def read_events(self):
        if self._wait_session is None:
            self._wait_session = _WaitSession()

        with self._wait_session as session:
            while session.wait():
                for event in session:
                    if event['event'] == 'commit':
                        # TODO If cursor formed by fulltext query,
                        # it should refreshed as well
                        continue
                    # TODO Replace by changed offset
                    self._reset()
                    yield None

    def __iter__(self):
        while self.offset + 1 < self.total:
            self.offset += 1
            obj = self.get(self.offset)
            if obj is None:
                break
            yield obj
        self.offset = -1

    def filter(self, query=None, **filters):
        """Change query parameters.

        :param query:
            full text search query string in Xapian format
        :param filters:
            a dictionary of properties to filter resulting list

        """
        if query == self._query and filters == self._filters:
            return
        self._query = query
        self._filters = filters
        self._reset()

    def get(self, key, default=None):
        """Get either object by key or default value.

        :param key:
            `key` value might be an `int` value (offset within the cursor),
            or a string to treat it as GUID
        :param default:
            value to return if key if not found
        :returns:
            `Object` value or `default`

        """
        if type(key) is not int:
            for page in self._pages.values():
                for obj in page:
                    if obj is not None and obj.guid == key:
                        return obj
            return Object(self._bus, self._reply, key)
        else:
            offset = key

        if offset < 0 or self._total is not None and \
                (offset >= self._total):
            return default

        page = offset / self._page_size
        offset -= page * self._page_size

        if page not in self._pages:
            if not self._fetch_page(page):
                return default

        if offset >= len(self._pages[page]):
            total = page + len(self._pages[page])
            _logger.warning('Miscalculated total number, %s instead of %s',
                    total, self._total)
            self._total = total
            return default

        return self._pages[page][offset]

    def __getitem__(self, key):
        """Get object by key.

        :param key:
            `key` value might be an `int` value (offset within the cursor),
            or a string to treat it as GUID
        :returns:
            `Object` value or raise `KeyError` exception if key is not found

        """
        result = self.get(key)
        enforce(result is not None, KeyError, _('Key is out of range'))
        return result

    def _fetch_page(self, page):
        offset = page * self._page_size

        params = {}
        if self._filters:
            params.update(self._filters)
        params['offset'] = offset
        params['limit'] = self._page_size
        if self._query:
            params['query'] = self._query
        if self._order_by:
            params['order_by'] = self._order_by
        if self._reply:
            params['reply'] = self._reply

        try:
            response = self._bus.send('GET', **params)
            self._total = response['total']
        except Exception:
            util.exception(_logger,
                    _('Failed to fetch query result: resource=%r query=%r'),
                    self._bus, params)
            self._total = None
            return False

        result = [None] * len(response['result'])
        for i, props in enumerate(response['result']):
            result[i] = Object(self._bus, self._reply,
                    props['guid'], props, offset + i)

        if not self._page_access or self._page_access[-1] != page:
            if len(self._page_access) == _QUERY_PAGES_NUMBER:
                del self._pages[self._page_access[0]]
            self._page_access.append(page)
        self._pages[page] = result

        return True

    def _reset(self):
        self._page_access.clear()
        self._pages.clear()
        self._total = None

    def __event_cb(self, event):
        # TODO More optimal resetting
        self._reset()

        if self._wait_session is not None:
            self._wait_session.push(event)


class _WaitSession(object):

    def __init__(self):
        self._signal = coroutine.Event()
        self._users = 0
        self._queue = collections.deque()

    def __enter__(self):
        if self._users:
            self._signal.set()
            self._signal.clear()
        self._users += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._users -= 1

    def push(self, event):
        if self._users > 0:
            self._queue.append(event)
            self._signal.set()
            self._signal.clear()

    def wait(self):
        self._queue.clear()
        self._signal.wait()
        return bool(self._queue)

    def __iter__(self):
        return iter(self._queue)
