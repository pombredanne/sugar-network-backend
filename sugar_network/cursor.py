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

from gevent.event import Event
from active_document import util, enforce


_QUERY_PAGES_NUMBER = 2


_logger = logging.getLogger('sugar_network')


class Cursor(object):

    def __init__(self, request, object_class, query, order_by, reply,
            page_size, **filters):
        self._request = request
        self._object_class = object_class
        self._query = query
        self._order_by = order_by
        self._reply = reply or ['guid']
        if 'guid' not in self._reply:
            self._reply.append('guid')
        if self._request.online:
            if 'keep' in self._reply:
                self._reply.remove('keep')
            if 'keep_impl' in self._reply:
                self._reply.remove('keep_impl')
        self._page_size = page_size
        self._filters = filters
        self._total = None
        self._page_access = collections.deque([], _QUERY_PAGES_NUMBER)
        self._pages = {}
        self._offset = -1
        self._subscription = None

        self._reset()

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
        if self._subscription is None:
            self._subscription = _Subscription(self._request)

        with self._subscription as subscription:
            while subscription.wait():
                for __ in subscription.events():
                    self._reset()
                    # TODO Replace by changed offset
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
            return self._object_class(self._request, self._reply, key)
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
            response = self._request.send('GET', **params)
            self._total = response['total']
        except Exception:
            util.exception(_logger,
                    _('Failed to fetch query result: resource=%r query=%r'),
                    self._request, params)
            self._total = None
            return False

        result = [None] * len(response['result'])
        for i, props in enumerate(response['result']):
            result[i] = self._object_class(self._request, self._reply,
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


class _Subscription(object):

    def __init__(self, request):
        self._request = request
        self._signal = Event()
        self._users = 0
        self._queue = collections.deque()

    def __enter__(self):
        if not self._users:
            _logger.debug('Start listening notifications for %r',
                    self._request)
            self._request.connect(self.__event_cb)
        self._users += 1
        self._replace()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._users -= 1
        if not self._users:
            _logger.debug('Stop listening notifications for %r',
                    self._request)
            self._request.disconnect(self.__event_cb)

    def wait(self):
        self._queue.clear()
        self._signal.wait()
        return bool(self._queue)

    def events(self):
        return iter(self._queue)

    def _replace(self):
        if self._users <= 1:
            return
        self._queue.clear()
        self._signal.set()
        self._signal.clear()

    def __event_cb(self, event):
        self._queue.append(event)
        self._signal.set()
        self._signal.clear()
