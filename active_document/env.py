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
import locale
import logging
from uuid import uuid1
from os.path import exists

from active_toolkit.options import Option
from active_toolkit import util


_logger = logging.getLogger('active_document')


#: Default language to fallback for localized properties
DEFAULT_LANG = 'en'

#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

#: Additional Xapian term prefix for exact search terms
EXACT_PREFIX = 'X'

ACCESS_CREATE = 1
ACCESS_WRITE = 2
ACCESS_READ = 4
ACCESS_DELETE = 8
ACCESS_PUBLIC = ACCESS_CREATE | ACCESS_WRITE | ACCESS_READ | ACCESS_DELETE

ACCESS_AUTH = 16
ACCESS_AUTHOR = 32

ACCESS_SYSTEM = 64
ACCESS_LOCAL = 128
ACCESS_REMOTE = 256
ACCESS_LEVELS = ACCESS_SYSTEM | ACCESS_LOCAL | ACCESS_REMOTE

ACCESS_NAMES = {
        ACCESS_CREATE: 'Create',
        ACCESS_WRITE: 'Write',
        ACCESS_READ: 'Read',
        ACCESS_DELETE: 'Delete',
        }


index_flush_timeout = Option(
        'flush index index after specified seconds since the last change',
        default=5, type_cast=int)

index_flush_threshold = Option(
        'flush index every specified changes',
        default=32, type_cast=int)

index_write_queue = Option(
        'if active-document is being used for the scheme with one writer '
            'process and multiple reader processes, this option specifies '
            'the writer\'s queue size',
        default=256, type_cast=int)


def uuid():
    """Generate GUID value.

    Function will tranform `uuid.uuid1()` result to leave only alnum symbols.
    The reason is reusing the same resulting GUID in different cases, e.g.,
    for Telepathy names where `-` symbols, from `uuid.uuid1()`, are not
    permitted.

    :returns:
        GUID string value

    """
    return ''.join(str(uuid1()).split('-'))


def default_lang():
    lang = locale.getdefaultlocale()[0]
    if lang:
        return lang.replace('_', '-')
    else:
        return DEFAULT_LANG


class BadRequest(Exception):
    """Bad requested resource."""
    pass


class NotFound(Exception):
    """Resource was not found."""
    pass


class Forbidden(Exception):
    """Caller does not have permissions to get access."""
    pass


class Query(object):

    def __init__(self, offset=None, limit=None, query='', reply=None,
            order_by=None, no_cache=False, group_by=None, **kwargs):
        """
        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size;
            the `--find-limit` will be used by default
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param reply:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            property name to sort resulting list; might be prefixed with ``+``
            (or without any prefixes) for ascending order, and ``-`` for
            descending order
        :param group_by:
            property name to group resulting list by; no groupping by default
        :param kwargs:
            a dictionary with property values to restrict the search

        """
        self.query = query
        self.no_cache = no_cache
        self.group_by = group_by

        if offset is None:
            offset = 0
        self.offset = offset

        self.limit = limit or 16

        if reply is None:
            reply = ['guid']
        self.reply = reply

        if order_by is None:
            order_by = 'ctime'
        self.order_by = order_by

        self.request = kwargs

    def __repr__(self):
        return 'offset=%s limit=%s request=%r query=%r order_by=%s ' \
               'group_by=%s' % (self.offset, self.limit, self.request,
                       self.query, self.order_by, self.group_by)


class Seqno(object):
    """Sequence number counter with persistent storing in a file."""

    def __init__(self, path):
        """
        :param path:
            path to file to [re]store seqno value

        """
        self._path = path
        self._value = 0

        if exists(path):
            with file(path) as f:
                self._value = int(f.read().strip())

        self._orig_value = self._value

    @property
    def value(self):
        """Current seqno value."""
        return self._value

    def next(self):
        """Incerement seqno.

        :returns:
            new seqno value

        """
        self._value += 1
        return self._value

    def commit(self):
        """Store current seqno value in a file.

        :returns:
            `True` if commit was happened

        """
        if self._value == self._orig_value:
            return False
        with util.new_file(self._path) as f:
            f.write(str(self._value))
            f.flush()
            os.fsync(f.fileno())
        self._orig_value = self._value
        return True
