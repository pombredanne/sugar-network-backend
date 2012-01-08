# Copyright (C) 2011, Aleksey Lim
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

from gettext import gettext as _

from active_document import util


#: To invalidate existed Xapian db on stcuture changes in stored documents
LAYOUT_VERSION = 1

#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

#: Additional Xapian term prefix for exact search terms
EXACT_PREFIX = 'X'


data_root = util.Option(
        _('path to the root directory to place documents\' data and indexes'))

index_flush_timeout = util.Option(
        _('flush index index after specified seconds since the last change'),
        default=5, type_cast=int)

index_flush_threshold = util.Option(
        _('flush index every specified changes'),
        default=32, type_cast=int)

index_write_queue = util.Option(
        _('for concurent access, run index writer in separate thread; ' \
                'this option specifies the writer\'s queue size; ' \
                '0 means not threading the writer'),
        default=0, type_cast=int)

find_limit = util.Option(
        _('limit the resulting list for search requests'),
        default=32, type_cast=int)


def term(prefix, term_value):
    """Compose full Xapian term value applying all needed prefixes.

    :param prefix:
        term prefix
    :param term_value:
        term value; for long strings, only short partion will be used
        to avoid storing big terms
    :returns:
        final term value

    """
    return EXACT_PREFIX + prefix + str(term_value).split('\n')[0][:243]


def value(raw_value):
    """Convert value to a string before passing it to Xapian.

    :param raw_value:
        arbitrary type value
    :returns:
        value in string representation

    """
    if isinstance(raw_value, unicode):
        return raw_value.encode('utf-8')
    elif isinstance(raw_value, bool):
        return '1' if raw_value else '0'
    elif not isinstance(raw_value, basestring):
        return str(raw_value)
    else:
        return raw_value


class NotFound(Exception):
    pass
