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

import os
from os.path import join, exists, dirname, abspath
from gettext import gettext as _

from active_document import util
from active_document.util import enforce


#: To invalidate existed Xapian db on stcuture changes in stored documents
LAYOUT_VERSION = 1

#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

#: Additional Xapian term prefix for exact search terms
EXACT_PREFIX = 'X'


root = util.Option(
        _('path to the root directory to place documents\' data and indexes'))

flush_timeout = util.Option(
        _('force a flush after specified seconds since the last ' \
                'index change'),
        default=5, type_cast=int)

flush_threshold = util.Option(
        _('force a flush every specified changes to the index'),
        default=32, type_cast=int)

threading = util.Option(
        _('use index from different threads in optimal manner'),
        default=False, type_cast=bool)

write_queue = util.Option(
        _('if threading is enabled, define the queue size of ' \
                'singular writer; 0 is infinite size'),
        default=256, type_cast=int)

find_limit = util.Option(
        _('limit the resulting list for search requests'),
        default=64, type_cast=int)


def path(*args):
    """Calculate a path from the root one.

    If resulting directory path doesn't exists, it will be created.

    :param args:
        path parts to add to the root path; if ends with empty string,
        the resulting path will be treated as a path to a directory
    :returns:
        absolute path

    """
    enforce(root.value,
            _('The active_document.env.root.value is not set'))

    result = join(root.value, *args)
    if result.endswith(os.sep):
        result_dir = result = result.rstrip(os.sep)
    else:
        result_dir = dirname(result)

    if not exists(result_dir):
        os.makedirs(result_dir)

    return abspath(result)


def index_path(name):
    """Path to a directory with Xapian index.

    :param name:
        Xapian database name
    :returns:
        absolute path

    """
    return path(name, 'index', '')


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
