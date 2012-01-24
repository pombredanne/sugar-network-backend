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


class NotFound(Exception):
    """Document was not found."""
    pass


class Forbidden(Exception):
    """Caller does not have permissions to get access."""
    pass
