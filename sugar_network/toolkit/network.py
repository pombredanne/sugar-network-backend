# Copyright (C) 2012 Aleksey Lim
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

import ctypes
import logging
from ctypes.util import find_library


_logger = logging.getLogger('network')


def res_init():
    """Reset resolving cache.

    Calling this function will enforce libc to avoid using stale resolving
    cache after getting [re]connected. For example, if application process
    was launched when there were no any DNS servers available, after getting
    connected, call `res_init()` to reuse newly appeared DNS servers.

    """
    try:
        lib_name = find_library('c')
        libc = ctypes.CDLL(lib_name)
        getattr(libc, '__res_init')(None)
    except Exception:
        _logger.exception('Failed to call res_init()')
