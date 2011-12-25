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
from os.path import join, exists, dirname
from gettext import gettext as _

from active_document import util
from active_document.util import enforce


root = util.Option(
        _('path to the root directory to place documents\' data and indexes'))


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

    return result
