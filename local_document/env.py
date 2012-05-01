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

import os
from os.path import join, exists, abspath, dirname
from gettext import gettext as _

from active_document import optparse


ipc_root = optparse.Option(
        _('path to a directory with IPC sockets'))

api_url = optparse.Option(
        _('url to connect to Sugar Network server API'),
        default='http://18.85.44.120:8000', short_option='-a')

certfile = optparse.Option(
        _('path to SSL certificate file to connect to server via HTTPS'))

no_check_certificate = optparse.Option(
        _('do not check the server certificate against the available ' \
                'certificate authorities'),
        default=False, type_cast=optparse.Option.bool_cast,
        action='store_true')

local_data_root = optparse.Option(
        _('path to directory to keep local data; ' \
                'if omited, ~/sugar/*/sugar-network directory will be used'))


def ensure_path(path, *args):
    """Calculate a path from the root.

    If resulting directory path doesn't exists, it will be created.

    :param args:
        path parts to add to the root path; if ends with empty string,
        the resulting path will be treated as a path to a directory
    :returns:
        absolute path

    """
    result = join(path, *args)
    if result.endswith(os.sep):
        result_dir = result = result.rstrip(os.sep)
    else:
        result_dir = dirname(result)
    if not exists(result_dir):
        os.makedirs(result_dir)
    return abspath(result)
