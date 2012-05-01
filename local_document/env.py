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
import errno
from os.path import join, exists, abspath, dirname, expanduser
from gettext import gettext as _

from active_document import optparse
from local_document import sugar


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
        _('path to the directory to keep all local data'),
        default=sugar.profile_path('network'))

activities_root = optparse.Option(
        _('path to the default directory with Sugar activities'),
        default=expanduser('~/Activities'))


def path(*args):
    """Calculate a path from the root.

    :param args:
        path parts to add to the root path; if ends with empty string,
        the resulting path will be treated as a path to a directory
    :returns:
        absolute path

    """
    if not args:
        result = local_data_root.value
    elif args[0].startswith(os.sep):
        result = join(*args)
    else:
        result = join(local_data_root.value, *args)
    return str(result)


def ensure_path(*args):
    """Calculate a path from the root.

    If resulting directory path doesn't exists, it will be created.

    :param args:
        path parts to add to the root path; if ends with empty string,
        the resulting path will be treated as a path to a directory
    :returns:
        absolute path

    """
    if not args:
        result = local_data_root.value
    elif args[0].startswith(os.sep):
        result = join(*args)
    else:
        result = join(local_data_root.value, *args)
    result = str(result)

    if result.endswith(os.sep):
        result_dir = result = result.rstrip(os.sep)
    else:
        result_dir = dirname(result)

    if not exists(result_dir):
        try:
            os.makedirs(result_dir)
        except OSError, error:
            # In case if another process already create directory
            if error.errno != errno.EEXIST:
                raise

    return abspath(result)
