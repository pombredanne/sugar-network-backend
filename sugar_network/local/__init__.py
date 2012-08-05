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

import os
import errno
from os.path import join, exists, abspath, dirname, expanduser

from active_toolkit.options import Option
from sugar_network.toolkit import sugar


api_url = Option(
        'url to connect to Sugar Network server API',
        default='http://api-testing.network.sugarlabs.org', short_option='-a',
        name='api-url')

certfile = Option(
        'path to SSL certificate file to connect to server via HTTPS')

no_check_certificate = Option(
        'do not check the server certificate against the available '
        'certificate authorities',
        default=False, type_cast=Option.bool_cast, action='store_true')

local_root = Option(
        'path to the directory to keep all local data',
        default=sugar.profile_path('network'))

activity_dirs = Option(
        'colon separated list of paths to directories with Sugar '
        'activities; first path will be used to keep check-in activities',
        type_cast=Option.paths_cast, type_repr=Option.paths_repr, default=[
            expanduser('~/Activities'),
            '/usr/share/sugar/activities',
            '/opt/sweets',
            ])

server_mode = Option(
        'start server to share local documents',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='server-mode')

delayed_start = Option(
        'immediate start only database and the rest on getting '
        'notification from IPC client',
        default=False, type_cast=Option.bool_cast, action='store_true')

mounts_root = Option(
        'path to a directory with remote devices mounts',
        default='/media')

lazy_open = Option(
        'do not open all indexes at once on startup',
        default=True, type_cast=Option.bool_cast, action='store_true')

tmpdir = Option(
        'if specified, use this directory for temporary files, such files '
        'might take hunder of megabytes while node synchronizing')


def path(*args):
    """Calculate a path from the root.

    :param args:
        path parts to add to the root path; if ends with empty string,
        the resulting path will be treated as a path to a directory
    :returns:
        absolute path

    """
    if not args:
        result = local_root.value
    elif args[0].startswith(os.sep):
        result = join(*args)
    else:
        result = join(local_root.value, *args)
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
        result = local_root.value
    elif args[0].startswith(os.sep):
        result = join(*args)
    else:
        result = join(local_root.value, *args)
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
