# Copyright (C) 2012-2013 Aleksey Lim
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
from os.path import join, expanduser

from sugar_network.toolkit import Option, sugar


api_url = Option(
        'url to connect to Sugar Network server API',
        default='http://api-devel.network.sugarlabs.org', short_option='-a',
        name='api-url')

certfile = Option(
        'path to SSL certificate file to connect to server via HTTPS')

no_check_certificate = Option(
        'do not check the server certificate against the available '
        'certificate authorities',
        default=False, type_cast=Option.bool_cast, action='store_true')

local_root = Option(
        'path to the directory to keep all local data',
        default=sugar.profile_path('network'), name='local_root')

activity_dirs = Option(
        'colon separated list of paths to directories with Sugar '
        'activities; first path will be used to keep check-in activities',
        type_cast=Option.paths_cast, type_repr=Option.paths_repr, default=[
            expanduser('~/Activities'),
            '/usr/share/sugar/activities',
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
        default=False, type_cast=Option.bool_cast, action='store_true')

ipc_port = Option(
        'port number to listen for incomming connections from IPC clients',
        default=5001, type_cast=int, name='ipc_port')

hub_root = Option(
        'path to Contributor Hub site directory to serve from /hub location '
        'for IPC clients to workaround lack of CORS for SSE while using Hub '
        'from file:// url',
        default='/usr/share/sugar-network/hub')

layers = Option(
        'space separated list of layers to restrict Sugar Network content by',
        default=[], type_cast=Option.list_cast, type_repr=Option.list_repr,
        name='layers')

discover_server = Option(
        'discover servers in local network instead of using --api-url',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='discover_server')

no_dbus = Option(
        'disable any DBus usage',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='no-dbus')


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


def clones(*args, **kwargs):
    import sugar_network.zerosugar.clones
    return sugar_network.zerosugar.clones.walk(*args, **kwargs)


def Client(url=None, sugar_auth=True, **session):
    from sugar_network.toolkit import http
    if url is None:
        url = api_url.value
    return http.Client(url, sugar_auth=sugar_auth, **session)


def IPCClient(**session):
    from sugar_network.toolkit import http
    # Since `IPCClient` uses only localhost, ignore `http_proxy` envar
    session['config'] = {'trust_env': False}
    return http.Client('http://localhost:%s' % ipc_port.value, **session)
