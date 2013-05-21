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
import logging
from os.path import join, expanduser, exists

from sugar_network.toolkit import Option, util


SUGAR_API_COMPATIBILITY = {
        '0.94': frozenset(['0.86', '0.88', '0.90', '0.92', '0.94']),
        }

_NICKNAME_GCONF = '/desktop/sugar/user/nick'
_COLOR_GCONF = '/desktop/sugar/user/color'
_XO_SERIAL_PATH = ['/ofw/mfg-data/SN', '/proc/device-tree/mfg-data/SN']
_XO_UUID_PATH = ['/ofw/mfg-data/U#', '/proc/device-tree/mfg-data/U#']

_logger = logging.getLogger('client')


def profile_path(*args):
    """Path within sugar profile directory.

    Missed directories will be created.

    :param args:
        path parts that will be added to the resulting path
    :returns:
        full path with directory part existed

    """
    if os.geteuid():
        root_dir = join(os.environ['HOME'], '.sugar',
                os.environ.get('SUGAR_PROFILE', 'default'))
    else:
        root_dir = '/var/sugar-network'
    return join(root_dir, *args)


api_url = Option(
        'url to connect to Sugar Network server API',
        default='http://node-devel.sugarlabs.org', short_option='-a',
        name='api-url')

certfile = Option(
        'path to SSL certificate file to connect to server via HTTPS')

no_check_certificate = Option(
        'do not check the server certificate against the available '
        'certificate authorities',
        default=False, type_cast=Option.bool_cast, action='store_true')

local_root = Option(
        'path to the directory to keep all local data',
        default=profile_path('network'), name='local_root')

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

anonymous = Option(
        'use anonymous user to access to Sugar Network server; '
        'only read-only operations are available in this mode',
        default=False, type_cast=Option.bool_cast, action='store_true',
        name='anonymous')


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


def Client(url=None):
    from sugar_network.toolkit import http
    if url is None:
        url = api_url.value
    creds = None
    if not anonymous.value:
        if exists(key_path()):
            creds = (sugar_uid(), key_path(), _profile)
        else:
            _logger.warning('Sugar session was never started (no DSA key),'
                    'fallback to anonymous mode')
    return http.Client(url, creds=creds)


def IPCClient():
    from sugar_network.toolkit import http
    url = 'http://localhost:%s' % ipc_port.value
    # It is localhost, so, ignore `http_proxy` envar disabling `trust_env`
    return http.Client(url, creds=None, trust_env=False)


def IPCRouter(*args, **kwargs):
    from sugar_network import db
    from sugar_network.db.router import Router

    class _IPCRouter(Router):

        def authenticate(self, request):
            if not anonymous.value:
                return sugar_uid()

        def call(self, request, response):
            request.access_level = db.ACCESS_LOCAL
            return Router.call(self, request, response)

    return _IPCRouter(*args, **kwargs)


def logger_level():
    """Current Sugar logger level as --debug value."""
    _LEVELS = {
            'error': 0,
            'warning': 0,
            'info': 1,
            'debug': 2,
            'all': 3,
            }
    level = os.environ.get('SUGAR_LOGGER_LEVEL')
    return _LEVELS.get(level, 0)


def key_path():
    return profile_path('owner.key')


def sugar_uid():
    import hashlib
    pubkey = util.pubkey(key_path()).split()[1]
    return str(hashlib.sha1(pubkey).hexdigest())


def _profile():
    import gconf
    conf = gconf.client_get_default()
    return {'name': conf.get_string(_NICKNAME_GCONF) or '',
            'color': conf.get_string(_COLOR_GCONF) or '#000000,#000000',
            'machine_sn': _read_XO_value(_XO_SERIAL_PATH) or '',
            'machine_uuid': _read_XO_value(_XO_UUID_PATH) or '',
            'pubkey': util.pubkey(key_path()),
            }


def _read_XO_value(paths):
    for value_path in paths:
        if exists(value_path):
            with file(value_path) as f:
                return f.read().rstrip('\x00\n')
