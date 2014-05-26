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

from sugar_network.toolkit import http, Option


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


api = Option(
        'url to connect to Sugar Network node API',
        default='http://node-devel.sugarlabs.org', short_option='-a',
        name='api')

certfile = Option(
        'path to SSL certificate file to connect to node via HTTPS')

no_check_certificate = Option(
        'do not check the node certificate against the available '
        'certificate authorities',
        default=False, type_cast=Option.bool_cast, action='store_true')

local_root = Option(
        'path to the directory to keep all local data',
        default=profile_path('network'), name='local_root')

node_mode = Option(
        'start node to share local documents',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='node-mode')

delayed_start = Option(
        'immediate start only database and the rest on getting '
        'notification from IPC client',
        default=False, type_cast=Option.bool_cast, action='store_true')

mounts_root = Option(
        'path to a directory with remote devices mounts',
        default='/media')

ipc_port = Option(
        'port number to listen for incomming connections from IPC clients',
        default=5001, type_cast=int, name='ipc_port')

hub_root = Option(
        'path to Contributor Hub site directory to serve from /hub location '
        'for IPC clients to workaround lack of CORS for SSE while using Hub '
        'from file:// url',
        default='/usr/share/sugar-network/hub')

discover_node = Option(
        'discover nodes in local network instead of using --api',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='discover-node')

cache_limit = Option(
        'the minimal disk free space, in bytes, to preserve while recycling '
        'disk cache; the final limit will be a minimal between --cache-limit '
        'and --cache-limit-percent',
        default=10, type_cast=int, name='cache-limit')

cache_limit_percent = Option(
        'the minimal disk free space, in percentage terms, to preserve while '
        'recycling disk cache; the final limit will be a minimal between '
        '--cache-limit and --cache-limit-percent',
        default=1024 * 1024 * 10, type_cast=int, name='cache-limit-percent')

cache_lifetime = Option(
        'the number of days to keep unused objects on disk cache '
        'before recycling',
        default=7, type_cast=int, name='cache-lifetime')

cache_timeout = Option(
        'check disk cache for recycling in specified delay in seconds',
        default=3600, type_cast=int, name='cache-timeout')

login = Option(
        'Sugar Labs account to connect to Sugar Network API node; '
        'should be set only if either password is provided or public key '
        'for Sugar Labs account was uploaded to the Sugar Network',
        name='login', short_option='-l')

password = Option(
        'Sugar Labs account password to connect to Sugar Network API node '
        'using Basic authentication; if omitted, keys based authentication '
        'will be used',
        name='password', short_option='-p')

keyfile = Option(
        'path to RSA private key to connect to Sugar Network API node',
        name='keyfile', short_option='-k', default='~/.ssh/sugar-network')

api_version = Option(
        'API version to interact with a Sugar Network node',
        name='api-version')


_logger = logging.getLogger('client')


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


def stability(context):
    value = Option.get('stabilities', context) or \
            Option.get('stabilities', 'default') or \
            'stable'
    return value.split()


def Connection(url=None, creds=None, **kwargs):
    if url is None:
        url = api.value
    if creds is None and keyfile.value:
        from sugar_network.client.auth import SugarCreds
        creds = SugarCreds(keyfile.value)
    return http.Connection(url,
            auth_request={'method': 'GET', 'params': {'cmd': 'logon'}},
            creds=creds, verify=not no_check_certificate.value,
            api_version=api_version.value, **kwargs)


def IPCConnection():
    return http.Connection(
            'http://127.0.0.1:%s' % ipc_port.value,
            # Online ipc->client->node request might fail if node connection
            # is lost in client process, so, re-send ipc request immediately
            # to retrive data from client in offline mode without propagating
            # errors on ipc side
            max_retries=1,
            # No need in proxy settings to connect to localhost
            trust_env=False,
            )
