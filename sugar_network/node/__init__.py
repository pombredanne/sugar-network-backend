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

from active_toolkit.options import Option


host = Option(
        'hostname to listen for incomming connections and '
        'using for publicly visible urls',
        default='localhost', name='host')

port = Option(
        'port number to listen incomming connections',
        default=8000, type_cast=int, name='port')

keyfile = Option(
        'path to SSL certificate keyfile to serve requests via HTTPS',
        name='keyfile')

certfile = Option(
        'path to SSL certificate file to serve requests via HTTPS',
        name='certfile')

trust_users = Option(
        'switch off user credentials check; disabling this option will '
        'require OpenSSH-5.6 or later',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='trust_users')

data_root = Option(
        'path to a directory to place server data',
        default='/var/lib/sugar-network', name='data_root')

find_limit = Option(
        'limit the resulting list for search requests',
        default=32, type_cast=int)

tmpdir = Option(
        'if specified, use this directory for temporary files, such files '
                'might take hunder of megabytes while node synchronizing')

sync_dirs = Option(
        'colon separated list of paths to directories to synchronize with '
                'master server',
        type_cast=Option.paths_cast, type_repr=Option.paths_repr,
        name='sync-dirs')

pull_timeout = Option(
        'delay in seconds to return to sync-pull requester to wait until '
        'pull request will be ready',
        default=30, type_cast=int)
