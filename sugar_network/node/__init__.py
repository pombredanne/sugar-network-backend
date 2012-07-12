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
        'hostname to listen incomming connections',
        default='0.0.0.0', name='host')

port = Option(
        'port number to listen incomming connections',
        default=8000, type_cast=int, name='port')

subscribe_port = Option(
        'port number to listen incomming subscribtion requests',
        default=8001, type_cast=int, name='subscribe_port')

keyfile = Option(
        'path to SSL certificate keyfile to serve requests via HTTPS',
        name='keyfile')

certfile = Option(
        'path to SSL certificate file to serve requests via HTTPS',
        name='certfile')

trust_users = Option(
        'switch off user credentials check; disabling this option will ' \
                'require OpenSSH-5.6 or later',
        default=False, type_cast=Option.bool_cast,
        action='store_true', name='trust_users')

data_root = Option(
        'path to the root directory for placing documents\' ' \
                'data and indexes',
        default='/var/lib/sugar-network/db', name='data_root')

only_sync_notification = Option(
        'subscribers can be notified only with "sync" events; ' \
                'that is useful to minimize interactions between ' \
                'server and clients',
        default=False, type_cast=Option.bool_cast, action='store_true')

find_limit = Option(
        'limit the resulting list for search requests',
        default=32, type_cast=int)

tmpdir = Option(
        'if specified, use this directory for temporary files, such files ' \
                'might take hunder of megabytes while node synchronizing')


class HTTPStatus(Exception):

    status = None
    headers = None
    result = None


class BadRequest(HTTPStatus):

    status = '400 Bad Request'


class Unauthorized(HTTPStatus):

    status = '401 Unauthorized'
    headers = {'WWW-Authenticate': 'Sugar'}
