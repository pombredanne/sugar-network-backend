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

from gettext import gettext as _

from active_toolkit import optparse


CONTEXT_TYPES = ['application', 'library', 'activity', 'article']
COMMENT_PARENTS = ['feedback', 'solution']
NOTIFICATION_TYPES = ['create', 'update', 'delete', 'vote']
FEEDBACK_TYPES = ['question', 'idea', 'problem', 'review']

NOTIFICATION_OBJECT_TYPES = [
        '', 'content', 'feedback', 'solution', 'artifact', 'version', 'report',
        ]
STABILITIES = [
        'insecure', 'buggy', 'developer', 'testing', 'stable',
        ]


host = optparse.Option(
        _('hostname to listen incomming connections'),
        default='0.0.0.0', name='host')

port = optparse.Option(
        _('port number to listen incomming connections'),
        default=8000, type_cast=int, name='port')

subscribe_port = optparse.Option(
        _('port number to listen incomming subscribtion requests'),
        default=8001, type_cast=int, name='subscribe_port')

keyfile = optparse.Option(
        _('path to SSL certificate keyfile to serve requests via HTTPS'),
        name='keyfile')

certfile = optparse.Option(
        _('path to SSL certificate file to serve requests via HTTPS'),
        name='certfile')

trust_users = optparse.Option(
        _('switch off user credentials check; disabling this option will ' \
                'require OpenSSH-5.6 or later'),
        default=False, type_cast=optparse.Option.bool_cast,
        action='store_true', name='trust_users')

data_root = optparse.Option(
        _('path to the root directory for placing documents\' ' \
                'data and indexes'),
        default='/var/lib/sugar-network/db', name='data_root')

master_url = optparse.Option(
        _('master API url to share with nodes; ' \
                'if omitted, node is not a master'),
        name='master-url')

only_sync_notification = optparse.Option(
        _('subscribers can be notified only with "sync" events; ' \
                'that is useful to minimize interactions between ' \
                'server and clients'),
        default=False, type_cast=optparse.Option.bool_cast,
        action='store_true')

find_limit = optparse.Option(
        _('limit the resulting list for search requests'),
        default=32, type_cast=int)


class HTTPStatus(Exception):

    status = None
    headers = None
    result = None


class BadRequest(HTTPStatus):

    status = '400 Bad Request'


class Unauthorized(HTTPStatus):

    status = '401 Unauthorized'
    headers = {'WWW-Authenticate': 'Sugar'}
