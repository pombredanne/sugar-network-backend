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
from restful_document.router import Router
from restful_document.subscribe_socket import SubscribeSocket


only_sync_notification = optparse.Option(
        _('subscribers can be notified only with "sync" events; ' \
                'that is useful to minimize interactions between ' \
                'server and clients'),
        default=True, type_cast=optparse.Option.bool_cast,
        action='store_true')


class HTTPStatus(Exception):

    status = None
    headers = None
    result = None


class BadRequest(HTTPStatus):

    status = '400 Bad Request'
