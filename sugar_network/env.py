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

from gettext import gettext as _

from sugar_network import util


api_url = util.Option(
        _('url to connect to Sugar Network server API'),
        default='https://api.network.sugarlabs.org')

certfile = util.Option(
        _('path to SSL certificate file to connect to server via HTTPS'))

no_check_certificate = util.Option(
        _('do not check the server certificate against the available ' \
                'certificate authorities'),
        default=False, type_cast=util.Option.bool_cast, action='store_true')
