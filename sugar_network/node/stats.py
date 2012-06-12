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


stats = optparse.Option(
        _('enable stats collecting'),
        default=False, type_cast=optparse.Option.bool_cast,
        action='store_true')

stats_root = optparse.Option(
        _('path to the root directory for placing stats'),
        default='/var/lib/sugar-network/stats')

stats_step = optparse.Option(
        _('step interval in seconds for RRD databases'),
        default=60, type_cast=int)

stats_server_rras = optparse.Option(
        _('space separated list of RRAs for RRD databases on a server side'),
        default='RRA:AVERAGE:0.5:1:4320 RRA:AVERAGE:0.5:5:2016',
        type_cast=lambda x: [i for i in x.split() if i],
        type_repr=lambda x: ' '.join(x))

stats_client_rras = optparse.Option(
        _('space separated list of RRAs for RRD databases on client side'),
        default='RRA:AVERAGE:0.5:1:4320 RRA:AVERAGE:0.5:5:2016',
        type_cast=lambda x: [i for i in x.split() if i],
        type_repr=lambda x: ' '.join(x))