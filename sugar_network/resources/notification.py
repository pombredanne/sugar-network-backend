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

import active_document as ad

from sugar_network import resources
from sugar_network.resources.volume import Resource


class Notification(Resource):

    @ad.active_property(slot=1, prefix='T',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ,
            typecast=resources.NOTIFICATION_TYPES)
    def type(self, value):
        return value

    @ad.active_property(slot=2, prefix='K',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ,
            default='', typecast=resources.NOTIFICATION_OBJECT_TYPES)
    def resource(self, value):
        return value

    @ad.active_property(slot=3, prefix='O',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ, default='')
    def object(self, value):
        return value

    @ad.active_property(slot=4, prefix='D',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ, default='')
    def to(self, value):
        return value

    @ad.active_property(prefix='M', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def message(self, value):
        return value
