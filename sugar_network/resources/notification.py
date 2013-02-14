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

from sugar_network import db, resources
from sugar_network.resources.volume import Resource


class Notification(Resource):

    @db.indexed_property(prefix='T',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ,
            typecast=resources.NOTIFICATION_TYPES)
    def type(self, value):
        return value

    @db.indexed_property(prefix='K',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ,
            default='', typecast=resources.NOTIFICATION_OBJECT_TYPES)
    def resource(self, value):
        return value

    @db.indexed_property(prefix='O',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ, default='')
    def object(self, value):
        return value

    @db.indexed_property(prefix='D',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ, default='')
    def to(self, value):
        return value

    @db.indexed_property(prefix='M', full_text=True, localized=True,
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def message(self, value):
        return value
