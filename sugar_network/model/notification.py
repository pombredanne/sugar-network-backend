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

from sugar_network import db, model
from sugar_network.toolkit.router import ACL


class Notification(db.Resource):

    @db.indexed_property(prefix='T',
            acl=ACL.CREATE | ACL.READ,
            typecast=model.NOTIFICATION_TYPES)
    def type(self, value):
        return value

    @db.indexed_property(prefix='K',
            acl=ACL.CREATE | ACL.READ,
            default='', typecast=model.NOTIFICATION_OBJECT_TYPES)
    def resource(self, value):
        return value

    @db.indexed_property(prefix='O',
            acl=ACL.CREATE | ACL.READ, default='')
    def object(self, value):
        return value

    @db.indexed_property(prefix='D',
            acl=ACL.CREATE | ACL.READ, default='')
    def to(self, value):
        return value

    @db.indexed_property(prefix='M', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value
