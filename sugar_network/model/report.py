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

from sugar_network import db
from sugar_network.toolkit.router import ACL


class Report(db.Resource):

    @db.indexed_property(prefix='C', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='V', default='', acl=ACL.CREATE | ACL.READ)
    def implementation(self, value):
        return value

    @implementation.setter
    def implementation(self, value):
        if value and 'version' not in self.props and 'implementation' in value:
            version = self.volume['implementation'].get(value)
            self['version'] = version['version']
        return value

    @db.stored_property(default='', acl=ACL.CREATE | ACL.READ)
    def version(self, value):
        return value

    @db.stored_property(typecast=dict, default={}, acl=ACL.CREATE | ACL.READ)
    def environ(self, value):
        return value

    @db.indexed_property(prefix='T', acl=ACL.CREATE | ACL.READ)
    def error(self, value):
        return value

    @db.blob_property()
    def data(self, value):
        return value
