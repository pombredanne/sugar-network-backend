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


class User(db.Resource):

    @db.indexed_property(slot=1, prefix='N', full_text=True)
    def name(self, value):
        return value

    @db.indexed_property(prefix='P', full_text=True, default='')
    def location(self, value):
        return value

    @db.indexed_property(slot=2, prefix='B', default=0, typecast=int)
    def birthday(self, value):
        return value

    @db.blob_property(acl=ACL.CREATE, mime_type='text/plain')
    def pubkey(self, value):
        return value
