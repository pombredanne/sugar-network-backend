# Copyright (C) 2012-2014 Aleksey Lim
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
from sugar_network.toolkit.coroutine import this


class User(db.Resource):

    @db.indexed_property(slot=1, prefix='N', full_text=True)
    def name(self, value):
        return value

    @db.indexed_property(prefix='P', full_text=True, default='')
    def location(self, value):
        return value

    @db.indexed_property(db.Numeric, slot=2, prefix='B', default=0)
    def birthday(self, value):
        return value

    @db.stored_property(default='')
    def email(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='assets/missing-avatar.png')
    def avatar(self, value):
        if not value.is_blob and self['email'] and hasattr(this, 'avatars'):
            value = this.avatars.get(self['email'], value)
        return value

    @db.stored_property()
    def pubkey(self, value):
        return value
