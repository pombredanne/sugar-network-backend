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


class Feedback(Resource):

    @db.indexed_property(prefix='C',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='T', typecast=[resources.FEEDBACK_TYPES])
    def type(self, value):
        return value

    @db.indexed_property(prefix='S', full_text=True, localized=True)
    def title(self, value):
        return value

    @db.indexed_property(prefix='N', full_text=True, localized=True)
    def content(self, value):
        return value

    @db.indexed_property(prefix='A', default='')
    def solution(self, value):
        return value
