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

from sugar_network import node
from sugar_network.resources.resource import Resource


class Comment(Resource):

    LAYOUT_VERSION = 2

    @ad.active_property(prefix='T',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ,
            typecast=node.COMMENT_PARENTS)
    def parent_resource(self, value):
        return value

    @ad.active_property(prefix='P',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def parent(self, value):
        return value

    @ad.active_property(prefix='M', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def message(self, value):
        return value
