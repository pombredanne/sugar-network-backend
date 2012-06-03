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
from sugar_network_server.resources.context import Context as _Context


class Context(_Context):

    LOCAL_PROPS = {
            'keep': False,
            'keep_impl': 0,
            'position': (-1, -1),
            }

    @ad.active_property(prefix='LK', typecast=bool,
            default=LOCAL_PROPS['keep'])
    def keep(self, value):
        return value

    @ad.active_property(prefix='LI', typecast=[0, 1, 2],
            default=LOCAL_PROPS['keep_impl'])
    def keep_impl(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=[int],
            default=LOCAL_PROPS['position'])
    def position(self, value):
        return value
