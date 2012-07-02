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

# pylint: disable-msg=E1101,E0102,E0202

import active_document as ad
from sweets_recipe import GOOD_LICENSES

from sugar_network import node
from sugar_network.resources.resource import Resource


class Implementation(Resource):

    @ad.active_property(prefix='C',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def context(self, value):
        return value

    @ad.active_property(prefix='L', full_text=True, typecast=[GOOD_LICENSES],
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def license(self, value):
        return value

    @ad.active_property(slot=2, prefix='V',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def version(self, value):
        return value

    @ad.active_property(slot=3, prefix='D',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ, typecast=int)
    def date(self, value):
        return value

    @ad.active_property(slot=4, prefix='S', full_text=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ,
            typecast=node.STABILITIES)
    def stability(self, value):
        return value

    @ad.active_property(prefix='N', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def notes(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def data(self, stat):
        return stat
