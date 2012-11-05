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

from os.path import join

import active_document as ad
from sugar_network import resources, static
from sugar_network.resources.volume import Resource


class Artifact(Resource):

    @ad.active_property(prefix='C',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def context(self, value):
        return value

    @ad.active_property(slot=1, prefix='S', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def title(self, value):
        return value

    @ad.active_property(prefix='D', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def description(self, value):
        return value

    @ad.active_property(slot=2, typecast=int, default=0,
            permissions=ad.ACCESS_READ | ad.ACCESS_SYSTEM)
    def downloads(self, value):
        return value

    @ad.active_property(slot=3, typecast=resources.RATINGS, default=0,
            permissions=ad.ACCESS_READ | ad.ACCESS_SYSTEM)
    def rating(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=[], default=[0, 0],
            permissions=ad.ACCESS_READ | ad.ACCESS_SYSTEM)
    def reviews(self, value):
        if value is None:
            return 0
        else:
            return value[0]

    @ad.active_property(ad.BlobProperty, mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return ad.PropertyMeta(
                url='/static/images/missing.png',
                path=join(static.PATH, 'images', 'missing.png'),
                mime_type='image/png')

    @ad.active_property(ad.BlobProperty)
    def data(self, value):
        return value
