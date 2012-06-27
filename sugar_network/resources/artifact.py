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

from sugar_network.resources.resource import Resource


class Artifact(Resource):

    @ad.active_property(prefix='C',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def context(self, value):
        return value

    @ad.active_property(prefix='T', full_text=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def mime_type(self, value):
        return value

    @ad.active_property(prefix='S', full_text=True, localized=True)
    def title(self, value):
        return value

    @ad.active_property(prefix='D', full_text=True, localized=True)
    def description(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def preview(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def data(self, value):
        return value
