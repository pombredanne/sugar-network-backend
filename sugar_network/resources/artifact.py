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

from sugar_network.resources.volume import Resource


class Artifact(Resource):

    @ad.active_property(slot=1, prefix='C', default='')
    def context(self, value):
        return value

    @ad.active_property(slot=2, prefix='K', typecast=bool, default=False)
    def keep(self, value):
        return value

    @ad.active_property(slot=3, prefix='T', full_text=True, default='')
    def mime_type(self, value):
        return value

    @ad.active_property(slot=4, prefix='S', default='', full_text=True,
            localized=True)
    def title(self, value):
        return value

    @ad.active_property(slot=5, prefix='D', default='', full_text=True,
            localized=True)
    def description(self, value):
        return value

    @ad.active_property(slot=6, prefix='A', default='')
    def activity_id(self, value):
        return value

    @ad.active_property(slot=7, prefix='Z', typecast=int, default=0)
    def filesize(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=dict, default={})
    def traits(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def preview(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def data(self, value):
        return value
