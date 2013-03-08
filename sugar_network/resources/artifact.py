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

from os.path import join

from sugar_network import db, resources, static
from sugar_network.resources.volume import Resource


class Artifact(Resource):

    @db.indexed_property(prefix='C',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='T', typecast=[resources.ARTIFACT_TYPES])
    def type(self, value):
        return value

    @db.indexed_property(slot=1, prefix='S', full_text=True, localized=True,
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def title(self, value):
        return value

    @db.indexed_property(prefix='D', full_text=True, localized=True,
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def description(self, value):
        return value

    @db.indexed_property(slot=3, typecast=resources.RATINGS, default=0,
            permissions=db.ACCESS_READ | db.ACCESS_CALC)
    def rating(self, value):
        return value

    @db.stored_property(typecast=[], default=[0, 0],
            permissions=db.ACCESS_READ | db.ACCESS_CALC)
    def reviews(self, value):
        if value is None:
            return 0
        else:
            return value[0]

    @db.blob_property(mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return db.PropertyMetadata(
                url='/static/images/missing.png',
                blob=join(static.PATH, 'images', 'missing.png'),
                mime_type='image/png')

    @db.blob_property()
    def data(self, value):
        if value:
            value['name'] = self['title']
        return value

    @db.indexed_property(prefix='K', typecast=bool, default=False,
            permissions=db.ACCESS_READ | db.ACCESS_LOCAL)
    def favorite(self, value):
        return value

    @db.indexed_property(prefix='L', typecast=[0, 1, 2], default=0,
            permissions=db.ACCESS_READ | db.ACCESS_LOCAL)
    def clone(self, value):
        return value
