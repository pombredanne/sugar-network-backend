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

from sugar_network import db, model, static
from sugar_network.toolkit.router import Blob, ACL


class Artifact(db.Resource):

    @db.indexed_property(prefix='C',
            acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='T', typecast=[model.ARTIFACT_TYPES])
    def type(self, value):
        return value

    @db.indexed_property(slot=1, prefix='S', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def title(self, value):
        return value

    @db.indexed_property(prefix='D', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def description(self, value):
        return value

    @db.indexed_property(slot=3, typecast=model.RATINGS, default=0,
            acl=ACL.READ | ACL.CALC)
    def rating(self, value):
        return value

    @db.stored_property(typecast=[], default=[0, 0],
            acl=ACL.READ | ACL.CALC)
    def reviews(self, value):
        if value is None:
            return 0
        else:
            return value[0]

    @db.blob_property(mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return Blob({
            'url': '/static/images/missing.png',
            'blob': join(static.PATH, 'images', 'missing.png'),
            'mime_type': 'image/png',
            })

    @db.blob_property()
    def data(self, value):
        if value:
            value['name'] = self['title']
        return value
