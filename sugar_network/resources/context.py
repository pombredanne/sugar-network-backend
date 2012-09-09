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


class Context(Resource):

    @ad.active_property(prefix='T', full_text=True,
            typecast=[resources.CONTEXT_TYPES])
    def type(self, value):
        return value

    @ad.active_property(slot=1, prefix='N', full_text=True,
            permissions=ad.ACCESS_READ, default='')
    def name(self, value):
        return value

    @ad.active_property(prefix='M',
            full_text=True, default=[], typecast=[])
    def implement(self, value):
        return value

    @ad.active_property(prefix='S', full_text=True, localized=True)
    def title(self, value):
        return value

    @ad.active_property(prefix='R', full_text=True, localized=True)
    def summary(self, value):
        return value

    @ad.active_property(prefix='D', full_text=True, localized=True)
    def description(self, value):
        return value

    @ad.active_property(prefix='H', default='', full_text=True)
    def homepage(self, value):
        return value

    @ad.active_property(prefix='Y', default=[], typecast=[], full_text=True)
    def mime_types(self, value):
        return value

    @ad.active_property(ad.BlobProperty, mime_type='image/png')
    def icon(self, value):
        if value is None:
            if 'package' in self['type']:
                return {'url': '/static/images/package.png',
                        'path': join(static.PATH, 'images', 'package.png'),
                        'mime_type': 'image/png'}
            else:
                return {'url': '/static/images/missing.png',
                        'path': join(static.PATH, 'images', 'missing.png'),
                        'mime_type': 'image/png'}
        else:
            return value

    @ad.active_property(ad.BlobProperty, mime_type='image/svg+xml')
    def artifact_icon(self, value):
        if value is None:
            return {'url': '/static/images/missing.svg',
                    'path': join(static.PATH, 'images', 'missing.svg'),
                    'mime_type': 'image/svg+xml'}
        return value

    @ad.active_property(ad.BlobProperty)
    def preview(self, value):
        return value

    @ad.active_property(ad.BlobProperty,
            permissions=ad.ACCESS_READ, mime_type='application/json')
    def feed(self, value):
        return value

    @ad.active_property(prefix='K', typecast=bool, default=False)
    def keep(self, value):
        return value

    @ad.active_property(prefix='L', typecast=[0, 1, 2], default=0)
    def keep_impl(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=[int], default=(-1, -1))
    def position(self, value):
        return value
