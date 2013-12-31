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

import hashlib
from cStringIO import StringIO

from sugar_network import db, model, static, toolkit
from sugar_network.toolkit.router import Blob, ACL


class Context(db.Resource):

    @db.indexed_property(prefix='T', full_text=True,
            typecast=[model.CONTEXT_TYPES])
    def type(self, value):
        return value

    @type.setter
    def type(self, value):
        if value and 'package' in value and 'common' not in self['layer']:
            self['layer'] = tuple(self['layer']) + ('common',)
        if 'artifact_icon' not in self:
            for name in ('activity', 'book', 'group'):
                if name not in self.type:
                    continue
                with file(static.path('images', name + '.svg')) as f:
                    Context.populate_images(self, f.read())
                break
        return value

    @db.indexed_property(slot=1, prefix='S', full_text=True, localized=True)
    def title(self, value):
        return value

    @db.indexed_property(prefix='R', full_text=True, localized=True)
    def summary(self, value):
        return value

    @db.indexed_property(prefix='D', full_text=True, localized=True)
    def description(self, value):
        return value

    @db.indexed_property(prefix='H', default='', full_text=True)
    def homepage(self, value):
        return value

    @db.indexed_property(prefix='Y', default=[], typecast=[], full_text=True)
    def mime_types(self, value):
        return value

    @db.blob_property(mime_type='image/png')
    def icon(self, value):
        if value:
            return value
        if 'package' in self['type']:
            return Blob({
                'url': '/static/images/package.png',
                'blob': static.path('images', 'package.png'),
                'mime_type': 'image/png',
                })
        else:
            return Blob({
                'url': '/static/images/missing.png',
                'blob': static.path('images', 'missing.png'),
                'mime_type': 'image/png',
                })

    @db.blob_property(mime_type='image/svg+xml')
    def artifact_icon(self, value):
        if value:
            return value
        if 'package' in self['type']:
            return Blob({
                'url': '/static/images/package.svg',
                'blob': static.path('images', 'package.svg'),
                'mime_type': 'image/png',
                })
        else:
            return Blob({
                'url': '/static/images/missing.svg',
                'blob': static.path('images', 'missing.svg'),
                'mime_type': 'image/svg+xml',
                })

    @db.blob_property(mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        if 'package' in self['type']:
            return Blob({
                'url': '/static/images/package-preview.png',
                'blob': static.path('images', 'package-preview.png'),
                'mime_type': 'image/png',
                })
        else:
            return Blob({
                'url': '/static/images/missing-preview.png',
                'blob': static.path('images', 'missing-preview.png'),
                'mime_type': 'image/png',
                })

    @db.indexed_property(slot=3, default=0, acl=ACL.READ | ACL.CALC)
    def downloads(self, value):
        return value

    @db.indexed_property(slot=4, typecast=model.RATINGS, default=0,
            acl=ACL.READ | ACL.CALC)
    def rating(self, value):
        return value

    @db.stored_property(typecast=[], default=[0, 0], acl=ACL.READ | ACL.CALC)
    def reviews(self, value):
        if value is None:
            return 0
        else:
            return value[0]

    @reviews.setter
    def reviews(self, value):
        if isinstance(value, int):
            return [value, 0]
        else:
            return value

    @db.stored_property(typecast=[], default=[], acl=ACL.PUBLIC | ACL.LOCAL)
    def dependencies(self, value):
        """Software dependencies.

        This is a transition method how to improve dependencies handling.
        The regular way should be setting up them in activity.info instead.

        """
        return value

    @db.stored_property(typecast=dict, default={},
            acl=ACL.PUBLIC | ACL.LOCAL)
    def aliases(self, value):
        return value

    @db.stored_property(typecast=dict, default={}, acl=ACL.PUBLIC | ACL.LOCAL)
    def packages(self, value):
        return value

    @staticmethod
    def populate_images(props, svg):
        if 'guid' in props:
            from sugar_network.toolkit.sugar import color_svg
            svg = color_svg(svg, props['guid'])

        def convert(w, h):
            png = toolkit.svg_to_png(svg, w, h)
            return {'blob': png,
                    'mime_type': 'image/png',
                    'digest': hashlib.sha1(png.getvalue()).hexdigest(),
                    }

        props['artifact_icon'] = {
                'blob': StringIO(svg),
                'mime_type': 'image/svg+xml',
                'digest': hashlib.sha1(svg).hexdigest(),
                }
        props['icon'] = convert(55, 55)
        props['preview'] = convert(140, 140)
