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

import logging
from os.path import join

from sugar_network import db, resources, static
from sugar_network.resources.volume import Resource


_logger = logging.getLogger('resources.context')


class Context(Resource):

    @db.indexed_property(prefix='T', full_text=True,
            typecast=[resources.CONTEXT_TYPES])
    def type(self, value):
        return value

    @type.setter
    def type(self, value):
        if 'package' in value and 'common' not in self['layer']:
            self['layer'] = tuple(self['layer']) + ('common',)
        return value

    @db.indexed_property(prefix='M',
            full_text=True, default=[], typecast=[])
    def implement(self, value):
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
            return db.PropertyMetadata(
                    url='/static/images/package.png',
                    blob=join(static.PATH, 'images', 'package.png'),
                    mime_type='image/png')
        else:
            return db.PropertyMetadata(
                    url='/static/images/missing.png',
                    blob=join(static.PATH, 'images', 'missing.png'),
                    mime_type='image/png')

    @db.blob_property(mime_type='image/svg+xml')
    def artifact_icon(self, value):
        if value:
            return value
        return db.PropertyMetadata(
                url='/static/images/missing.svg',
                blob=join(static.PATH, 'images', 'missing.svg'),
                mime_type='image/svg+xml')

    @db.blob_property(mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return db.PropertyMetadata(
                url='/static/images/missing.png',
                blob=join(static.PATH, 'images', 'missing.png'),
                mime_type='image/png')

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

    @db.indexed_property(prefix='K', typecast=bool, default=False,
            permissions=db.ACCESS_READ | db.ACCESS_LOCAL)
    def favorite(self, value):
        return value

    @db.indexed_property(prefix='L', typecast=[0, 1, 2], default=0,
            permissions=db.ACCESS_READ | db.ACCESS_LOCAL)
    def clone(self, value):
        return value

    @db.stored_property(typecast=[int], default=(-1, -1),
            permissions=db.ACCESS_PUBLIC | db.ACCESS_LOCAL)
    def position(self, value):
        return value

    @db.stored_property(typecast=[], default=[],
            permissions=db.ACCESS_PUBLIC | db.ACCESS_LOCAL)
    def dependencies(self, value):
        """Software dependencies.

        This is a transition method how to improve dependencies handling.
        The regular way should be setting up them in activity.info instead.

        """
        return value

    @db.stored_property(typecast=dict, default={},
            permissions=db.ACCESS_PUBLIC | db.ACCESS_LOCAL)
    def aliases(self, value):
        return value

    @db.stored_property(typecast=dict, default={},
            permissions=db.ACCESS_PUBLIC | db.ACCESS_LOCAL | db.ACCESS_SYSTEM)
    def packages(self, value):
        return value
