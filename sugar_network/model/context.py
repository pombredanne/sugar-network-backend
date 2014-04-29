# Copyright (C) 2012-2014 Aleksey Lim
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

from sugar_network import db, model
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit import svg_to_png


class Context(db.Resource):

    @db.indexed_property(db.List, prefix='T',
            subtype=db.Enum(model.CONTEXT_TYPES))
    def type(self, value):
        return value

    @type.setter
    def type(self, value):
        if 'package' in value:
            self.post('icon', 'assets/package.png')
            self.post('logo', 'assets/package-logo.png')
            self.post('artefact_icon', 'assets/package.svg')
        elif 'activity' not in value:
            if not self['artefact_icon']:
                self._generate_default_icons(value)
        return value

    @db.indexed_property(db.Localized, slot=1, prefix='S', full_text=True)
    def title(self, value):
        return value

    @db.indexed_property(db.Localized, prefix='R', full_text=True)
    def summary(self, value):
        return value

    @db.indexed_property(db.Localized, prefix='D', full_text=True)
    def description(self, value):
        return value

    @db.indexed_property(prefix='H', default='', full_text=True)
    def homepage(self, value):
        return value

    @db.indexed_property(db.List, prefix='Y', default=[])
    def mime_types(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='assets/missing.png')
    def icon(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/svg+xml',
            default='assets/missing.svg')
    def artefact_icon(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='assets/missing-logo.png')
    def logo(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
    def previews(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Dict(),
            acl=ACL.READ | ACL.LOCAL)
    def releases(self, value):
        return value

    @db.indexed_property(db.Numeric, slot=2, default=0,
            acl=ACL.READ | ACL.LOCAL)
    def downloads(self, value):
        return value

    @db.indexed_property(model.Rating, slot=3, acl=ACL.READ | ACL.LOCAL)
    def rating(self, value):
        return value

    @db.stored_property(default='', acl=ACL.PUBLIC)
    def dependencies(self, value):
        """Software dependencies.

        This is a transition method how to improve dependencies handling.
        The regular way should be setting up them in activity.info instead.

        """
        return value

    def _generate_default_icons(self, types):
        blobs = this.volume.blobs
        svg = None
        for type_ in ('activity', 'book', 'group'):
            if type_ in types:
                with file(blobs.get('assets/%s.svg' % type_).path) as f:
                    svg = f.read()
                from sugar_network.toolkit.sugar import color_svg
                svg = color_svg(svg, self['guid'])
                self.post('artefact_icon',
                        blobs.post(svg, 'image/svg+xml').digest)
                break
        else:
            return
        for prop, size in (
                ('icon', model.ICON_SIZE),
                ('logo', model.LOGO_SIZE),
                ):
            if self[prop]:
                continue
            png = blobs.post(svg_to_png(svg, size), 'image/png').digest
            self.post(prop, png)
