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


class Context(db.Resource):

    @db.indexed_property(db.List, prefix='T', full_text=True,
            subtype=db.Enum(model.CONTEXT_TYPES))
    def type(self, value):
        return value

    @type.setter
    def type(self, value):
        if 'package' in value and 'common' not in self['layer']:
            self.post('layer', self['layer'] + ['common'])
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

    @db.indexed_property(db.List, prefix='Y', default=[], full_text=True)
    def mime_types(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png', default='missing.png')
    def icon(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/svg+xml',
            default='missing.svg')
    def artifact_icon(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='missing-logo.png')
    def logo(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob())
    def previews(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=model.Release(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
    def releases(self, value):
        return value

    @releases.setter
    def releases(self, value):
        if value or this.request.method != 'POST':
            self.invalidate_solutions()
        return value

    @db.indexed_property(db.Numeric, slot=2, default=0,
            acl=ACL.READ | ACL.CALC)
    def downloads(self, value):
        return value

    @db.indexed_property(model.Rating, slot=3, acl=ACL.READ | ACL.CALC)
    def rating(self, value):
        return value

    @db.stored_property(db.List, default=[], acl=ACL.PUBLIC | ACL.LOCAL)
    def dependencies(self, value):
        """Software dependencies.

        This is a transition method how to improve dependencies handling.
        The regular way should be setting up them in activity.info instead.

        """
        return value

    @dependencies.setter
    def dependencies(self, value):
        if value or this.request.method != 'POST':
            self.invalidate_solutions()
        return value

    def deleted(self):
        self.invalidate_solutions()

    def restored(self):
        self.invalidate_solutions()

    def invalidate_solutions(self):
        this.broadcast({
            'event': 'release',
            'seqno': this.volume.releases_seqno.next(),
            })
