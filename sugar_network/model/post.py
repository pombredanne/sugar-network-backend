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
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this


class Post(db.Resource):

    @db.indexed_property(prefix='C', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='A', default='', acl=ACL.CREATE | ACL.READ)
    def topic(self, value):
        return value

    @db.indexed_property(db.Enum, prefix='T', items=model.POST_TYPES)
    def type(self, value):
        return value

    @db.indexed_property(db.Localized, slot=1, prefix='N', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def title(self, value):
        return value

    @db.indexed_property(db.Localized, prefix='M', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value

    @db.indexed_property(prefix='R', default='')
    def solution(self, value):
        return value

    @db.indexed_property(db.Enum, prefix='V', items=range(5), default=0,
            acl=ACL.CREATE | ACL.READ)
    def vote(self, value):
        return value

    @vote.setter
    def vote(self, value):
        if value:
            if self['topic']:
                resource = this.volume['post']
                guid = self['topic']
            else:
                resource = this.volume['context']
                guid = self['context']
            orig = resource[guid]['rating']
            resource.update(guid, {'rating': [orig[0] + 1, orig[1] + value]})
        return value

    @db.indexed_property(db.Aggregated, prefix='D', full_text=True,
            subtype=db.Localized())
    def comments(self, value):
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='missing-logo.png')
    def preview(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob())
    def attachments(self, value):
        if value:
            value['name'] = self['title']
        return value

    @db.indexed_property(db.Numeric, slot=2, default=0,
            acl=ACL.READ | ACL.CALC)
    def downloads(self, value):
        return value

    @db.indexed_property(model.Rating, slot=3, acl=ACL.READ | ACL.CALC)
    def rating(self, value):
        return value
