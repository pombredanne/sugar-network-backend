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

from sugar_network import db, model, static
from sugar_network.toolkit.router import Blob, ACL


class Post(db.Resource):

    @db.indexed_property(prefix='C',
            acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='A', default='',
            acl=ACL.CREATE | ACL.READ)
    def topic(self, value):
        return value

    @topic.setter
    def topic(self, value):
        if value and not self['context']:
            post = self.volume['post'].get(value)
            self['context'] = post['context']
        return value

    @db.indexed_property(prefix='T', typecast=model.POST_TYPES)
    def type(self, value):
        return value

    @db.indexed_property(slot=1, prefix='N', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def title(self, value):
        return value

    @db.indexed_property(prefix='M', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value

    @db.indexed_property(prefix='R', default='')
    def solution(self, value):
        return value

    @db.indexed_property(prefix='V', typecast=model.RATINGS, default=0,
            acl=ACL.CREATE | ACL.READ)
    def vote(self, value):
        return value

    @db.indexed_property(prefix='D', typecast=db.AggregatedType,
            full_text=True, default=db.AggregatedType(),
            fmt=lambda x: [i.get('message') for i in x.values()],
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE)
    def comments(self, value):
        return value

    @db.blob_property(mime_type='image/png')
    def preview(self, value):
        if value:
            return value
        return Blob({
            'url': '/static/images/missing-preview.png',
            'blob': static.path('images', 'missing-preview.png'),
            'mime_type': 'image/png',
            })

    @db.blob_property()
    def data(self, value):
        if value:
            value['name'] = self['title']
        return value

    @db.indexed_property(slot=2, default=0, acl=ACL.READ | ACL.CALC)
    def downloads(self, value):
        return value

    @db.indexed_property(slot=3, typecast=[], default=[0, 0],
            sortable_serialise=lambda x: float(x[1]) / x[0] if x[0] else 0,
            acl=ACL.READ | ACL.CALC)
    def rating(self, value):
        return value
