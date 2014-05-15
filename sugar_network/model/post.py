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
from sugar_network.toolkit import enforce


class Post(db.Resource):

    @db.indexed_property(db.Reference, prefix='C', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(db.Reference, prefix='A', default='',
            acl=ACL.CREATE | ACL.READ)
    def topic(self, value):
        return value

    @db.indexed_property(db.Enum, prefix='T', items=model.POST_TYPES,
            acl=ACL.CREATE | ACL.READ)
    def type(self, value):
        return value

    @type.setter
    def type(self, value):
        is_not_topic = value in ('post', 'solution')
        enforce(is_not_topic == bool(self['topic']), 'Inappropriate type')
        return value

    @db.indexed_property(db.Localized, slot=1, prefix='N', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def title(self, value):
        return value

    @db.indexed_property(db.Localized, prefix='M', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value

    @db.indexed_property(db.Reference, prefix='R', default='')
    def solution(self, value):
        return value

    @db.indexed_property(db.Enum, prefix='V', items=range(6), default=0,
            acl=ACL.CREATE | ACL.READ)
    def vote(self, value):
        return value

    @vote.setter
    def vote(self, value):
        if value:
            self._update_rating(value, +1)
        return value

    @db.stored_property(db.Blob, mime_type='image/png',
            default='assets/missing-logo.png')
    def preview(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
    def attachments(self, value):
        return value

    @db.indexed_property(model.Rating, slot=2, acl=ACL.READ | ACL.LOCAL)
    def rating(self, value):
        return value

    def updated(self):
        if self.posts.get('state') == 'deleted':
            self._update_rating(self['vote'], -1)
        db.Resource.updated(self)

    @staticmethod
    def rating_regen():

        def calc_rating(**kwargs):
            rating = [0, 0]
            alldocs, __ = this.volume['post'].find(not_vote=0, **kwargs)
            for post in alldocs:
                rating[0] += 1
                rating[1] += post['vote']
            return rating

        alldocs, __ = this.volume['context'].find()
        for context in alldocs:
            rating = calc_rating(topic='', context=context.guid)
            this.volume['context'].update(context.guid, {'rating': rating})

        alldocs, __ = this.volume['post'].find(topic='')
        for topic in alldocs:
            rating = calc_rating(topic=topic.guid)
            this.volume['post'].update(topic.guid, {'rating': rating})

    def _update_rating(self, vote, shift):
        if self['topic']:
            resource = this.volume['post']
            guid = self['topic']
        else:
            resource = this.volume['context']
            guid = self['context']
        orig = resource[guid]['rating']
        resource.update(guid, {
            'rating': [orig[0] + shift, orig[1] + shift * vote],
            })
