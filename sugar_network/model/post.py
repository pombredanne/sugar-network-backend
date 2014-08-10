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
from sugar_network.toolkit import http, enforce


class Post(db.Resource):

    @db.indexed_property(db.Reference, prefix='A', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(db.Reference, prefix='B', default='',
            acl=ACL.CREATE | ACL.READ)
    def topic(self, value):
        return value

    @topic.setter
    def topic(self, value):
        self._update_replies(value, +1)
        return value

    @db.indexed_property(db.Enum, prefix='C', items=model.POST_TYPES.keys(),
            acl=ACL.CREATE | ACL.READ)
    def type(self, value):
        return value

    @db.indexed_property(db.Localized, slot=1, prefix='D', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def title(self, value):
        return value

    @db.indexed_property(db.Localized, prefix='E', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value

    @db.indexed_property(prefix='F', default='', acl=ACL.CREATE | ACL.READ)
    def resolution(self, value):
        return value

    @db.indexed_property(db.Enum, prefix='G', items=range(6), default=0,
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
            acl=ACL.READ | ACL.CREATE | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
    def attachments(self, value):
        return value

    @db.indexed_property(model.Rating, slot=2, acl=ACL.READ | ACL.LOCAL)
    def rating(self, value):
        return value

    @db.indexed_property(db.Numeric, slot=3, prefix='H', default=0,
            acl=ACL.READ | ACL.LOCAL)
    def replies(self, value):
        return value

    def routed_creating(self):
        context = this.volume['context'][self['context']]
        enforce(context.available, http.BadRequest, 'Context does not exist')
        allowed_contexts = model.POST_TYPES[self['type']]
        enforce(allowed_contexts is None or
                allowed_contexts & set(context['type']),
                http.BadRequest, 'Inappropriate type')
        enforce((self['type'] == 'post') == bool(self['topic']),
                http.BadRequest, 'Inappropriate relation')
        if self['resolution']:
            enforce(self['topic'], http.BadRequest,
                    'Inappropriate resolution')
            topic = this.volume['post'][self['topic']]
            enforce(topic.available, http.NotFound, 'Topic not found')
            allowed_topic = model.POST_RESOLUTIONS.get(self['resolution'])
            enforce(allowed_topic == topic['type'], http.BadRequest,
                    'Inappropriate resolution')
            if not this.principal.cap_author_override:
                if topic['type'] == 'issue':
                    author = this.volume['context'][topic['context']]['author']
                    message = 'Context authors only'
                else:
                    author = topic['author']
                    message = 'Topic authors only'
                enforce(this.principal in author, http.Forbidden, message)
        else:
            self.posts['resolution'] = \
                    model.POST_RESOLUTION_DEFAULTS.get(self['type']) or ''
        db.Resource.routed_creating(self)

    def routed_created(self):
        db.Resource.routed_created(self)
        if self['resolution'] and self['topic']:
            this.volume['post'].update(self['topic'], {
                'resolution': self['resolution'],
                })

    def deleted(self):
        if self['topic']:
            self._update_replies(self['topic'], -1)
        if self['vote']:
            self._update_rating(self['vote'], -1)

    @staticmethod
    def recalc():

        def calc_rating(**kwargs):
            rating = [0, 0]
            alldocs, __ = this.volume['post'].find(
                    not_state='deleted', not_vote=0, **kwargs)
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
            __, replies = this.volume['post'].find(
                    not_state='deleted', topic=topic.guid, limit=0)
            this.volume['post'].update(topic.guid, {
                'rating': rating,
                'replies': replies,
                })

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

    def _update_replies(self, topic, shift):
        orig = this.volume['post'][topic]
        if orig.exists:
            this.volume['post'].update(topic, {
                'replies': orig['replies'] + shift,
                })
