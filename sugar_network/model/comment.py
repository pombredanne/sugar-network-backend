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

from sugar_network import db
from sugar_network.toolkit.router import ACL


class Comment(db.Resource):

    @db.indexed_property(prefix='C',
            acl=ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='R', default='',
            acl=ACL.CREATE | ACL.READ)
    def review(self, value):
        return value

    @review.setter
    def review(self, value):
        if value:
            review = self.volume['review'].get(value)
            self['context'] = review['context']
        return value

    @db.indexed_property(prefix='F', default='',
            acl=ACL.CREATE | ACL.READ)
    def feedback(self, value):
        return value

    @feedback.setter
    def feedback(self, value):
        if value:
            feedback = self.volume['feedback'].get(value)
            self['context'] = feedback['context']
        return value

    @db.indexed_property(prefix='S', default='',
            acl=ACL.CREATE | ACL.READ)
    def solution(self, value):
        return value

    @solution.setter
    def solution(self, value):
        if value:
            solution = self.volume['solution'].get(value)
            self['context'] = solution['context']
        return value

    @db.indexed_property(prefix='M', full_text=True, localized=True,
            acl=ACL.CREATE | ACL.READ)
    def message(self, value):
        return value
