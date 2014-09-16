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

from sugar_network import db
from sugar_network.toolkit.router import ACL


class _Solution(db.Dict):

    def __init__(self, **kwargs):
        db.Dict.__init__(self, db.Dict(), default={}, **kwargs)

    def encode(self, solution):
        for context, value in solution.items():
            yield context
            if 'version' in value:
                yield db.IndexableText('-'.join((context, value['version'])))
            if 'title' in value:
                yield db.IndexableText(value['title'])
            if 'packages' in value:
                for pkg in value['packages']:
                    yield pkg


class Report(db.Resource):

    one_way = True

    @db.indexed_property(db.Reference, prefix='A', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(_Solution, prefix='B', acl=ACL.CREATE | ACL.READ)
    def solution(self, value):
        return value

    @db.indexed_property(prefix='C', default='', acl=ACL.CREATE | ACL.READ)
    def version(self, value):
        return value

    @db.indexed_property(prefix='D', full_text=True, acl=ACL.CREATE | ACL.READ)
    def error(self, value):
        return value

    @db.indexed_property(prefix='E', full_text=True, acl=ACL.CREATE | ACL.READ)
    def uname(self, value):
        return value

    @db.indexed_property(prefix='F', acl=ACL.CREATE | ACL.READ)
    def lsb_release(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob(),
            acl=ACL.READ | ACL.CREATE | ACL.INSERT | ACL.AUTHOR)
    def logs(self, value):
        return value
