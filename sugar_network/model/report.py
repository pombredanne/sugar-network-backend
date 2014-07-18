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


class _Solution(db.Property):

    def __init__(self, **kwargs):
        db.Property.__init__(self, default=[], **kwargs)

    def typecast(self, value):
        return [] if value is None else list(value)

    def encode(self, value):
        for i in value:
            yield i[0]


class Report(db.Resource):

    one_way = True

    @db.indexed_property(db.Reference, prefix='C', acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='V', default='', acl=ACL.CREATE | ACL.READ)
    def version(self, value):
        return value

    @db.indexed_property(prefix='E', full_text=True, acl=ACL.CREATE | ACL.READ)
    def error(self, value):
        return value

    @db.indexed_property(prefix='U', full_text=True, acl=ACL.CREATE | ACL.READ)
    def uname(self, value):
        return value

    @db.indexed_property(db.Dict, prefix='L', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def lsb_release(self, value):
        return value

    @db.indexed_property(_Solution, prefix='S', full_text=True,
            acl=ACL.CREATE | ACL.READ)
    def solution(self, value):
        return value

    @db.stored_property(db.Aggregated, subtype=db.Blob(),
            acl=ACL.READ | ACL.CREATE | ACL.INSERT | ACL.AUTHOR)
    def logs(self, value):
        return value
