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

import xapian

from sugar_network import db, model
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.licenses import GOOD_LICENSES
from sugar_network.toolkit.spec import parse_version
from sugar_network.toolkit import http, enforce


class Implementation(db.Resource):

    @db.indexed_property(prefix='C',
            acl=ACL.CREATE | ACL.READ)
    def context(self, value):
        return value

    @context.setter
    def context(self, value):
        context = self.volume['context'].get(value)
        enforce(self.request.principal in context['author'], http.Forbidden,
                'Only Context authors can submit new Implementations')
        return value

    @db.indexed_property(prefix='L', full_text=True, typecast=[GOOD_LICENSES],
            acl=ACL.CREATE | ACL.READ)
    def license(self, value):
        return value

    @db.indexed_property(slot=1, prefix='V', fmt=lambda x: _fmt_version(x),
            acl=ACL.CREATE | ACL.READ)
    def version(self, value):
        return value

    @db.indexed_property(prefix='S', default='stabile',
            acl=ACL.CREATE | ACL.READ, typecast=model.STABILITIES)
    def stability(self, value):
        return value

    @db.indexed_property(prefix='N', full_text=True, localized=True,
            default='', acl=ACL.CREATE | ACL.READ)
    def notes(self, value):
        return value

    @db.blob_property()
    def data(self, value):
        return value


def _fmt_version(version):
    version = parse_version(version)
    # Convert to [(`version`, `modifier`)]
    version = zip(*([iter(version)] * 2))
    major, modifier = version.pop(0)

    result = sum([(rank % 10000) * pow(10000, 3 - i)
            for i, rank in enumerate((major + [0, 0])[:3])])
    result += (5 + modifier) * 1000
    if modifier and version:
        minor, __ = version.pop(0)
        if minor:
            result += (minor[0] % 1000)

    return xapian.sortable_serialise(result)
