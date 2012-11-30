# Copyright (C) 2012 Aleksey Lim
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

# pylint: disable-msg=E1101,E0102,E0202

import xapian

import active_document as ad
from sugar_network.zerosugar.licenses import GOOD_LICENSES
from sugar_network import resources
from sugar_network.zerosugar.spec import parse_version
from sugar_network.resources.volume import Resource


def _encode_version(version):
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


class Implementation(Resource):

    @ad.active_property(prefix='C',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def context(self, value):
        return value

    @ad.active_property(prefix='L', full_text=True, typecast=[GOOD_LICENSES],
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def license(self, value):
        return value

    @ad.active_property(slot=1, prefix='V', reprcast=_encode_version,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def version(self, value):
        return value

    @ad.active_property(prefix='S',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ,
            typecast=resources.STABILITIES)
    def stability(self, value):
        return value

    @ad.active_property(prefix='R', typecast=[], default=[],
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def requires(self, value):
        return value

    @ad.active_property(prefix='N', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def notes(self, value):
        return value

    @ad.active_property(ad.StoredProperty, typecast=dict, default={})
    def spec(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def data(self, value):
        if value:
            context = self.volume['context'].get(self['context'])
            value['name'] = [context['title'], self['version']]
        return value

    @data.setter
    def data(self, value):
        context = self.volume['context'].get(self['context'])
        if 'activity' in context['type']:
            self.request.content_type = 'application/vnd.olpc-sugar'
        return value
