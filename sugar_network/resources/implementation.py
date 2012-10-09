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

import active_document as ad
from sugar_network.zerosugar.licenses import GOOD_LICENSES

from sugar_network import resources
from sugar_network.zerosugar.spec import parse_version
from sugar_network.resources.volume import Resource


def _encode_version(version):
    result = ''
    version = parse_version(version)
    while version:
        unit, modificator = version[:2]
        del version[:2]
        unit = ([0, 0] + unit)[-3:]
        result += ''.join(['%05d' % i for i in unit])
        result += '%02d' % (10 + modificator)
    return result


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

    @ad.active_property(slot=2, prefix='D',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ, typecast=int)
    def date(self, value):
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
    def data(self, stat):
        return stat
