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

# pylint: disable-msg=E1101,E0102,E0202

import os

import xapian

from sugar_network import db, resources
from sugar_network.resources.volume import Resource
from sugar_network.toolkit.licenses import GOOD_LICENSES
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import http, util, enforce


def _encode_version(version):
    version = util.parse_version(version)
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

    @db.indexed_property(prefix='C',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def context(self, value):
        return value

    @context.setter
    def context(self, value):
        context = self.volume['context'].get(value)
        enforce(self.request.principal in context['author'], http.Forbidden,
                'Only Context authors can submit new Implementations')
        return value

    @db.indexed_property(prefix='L', full_text=True, typecast=[GOOD_LICENSES],
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def license(self, value):
        return value

    @db.indexed_property(slot=1, prefix='V', reprcast=_encode_version,
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def version(self, value):
        return value

    @db.indexed_property(prefix='S',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ,
            typecast=resources.STABILITIES)
    def stability(self, value):
        return value

    @db.indexed_property(prefix='R', typecast=[], default=[],
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def requires(self, value):
        return value

    @db.indexed_property(prefix='N', full_text=True, localized=True,
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def notes(self, value):
        return value

    @db.stored_property(typecast=dict, default={})
    def spec(self, value):
        return value

    @db.blob_property()
    def data(self, value):
        if value:
            context = self.volume['context'].get(self['context'])
            value['name'] = [context['title'], self['version']]
        return value

    @data.setter
    def data(self, value):
        context = self.volume['context'].get(self['context'])
        if 'activity' not in context['type']:
            return value

        def calc_uncompressed_size(path):
            uncompressed_size = 0
            with Bundle(path, mime_type='application/zip') as bundle:
                for arcname in bundle.get_names():
                    uncompressed_size += bundle.getmember(arcname).size
            value['uncompressed_size'] = uncompressed_size

        if 'blob' in value:
            calc_uncompressed_size(value['blob'])
        elif 'url' in value:
            with util.NamedTemporaryFile() as f:
                http.download(value['url'], f.name)
                value['blob_size'] = os.stat(f.name).st_size
                calc_uncompressed_size(f.name)

        value['mime_type'] = 'application/vnd.olpc-sugar'
        return value
