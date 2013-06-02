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
from sugar_network.resources.volume import Resource


class Report(Resource):

    @db.indexed_property(prefix='C',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def context(self, value):
        return value

    @db.indexed_property(prefix='V',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ, default='')
    def implementation(self, value):
        return value

    @implementation.setter
    def implementation(self, value):
        if value and 'version' not in self.props and 'implementation' in value:
            version = self.volume['implementation'].get(value)
            self['version'] = version['version']
        return value

    @db.stored_property(default='',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def version(self, value):
        return value

    @db.stored_property(typecast=dict, default={},
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def environ(self, value):
        return value

    @db.indexed_property(prefix='T',
            permissions=db.ACCESS_CREATE | db.ACCESS_READ)
    def error(self, value):
        return value

    @db.blob_property()
    def data(self, value):
        return value

    @db.document_command(method='GET', cmd='log',
            mime_type='text/html')
    def log(self, guid):
        # In further implementations, `data` might be a tarball
        data = self.meta('data')
        if data and 'blob' in data:
            return file(data['blob'], 'rb')
        else:
            return ''
