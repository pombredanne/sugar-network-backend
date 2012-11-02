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

from gettext import gettext as _

import active_document as ad

from sugar_network.resources.volume import Resource


class Report(Resource):

    @ad.active_property(prefix='C',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def context(self, value):
        return value

    @ad.active_property(prefix='V',
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ, default='')
    def implementation(self, value):
        return value

    @implementation.setter
    def implementation(self, value):
        if value:
            version = self.volume['implementation'].get(value)
            self['version'] = version['version']
        return value

    @ad.active_property(prefix='D', full_text=True, localized=True,
            permissions=ad.ACCESS_CREATE | ad.ACCESS_READ)
    def description(self, value):
        return value

    @ad.active_property(ad.StoredProperty,
            permissions=ad.ACCESS_READ, default=_('Unknown'))
    def version(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def data(self, value):
        return value

    @ad.document_command(method='GET', cmd='log',
            mime_type='text/html')
    def log(self, guid):
        # In further implementations, `data` might be a tarball
        data = self.meta('data')
        if data and 'path' in data:
            return file(data['path'], 'rb')
        else:
            return ''
