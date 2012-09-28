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

import active_document as ad
from sugar_network.node import stats
from active_toolkit import enforce


class User(ad.Document):

    @ad.active_property(prefix='L', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @ad.active_property(slot=1, prefix='N', full_text=True)
    def name(self, value):
        return value

    @ad.active_property(ad.StoredProperty)
    def color(self, value):
        return value

    @ad.active_property(prefix='S', permissions=ad.ACCESS_CREATE)
    def machine_sn(self, value):
        return value

    @ad.active_property(prefix='U', permissions=ad.ACCESS_CREATE)
    def machine_uuid(self, value):
        return value

    @ad.active_property(ad.StoredProperty, permissions=ad.ACCESS_CREATE)
    def pubkey(self, value):
        return value

    @ad.active_property(prefix='T', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    @ad.active_property(prefix='P', full_text=True, default='')
    def location(self, value):
        return value

    @ad.active_property(slot=2, prefix='B', default=0, typecast=int)
    def birthday(self, value):
        return value

    @ad.document_command(method='GET', cmd='stats-info')
    def _stats_info(self, request):
        enforce(request.principal == self['guid'], ad.Forbidden,
                'Operation is permitted only for authors')

        status = {}
        rrd = stats.get_rrd(self.guid)
        for name, __, last_update in rrd.dbs:
            status[name] = last_update + stats.stats_step.value
        # TODO Process client configuration in more general manner
        return {'enable': stats.stats.value,
                'step': stats.stats_step.value,
                'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
                'status': status,
                }

    @ad.document_command(method='POST', cmd='stats-upload')
    def _stats_upload(self, request):
        enforce(request.principal == self['guid'], ad.Forbidden,
                'Operation is permitted only for authors')

        name = request.content['name']
        values = request.content['values']
        rrd = stats.get_rrd(self.guid)
        for timestamp, values in values:
            rrd.put(name, values, timestamp)
