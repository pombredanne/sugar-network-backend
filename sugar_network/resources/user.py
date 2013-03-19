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
from sugar_network.node import stats_user
from sugar_network.toolkit import http, enforce


class User(db.Document):

    @db.indexed_property(prefix='L', typecast=[], default=['public'])
    def layer(self, value):
        return value

    @db.indexed_property(slot=1, prefix='N', full_text=True)
    def name(self, value):
        return value

    @db.stored_property()
    def color(self, value):
        return value

    @db.indexed_property(prefix='S', default='',
            permissions=db.ACCESS_CREATE | db.ACCESS_WRITE)
    def machine_sn(self, value):
        return value

    @db.indexed_property(prefix='U', default='',
            permissions=db.ACCESS_CREATE | db.ACCESS_WRITE)
    def machine_uuid(self, value):
        return value

    @db.stored_property(permissions=db.ACCESS_CREATE)
    def pubkey(self, value):
        return value

    @db.indexed_property(prefix='T', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    @db.indexed_property(prefix='P', full_text=True, default='')
    def location(self, value):
        return value

    @db.indexed_property(slot=2, prefix='B', default=0, typecast=int)
    def birthday(self, value):
        return value

    @db.document_command(method='GET', cmd='stats-info',
            mime_type='application/json')
    def _stats_info(self, request):
        enforce(request.principal == self['guid'], http.Forbidden,
                'Operation is permitted only for authors')

        status = {}
        for rdb in stats_user.get_rrd(self.guid):
            status[rdb.name] = rdb.last + stats_user.stats_user_step.value

        # TODO Process client configuration in more general manner
        return {'enable': True,
                'step': stats_user.stats_user_step.value,
                'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
                'status': status,
                }

    @db.document_command(method='POST', cmd='stats-upload')
    def _stats_upload(self, request):
        enforce(request.principal == self['guid'], http.Forbidden,
                'Operation is permitted only for authors')

        name = request.content['name']
        values = request.content['values']
        rrd = stats_user.get_rrd(self.guid)
        for timestamp, values in values:
            rrd[name].put(values, timestamp)
