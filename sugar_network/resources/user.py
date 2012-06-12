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

import logging
import hashlib
import tempfile
from os.path import join
from gettext import gettext as _

import active_document as ad
from active_toolkit import util, enforce
from sugar_network.node import stats, trust_users
from sugar_network.toolkit.rrd import Rrd


class User(ad.Document):

    LAYOUT_VERSION = 2

    _rrd = {}

    @ad.active_property(slot=1, prefix='N', full_text=True)
    def nickname(self, value):
        return value

    @ad.active_property(slot=2, prefix='F', full_text=True, default='')
    def fullname(self, value):
        return value

    @ad.active_property(ad.StoredProperty)
    def color(self, value):
        return value

    @ad.active_property(slot=3, prefix='S', permissions=ad.ACCESS_CREATE)
    def machine_sn(self, value):
        return value

    @ad.active_property(slot=4, prefix='U', permissions=ad.ACCESS_CREATE)
    def machine_uuid(self, value):
        return value

    @ad.active_property(ad.StoredProperty, permissions=ad.ACCESS_CREATE)
    def pubkey(self, value):
        return value

    @ad.active_property(prefix='T', full_text=True, default=[], typecast=[])
    def tags(self, value):
        return value

    @ad.active_property(slot=5, prefix='L', full_text=True, default='')
    def location(self, value):
        return value

    @ad.active_property(slot=6, prefix='B', default=0, typecast=int)
    def birthday(self, value):
        return value

    @classmethod
    def before_create(cls, props):
        enforce('pubkey' in props,
                _('Property "pubkey" is required for user registeration'))
        props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'].strip())
        super(User, cls).before_create(props)

    @ad.document_command(method='GET', cmd='stats-info',
            permissions=ad.ACCESS_AUTHOR)
    def _stats_info(self):
        status = {}
        rrd = User._get_rrd(self.guid)
        for name, __, last_update in rrd.dbs:
            status[name] = last_update + stats.stats_step.value
        return {'enable': stats.stats.value,
                'step': stats.stats_step.value,
                'rras': stats.stats_client_rras.value,
                'status': status,
                }

    @ad.document_command(method='POST', cmd='stats-upload',
            permissions=ad.ACCESS_AUTHOR)
    def _stats_upload(self, request):
        name = request.content['name']
        values = request.content['values']
        rrd = User._get_rrd(self.guid)
        for timestamp, values in values:
            rrd.put(name, values, timestamp)

    @classmethod
    def _get_rrd(cls, guid):
        rrd = cls._rrd.get(guid)
        if rrd is None:
            rrd = cls._rrd[guid] = Rrd(
                    join(stats.stats_root.value, guid[:2], guid),
                    stats.stats_step.value, stats.stats_server_rras.value)
        return rrd


def _load_pubkey(pubkey):
    try:
        with tempfile.NamedTemporaryFile() as key_file:
            key_file.file.write(pubkey)
            key_file.file.flush()
            # SSH key needs to be converted to PKCS8 to ket M2Crypto read it
            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', key_file.name, '-e', '-m', 'PKCS8'])
    except Exception:
        message = _('Cannot read DSS public key gotten for registeration')
        util.exception(message)
        if trust_users.value:
            logging.warning(_('Failed to read registration pubkey, ' \
                    'but we trust users'))
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise ad.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8
