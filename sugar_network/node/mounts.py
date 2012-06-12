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

import locale
import hashlib
import logging
from os.path import exists
from gettext import gettext as _

import active_document as ad
from active_toolkit import util
from sugar_network import node


_logger = logging.getLogger('node.mounts')


class NodeCommands(object):

    volume = None

    def __init__(self):
        if not exists(node.privkey.value):
            _logger.info(_('Create DSA server key'))
            util.assert_call([
                '/usr/bin/ssh-keygen', '-q', '-t', 'dsa', '-f',
                node.privkey.value, '-C', '', '-N', ''])
        self._guid = _load_pubkey(node.privkey.value + '.pub')

    @ad.volume_command(method='GET')
    def hello(self, response):
        response.content_type = 'text/html'
        return _HELLO_HTML

    @ad.volume_command(method='GET', cmd='stat')
    def stat(self):
        documents = {}
        for name, directory in self.volume.items():
            documents[name] = {
                    'seqno': directory.seqno,
                    }
        return {'guid': self._guid,
                'master': node.master.value,
                'documents': documents,
                }


class _LocalCommands(object):
    """Support webui started in the same process with the server."""

    volume = None

    def __init__(self, volume):
        self._locale = locale.getdefaultlocale()[0].replace('_', '-')

        self.connect = volume.connect
        self.disconnect = volume.disconnect

    @ad.volume_command(cmd='is_connected', access_level=ad.ACCESS_LOCAL)
    def is_connected(self):
        return True

    @ad.property_command(cmd='get_blob', access_level=ad.ACCESS_LOCAL)
    def get_blob(self, document, guid, prop, request):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)
        return directory.stat_blob(guid, prop) or None

    def publish(self, event):
        # TODO Forward event to subscription socket
        _logger.warning(_('Ignore %r event, not implementated'), event)

    def call(self, request, response=None):
        request.pop('mountpoint')
        request.accept_language = [self._locale]
        if response is None:
            response = ad.Response()
        return ad.VolumeCommands.call(self, request, response)


class Mount(ad.VolumeCommands, NodeCommands, _LocalCommands):

    def __init__(self, volume):
        ad.VolumeCommands.__init__(self, volume)
        NodeCommands.__init__(self)
        _LocalCommands.__init__(self, volume)


def _load_pubkey(path):
    with file(path) as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('ssh-'):
                key = line.split()[1]
                return str(hashlib.sha1(key).hexdigest())
    raise RuntimeError(_('No valid DSA public key in %r') % path)


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
