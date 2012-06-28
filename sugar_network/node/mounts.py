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
from sugar_network import node
from sugar_network.toolkit import sugar
from active_toolkit import util, enforce


_logger = logging.getLogger('node.mounts')


class NodeCommands(ad.ProxyCommands):

    def __init__(self, volume):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))

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

    @ad.directory_command(method='POST',
            permissions=ad.ACCESS_AUTH)
    def create(self, document, request):
        self._set_author(document, request.content)
        raise ad.CommandNotFound()

    @ad.document_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update(self, document, guid, request):
        self._set_author(document, request.content)
        raise ad.CommandNotFound()

    @ad.property_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update_prop(self, document, guid, prop, request, url=None):
        enforce(prop not in ('user', 'author'),
                _('Direct property setting is forbidden'))
        raise ad.CommandNotFound()

    def _set_author(self, document, props):
        if document == 'user' or 'user' not in props:
            return
        users = self.volume['user']
        authors = []
        for user_guid in props['user']:
            if not users.exists(user_guid):
                _logger.warning(_('No %s user to set author property'),
                        user_guid)
                continue
            user = users.get(user_guid)
            authors.append(user['nickname'])
            if user['fullname']:
                authors.append(user['fullname'])
        props['author'] = authors


class Mount(NodeCommands):

    def __init__(self, volume):
        NodeCommands.__init__(self, volume)
        self._locale = locale.getdefaultlocale()[0].replace('_', '-')

    @ad.volume_command(cmd='mounted', access_level=ad.ACCESS_LOCAL)
    def mounted(self):
        return True

    @ad.property_command(cmd='get_blob', access_level=ad.ACCESS_LOCAL)
    def get_blob(self, document, guid, prop, request):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)
        return directory.get(guid).meta(prop)

    def publish(self, event):
        # TODO Forward event to subscription socket
        _logger.warning(_('Ignore %r event, not implementated'), event)

    def call(self, request, response=None):
        if 'mountpoint' in request:
            # In case if mount is being used for local clients
            request.pop('mountpoint')
            request.principal = sugar.uid()
            request.accept_language = [self._locale]
            if response is None:
                response = ad.Response()
        return NodeCommands.call(self, request, response)

    def connect(self, callback, condition=None):
        return self.volume.connect(callback, condition)

    def disconnect(self, callback):
        return self.volume.disconnect(callback)


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
