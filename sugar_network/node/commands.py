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
from os.path import exists, join
from gettext import gettext as _

import active_document as ad
from sugar_network import node
from sugar_network.toolkit import sneakernet
from sugar_network.toolkit.collection import Sequences
from active_toolkit import util, enforce


_logger = logging.getLogger('node.commands')


class NodeCommands(ad.VolumeCommands):

    def __init__(self, master_url, volume, subscriber=None):
        ad.VolumeCommands.__init__(self, volume)
        self._subscriber = subscriber
        self._is_master = bool(master_url)

        if self._is_master:
            self._guid = master_url
        else:
            guid_path = join(volume.root, 'node')
            if exists(guid_path):
                with file(guid_path) as f:
                    self._guid = f.read().strip()
            else:
                self._guid = ad.uuid()
                with file(guid_path, 'w') as f:
                    f.write(self._guid)

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
                'master': self._is_master,
                'documents': documents,
                }

    @ad.volume_command(method='POST', cmd='subscribe',
            permissions=ad.ACCESS_AUTH)
    def subscribe(self):
        enforce(self._subscriber is not None, _('Subscription is disabled'))
        return self._subscriber.new_ticket()

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid):
        # Servers data should not be deleted immediately
        # to let master-node synchronization possible
        directory = self.volume[document]
        directory.update(guid, {'layer': ['deleted']})

    @ad.directory_command(method='GET')
    def find(self, document, request, offset=None, limit=None, query=None,
            reply=None, order_by=None, **kwargs):
        if limit is None:
            limit = node.find_limit.value
        elif limit > node.find_limit.value:
            _logger.warning(_('The find limit is restricted to %s'),
                    node.find_limit.value)
            limit = node.find_limit.value
        return ad.VolumeCommands.find(self, document, request, offset, limit,
                query, reply, order_by, **kwargs)

    def resolve(self, request):
        cmd = ad.VolumeCommands.resolve(self, request)
        if cmd is None:
            return

        if cmd.permissions & ad.ACCESS_AUTH:
            enforce(request.principal is not None, node.Unauthorized,
                    _('User is not authenticated'))

        if cmd.permissions & ad.ACCESS_AUTHOR and 'guid' in request:
            doc = self.volume[request['document']].get(request['guid'])
            enforce(request.principal in doc['user'], ad.Forbidden,
                    _('Operation is permitted only for authors'))

        return cmd

    def before_create(self, request, props):
        if request['document'] == 'user':
            props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'])
            props['user'] = [props['guid']]
        else:
            props['user'] = [request.principal]
            self._set_author(props)
        ad.VolumeCommands.before_create(self, request, props)

    def before_update(self, request, props):
        if 'user' in props:
            self._set_author(props)
        ad.VolumeCommands.before_update(self, request, props)

    def _set_author(self, props):
        users = self.volume['user']
        authors = []
        for user_guid in props['user']:
            if not users.exists(user_guid):
                _logger.warning(_('No %r user to set author property'),
                        user_guid)
                continue
            user = users.get(user_guid)
            if user['name']:
                authors.append(user['name'])
        props['author'] = authors


class MasterCommands(NodeCommands):

    def __init__(self, master_url, volume, subscriber=None):
        NodeCommands.__init__(self, master_url, volume, subscriber)
        self._api_url = master_url

    @ad.volume_command(method='POST', cmd='sync')
    def sync(self, request, response, accept_length=None):
        _logger.debug('Pushing %s bytes length packet', request.content_length)
        with sneakernet.InPacket(stream=request) as packet:
            enforce('src' in packet.header and \
                    packet.header['src'] != self._api_url,
                    _('Misaddressed packet'))
            enforce('dst' in packet.header and \
                    packet.header['dst'] == self._api_url,
                    _('Misaddressed packet'))

            if packet.header.get('type') == 'push':
                out_packet = sneakernet.OutPacket('ack')
                out_packet.header['dst'] = packet.header['src']
                out_packet.header['push_sequence'] = packet.header['sequence']
                out_packet.header['pull_sequence'] = self.volume.merge(packet)
            elif packet.header.get('type') == 'pull':
                out_packet = sneakernet.OutPacket('push', limit=accept_length)
                out_seq = out_packet.header['sequence'] = Sequences()
                try:
                    in_seq = Sequences(packet.header['sequence'])
                    self.volume.diff(in_seq, out_seq, out_packet)
                except sneakernet.DiskFull:
                    pass
            else:
                raise RuntimeError(_('Unrecognized packet'))

            if out_packet.closed:
                response.content_type = 'application/octet-stream'
                return

            out_packet.header['src'] = self._api_url
            content, response.content_length = out_packet.pop_content()
            return content


def _load_pubkey(pubkey):
    pubkey = pubkey.strip()
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
        if node.trust_users.value:
            logging.warning(_('Failed to read registration pubkey, ' \
                    'but we trust users'))
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise ad.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
