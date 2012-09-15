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

import active_document as ad
from sugar_network import node
from sugar_network.node.sync_master import SyncCommands
from active_toolkit import util, enforce


_DEFAULT_MASTER_GUID = 'api-testing.network.sugarlabs.org'

_logger = logging.getLogger('node.commands')


class NodeCommands(ad.VolumeCommands):

    def __init__(self, volume, subscriber=None):
        ad.VolumeCommands.__init__(self, volume)
        self._subscriber = subscriber
        self._is_master = False

        node_path = join(volume.root, 'node')
        master_path = join(volume.root, 'master')

        if exists(node_path):
            with file(node_path) as f:
                self._guid = f.read().strip()
        elif exists(master_path):
            with file(master_path) as f:
                self._guid = f.read().strip()
            self._is_master = True
        else:
            self._guid = ad.uuid()
            with file(node_path, 'w') as f:
                f.write(self._guid)

        if not self._is_master and not exists(master_path):
            with file(master_path, 'w') as f:
                f.write(_DEFAULT_MASTER_GUID)

        self._blobs = {}
        for document, directory in self.volume.items():
            self._blobs.setdefault(document, set())
            for prop in directory.metadata.values():
                if isinstance(prop, ad.BlobProperty):
                    self._blobs[document].add(prop.name)

    @ad.volume_command(method='GET')
    def hello(self, response):
        response.content_type = 'text/html'
        return _HELLO_HTML

    @ad.volume_command(method='GET', cmd='stat')
    def stat(self):
        return {'guid': self._guid,
                'master': self._is_master,
                'seqno': self.volume.seqno.value,
                }

    @ad.volume_command(method='POST', cmd='subscribe')
    def subscribe(self):
        enforce(self._subscriber is not None, 'Subscription is disabled')
        return self._subscriber.new_ticket()

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid):
        # Servers data should not be deleted immediately
        # to let master-node synchronization possible
        directory = self.volume[document]
        directory.update(guid, {'layer': ['deleted']})

    @ad.directory_command(method='GET',
            arguments={'offset': ad.to_int, 'limit': ad.to_int,
                'reply': ad.to_list})
    def find(self, document, request, offset=None, limit=None, query=None,
            reply=None, order_by=None, group_by=None, **kwargs):
        if limit is None:
            limit = node.find_limit.value
        elif limit > node.find_limit.value:
            _logger.warning('The find limit is restricted to %s',
                    node.find_limit.value)
            limit = node.find_limit.value

        blobs = None
        if reply:
            reply = set(reply)
            blobs = reply & self._blobs[document]
            reply = list(reply - blobs)

        layer = kwargs.get('layer', ['public'])
        if isinstance(layer, basestring):
            layer = [layer]
        if 'deleted' in layer:
            _logger.warning('Requesting "deleted" layer')
            layer.remove('deleted')
        kwargs['layer'] = layer

        result = ad.VolumeCommands.find(self, document, request, offset, limit,
                query, reply, order_by, group_by, **kwargs)

        if blobs:
            for props in result['result']:
                self._mixin_blob(document, blobs, props)

        return result

    @ad.document_command(method='GET', arguments={'reply': ad.to_list})
    def get(self, document, guid, request, reply=None):
        blobs = None
        if reply:
            reply = set(reply)
            blobs = reply & self._blobs[document]
            reply = list(reply - blobs)

        if not reply:
            reply = ['guid', 'layer']
        else:
            reply.append('layer')

        result = ad.VolumeCommands.get(self, document, guid, request, reply)
        enforce('deleted' not in result['layer'], ad.NotFound,
                'Document is not found')

        if blobs:
            self._mixin_blob(document, blobs, result)

        return result

    def resolve(self, request):
        cmd = ad.VolumeCommands.resolve(self, request)
        if cmd is None:
            return

        if cmd.permissions & ad.ACCESS_AUTH:
            enforce(request.principal is not None, node.Unauthorized,
                    'User is not authenticated')

        if cmd.permissions & ad.ACCESS_AUTHOR and 'guid' in request:
            doc = self.volume[request['document']].get(request['guid'])
            enforce(request.principal in doc['user'], ad.Forbidden,
                    'Operation is permitted only for authors')

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
                _logger.warning('No %r user to set author property',
                        user_guid)
                continue
            user = users.get(user_guid)
            if user['name']:
                authors.append(user['name'])
        props['author'] = authors

    def _mixin_blob(self, document, blobs, props):
        doc = self.volume[document].get(props['guid'])
        for name in blobs:

            def compose_url(value):
                if value is None:
                    value = '/'.join(['', document, props['guid'], name])
                if value.startswith('/'):
                    value = 'http://%s:%s%s' % \
                            (node.host.value, node.port.value, value)
                return value

            url = None
            meta = doc.meta(name)
            if meta is not None:
                url = meta.url()

            if type(url) is list:
                props[name] = [compose_url(i.get('url')) for i in url]
            else:
                props[name] = compose_url(url)


class MasterCommands(NodeCommands, SyncCommands):

    def __init__(self, volume, subscriber=None):
        NodeCommands.__init__(self, volume, subscriber)
        SyncCommands.__init__(self)


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
        message = 'Cannot read DSS public key gotten for registeration'
        util.exception(message)
        if node.trust_users.value:
            logging.warning('Failed to read registration pubkey, '
                    'but we trust users')
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
