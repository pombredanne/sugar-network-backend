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
from gettext import gettext as _

import active_document as ad
from sugar_network import node
from sugar_network.toolkit import crypto
from active_toolkit import enforce


_logger = logging.getLogger('node.commands')


class NodeCommands(ad.ProxyCommands):

    def __init__(self, keypath, volume, subscriber=None):
        ad.ProxyCommands.__init__(self, ad.VolumeCommands(volume))
        self._subscriber = subscriber
        self._guid = crypto.ensure_dsa_pubkey(keypath)

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

    @ad.volume_command(method='POST', cmd='subscribe',
            permissions=ad.ACCESS_AUTH)
    def subscribe(self):
        enforce(self._subscriber is not None, _('Subscription is disabled'))
        return self._subscriber.new_ticket()

    @ad.directory_command(method='POST',
            permissions=ad.ACCESS_AUTH)
    def create(self, document, request):
        user = [request.principal] if request.principal else []
        request.content['user'] = user
        self._set_author(document, request)
        raise ad.CommandNotFound()

    @ad.document_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update(self, document, guid, request):
        self._set_author(document, request)
        raise ad.CommandNotFound()

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid, request):
        # Servers data should not be deleted immediately
        # to let master-node synchronization possible
        return self.super_call('PUT', 'hide', document=document, guid=guid,
                principal=request.principal)

    @ad.property_command(method='PUT',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def update_prop(self, document, guid, prop, request, url=None):
        enforce(prop not in ('user', 'author'),
                _('Direct property setting is forbidden'))
        raise ad.CommandNotFound()

    def _set_author(self, document, request):
        props = request.content
        if document == 'user' or 'user' not in props:
            return
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


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
