# Copyright (C) 2013 Aleksey Lim
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
from urlparse import urlsplit
from os.path import join

from sugar_network import db, client
from sugar_network.node import sync, stats_user, files_root, files
from sugar_network.node.commands import NodeCommands
from sugar_network.toolkit import util


_logger = logging.getLogger('node.master')


class MasterCommands(NodeCommands):

    def __init__(self, volume):
        guid = urlsplit(client.api_url.value).netloc
        NodeCommands.__init__(self, True, guid, volume)
        self._files = None

        if files_root.value:
            self._files = files.Index(files_root.value,
                    join(volume.root, 'files.index'), volume.seqno)

    @db.volume_command(method='POST', cmd='sync',
            permissions=db.ACCESS_AUTH)
    def sync(self, request):
        reply = []

        for packet in sync.decode(request.content_stream):
            if packet.name == 'pull':
                seq = util.Sequence(packet['sequence'])
                reply.append(('diff', None, sync.diff(self.volume, seq)))
            elif packet.name == 'files_pull':
                if self._files is not None:
                    seq = util.Sequence(packet['sequence'])
                    reply.append(('files_diff', None, self._files.diff(seq)))
            elif packet.name == 'diff':
                seq, ack_seq = sync.merge(self.volume, packet)
                reply.append(('ack', {'ack': ack_seq, 'sequence': seq}, None))
            elif packet.name == 'stats_diff':
                seq = stats_user.merge(packet)
                reply.append(('stats_ack', {'sequence': seq}, None))

        return sync.encode(*reply)
