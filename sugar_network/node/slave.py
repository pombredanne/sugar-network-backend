# Copyright (C) 2012-2014 Aleksey Lim
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

import os
import sys
import shutil
import logging
from urlparse import urlsplit
from os.path import join, dirname, exists, isabs
from gettext import gettext as _

from sugar_network import toolkit
from sugar_network.model.context import Context
from sugar_network.model.post import Post
from sugar_network.model.report import Report
from sugar_network.node import model
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.router import route, ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, packets, ranges, enforce


_logger = logging.getLogger('node.slave')


class SlaveRoutes(NodeRoutes):

    RESOURCES = (model.User, Context, Post, Report)

    def __init__(self, master_url, volume, **kwargs):
        guid_path = join(volume.root, 'etc', 'node')
        if exists(guid_path):
            with file(guid_path) as f:
                guid = f.read().strip()
        else:
            guid = toolkit.uuid()
            if not exists(dirname(guid_path)):
                os.makedirs(dirname(guid_path))
            with file(guid_path, 'w') as f:
                f.write(guid)
        NodeRoutes.__init__(self, guid, volume=volume, **kwargs)
        vardir = join(volume.root, 'var')
        self._push_r = toolkit.Bin(join(vardir, 'push'), [[1, None]])
        self._pull_r = toolkit.Bin(join(vardir, 'pull'), [[1, None]])
        self._master_guid = urlsplit(master_url).netloc
        self._master_url = master_url

    @route('POST', cmd='online_sync', acl=ACL.AUTH | ACL.ADMIN,
            arguments={'no_pull': bool})
    def online_sync(self, no_pull=False):
        conn = http.Connection(self._master_url)
        response = conn.request('POST',
                data=packets.encode(self._export(not no_pull), header={
                    'from': self.guid,
                    'to': self._master_guid,
                    }),
                params={'cmd': 'sync'},
                headers={'Transfer-Encoding': 'chunked'})
        self._import(packets.decode(response.raw))

    @route('POST', cmd='offline_sync', acl=ACL.AUTH | ACL.ADMIN)
    def offline_sync(self, path):
        enforce(isabs(path), "Argument 'path' is not an absolute path")

        _logger.debug('Start offline synchronization in %r', path)
        if not exists(path):
            os.makedirs(path)

        this.broadcast({
            'event': 'sync_progress',
            'progress': _('Reading sneakernet packages'),
            })
        requests = self._import(packets.decode_dir(path))

        this.broadcast({
            'event': 'sync_progress',
            'progress': _('Generating new sneakernet package'),
            })
        offline_script = join(dirname(sys.argv[0]), 'sugar-network-sync')
        if exists(offline_script):
            shutil.copy(offline_script, path)
        packets.encode_dir(requests + self._export(True), root=path, header={
            'from': self.guid,
            'to': self._master_guid,
            })

        _logger.debug('Synchronization completed')

    def status(self):
        result = NodeRoutes.status(self)
        result['mode'] = 'slave'
        return result

    def _import(self, package):
        requests = []

        for packet in package:
            sender = packet['from']
            from_master = (sender == self._master_guid)
            if packet.name == 'push':
                seqno, committed = model.patch_volume(packet)
                if seqno is not None:
                    if from_master:
                        with self._pull_r as r:
                            ranges.exclude(r, committed)
                    else:
                        requests.append(('request', {
                            'origin': sender,
                            'ranges': committed,
                            }, []))
                    with self._push_r as r:
                        ranges.exclude(r, seqno, seqno)
            elif packet.name == 'ack' and from_master and \
                    packet['to'] == self.guid:
                with self._pull_r as r:
                    ranges.exclude(r, packet['ack'])
                if packet['ranges']:
                    with self._push_r as r:
                        ranges.exclude(r, packet['ranges'])

        return requests

    def _export(self, pull):
        export = []
        if pull:
            export.append(('pull', {'ranges': self._pull_r.value}, None))
        export.append(('push', None, model.diff_volume(self._push_r.value)))
        return export
