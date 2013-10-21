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

import os
import sys
import shutil
import logging
from urlparse import urlsplit
from os.path import join, dirname, exists, isabs
from gettext import gettext as _

from sugar_network import node, toolkit
from sugar_network.client import api_url
from sugar_network.node import sync, stats_user, files, volume
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.router import route, ACL
from sugar_network.toolkit import http, enforce


_logger = logging.getLogger('node.slave')


class SlaveRoutes(NodeRoutes):

    def __init__(self, key_path, volume_):
        self._auth = http.SugarAuth(key_path)
        NodeRoutes.__init__(self, self._auth.login, volume_)

        self._push_seq = toolkit.PersistentSequence(
                join(volume_.root, 'push.sequence'), [1, None])
        self._pull_seq = toolkit.PersistentSequence(
                join(volume_.root, 'pull.sequence'), [1, None])
        self._files_seq = toolkit.PersistentSequence(
                join(volume_.root, 'files.sequence'), [1, None])
        self._master_guid = urlsplit(api_url.value).netloc
        self._offline_session = None

    @route('POST', cmd='online-sync', acl=ACL.LOCAL)
    def online_sync(self, no_pull=False):
        conn = http.Connection(api_url.value, auth=self._auth)

        # TODO `http.Connection` should handle re-POSTing without
        # loosing payload after authentication
        conn.get(cmd='logon')

        push = [('diff', None, volume.diff(self.volume, self._push_seq))]
        if not no_pull:
            push.extend([
                ('pull', {
                    'sequence': self._pull_seq,
                    'layer': node.sync_layers.value,
                    }, None),
                ('files_pull', {'sequence': self._files_seq}, None),
                ])
        if stats_user.stats_user.value:
            push.append(('stats_diff', None, stats_user.diff()))
        response = conn.request('POST',
                data=sync.encode(push, src=self.guid, dst=self._master_guid),
                params={'cmd': 'sync'},
                headers={'Transfer-Encoding': 'chunked'})
        self._import(sync.decode(response.raw), None)

    @route('POST', cmd='offline-sync', acl=ACL.LOCAL)
    def offline_sync(self, path):
        enforce(node.sync_layers.value,
                '--sync-layers is not specified, the full master dump '
                'might be too big and should be limited')
        enforce(isabs(path), 'Argument \'path\' should be an absolute path')

        _logger.debug('Start %r synchronization session in %r',
                self._offline_session, path)

        if not exists(path):
            os.makedirs(path)

        try:
            self._offline_session = self._offline_sync(path,
                    **(self._offline_session or {}))
        except Exception:
            toolkit.exception(_logger, 'Failed to complete synchronization')
            self._offline_session = None
            raise

        if self._offline_session is None:
            _logger.debug('Synchronization completed')
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._offline_session)

    def status(self):
        result = NodeRoutes.status(self)
        result['level'] = 'slave'
        return result

    def _offline_sync(self, path, push_seq=None, stats_seq=None, session=None):
        push = []

        if push_seq is None:
            push_seq = toolkit.Sequence(self._push_seq)
        if stats_seq is None:
            stats_seq = {}
        if session is None:
            session = toolkit.uuid()
            push.append(('pull', {
                'sequence': self._pull_seq,
                'layer': node.sync_layers.value,
                }, None))
            push.append(('files_pull', {'sequence': self._files_seq}, None))

        self.broadcast({
            'event': 'sync_progress',
            'progress': _('Reading sneakernet packages'),
            })
        self._import(sync.sneakernet_decode(path), push_seq)

        offline_script = join(dirname(sys.argv[0]), 'sugar-network-sync')
        if exists(offline_script):
            shutil.copy(offline_script, path)

        self.broadcast({
            'event': 'sync_progress',
            'progress': _('Generating new sneakernet package'),
            })

        diff_seq = toolkit.Sequence([])
        push.append(('diff', None,
                volume.diff(self.volume, push_seq, diff_seq)))
        if stats_user.stats_user.value:
            push.append(('stats_diff', None, stats_user.diff(stats_seq)))
        complete = sync.sneakernet_encode(push, root=path,
                src=self.guid, dst=self._master_guid, api_url=api_url.value,
                session=session)
        if not complete:
            push_seq.exclude(diff_seq)
            return {'push_seq': push_seq,
                    'stats_seq': stats_seq,
                    'session': session,
                    }

    def _import(self, package, push_seq):
        for packet in package:
            from_master = (packet['src'] == self._master_guid)
            addressed = (packet['dst'] == self.guid)

            if packet.name == 'diff':
                _logger.debug('Processing %r', packet)
                seq, __ = volume.merge(self.volume, packet, shift_seqno=False)
                if from_master and seq:
                    self._pull_seq.exclude(seq)
                    self._pull_seq.commit()

            elif from_master:
                if packet.name == 'ack' and addressed:
                    _logger.debug('Processing %r', packet)
                    if push_seq:
                        push_seq.exclude(packet['sequence'])
                    self._pull_seq.exclude(packet['ack'])
                    self._pull_seq.commit()
                    self._push_seq.exclude(packet['sequence'])
                    self._push_seq.commit()
                elif packet.name == 'stats_ack' and addressed:
                    _logger.debug('Processing %r', packet)
                    stats_user.commit(packet['sequence'])
                elif packet.name == 'files_diff':
                    _logger.debug('Processing %r', packet)
                    seq = files.merge(node.files_root.value, packet)
                    if seq:
                        self._files_seq.exclude(seq)
                        self._files_seq.commit()
