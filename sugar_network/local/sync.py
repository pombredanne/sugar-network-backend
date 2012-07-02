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

import os
import logging
from os.path import join, exists
from gettext import gettext as _

import active_document as ad
from sugar_network.toolkit import crypto, sugar, sneakernet
from sugar_network.local.mounts import LocalMount
from sugar_network.toolkit.collection import Sequences, PersistentSequences
from active_toolkit import coroutine, util


_DEFAULT_MASTER = '67bc1da07c5642b5dfd1ec713de2cce811e1b0ec'
_DIFF_CHUNK = 1024

_logger = logging.getLogger('local.sync')


class NodeMount(LocalMount):

    def __init__(self, volume, home_volume):
        LocalMount.__init__(self, volume, home_volume)

        self._push_seq = PersistentSequences(
                join(volume.root, 'push.sequence'), [1, None], volume.keys())
        self._pull_seq = PersistentSequences(
                join(volume.root, 'pull.sequence'), [1, None], volume.keys())
        self._sync_session = None

        self._node_guid = crypto.ensure_dsa_pubkey(
                sugar.profile_path('owner.key'))
        master_guid_path = join(volume.root, 'master')
        if exists(master_guid_path):
            with file(master_guid_path) as f:
                self._master_guid = f.read().strip()
        else:
            self._master_guid = _DEFAULT_MASTER
            with file(master_guid_path, 'w') as f:
                f.write(self._master_guid)

    def sync(self, path, accept_length=None, push_sequence=None, session=None):
        to_push_seq = Sequences(empty_value=[1, None])
        if push_sequence is None:
            to_push_seq.update(self._push_seq)
        else:
            to_push_seq.update(push_sequence)

        if session is None:
            session_is_new = True
            session = ad.uuid()
        else:
            session_is_new = False

        while True:
            self._import(path, session)
            self._push_seq.commit()
            self._pull_seq.commit()

            if session_is_new:
                with sneakernet.OutPacket('pull', root=path,
                        sender=self._node_guid, receiver=self._master_guid,
                        session=session) as packet:
                    packet.header['sequence'] = self._pull_seq

            with sneakernet.OutPacket('push', root=path, limit=accept_length,
                    sender=self._node_guid, receiver=self._master_guid,
                    session=session) as packet:
                packet.header['sequence'] = pushed_seq = Sequences()
                try:
                    self._export(to_push_seq, pushed_seq, packet)
                except sneakernet.DiskFull:
                    _logger.debug('Reach package size limit')
                    if not pushed_seq:
                        packet.clear()
                    return {'push_sequence': to_push_seq, 'session': session}
                except Exception:
                    packet.clear()
                    raise
                else:
                    break

    def sync_session(self, mounts):
        _logger.debug('Start synchronization session with %r session ' \
                'for %r mounts', self._sync_session, mounts)

        try:
            for path in mounts:
                self.publish({'event': 'sync', 'path': path})
                self._sync_session = \
                        self.sync(path, **(self._sync_session or {}))
                if self._sync_session is None:
                    break
        except Exception, error:
            util.exception(_logger, _('Failed to complete synchronization'))
            self.publish({'event': 'sync_failed', 'error': str(error)})
            return

        if self._sync_session is None:
            _logger.debug('Synchronization completed')
            self.publish({'event': 'sync_complete'})
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._sync_session)
            self.publish({'event': 'sync_continue'})

    def _import(self, path, session):
        for packet in sneakernet.walk(path):
            if packet.header.get('type') == 'push' and \
                    packet.header.get('sender') != self._node_guid:
                _logger.debug('Processing %r PUSH packet', packet)
                for msg in packet:
                    directory = self.volume[msg['document']]
                    directory.merge(msg['guid'], msg['diff'])
                if packet.header.get('sender') == self._master_guid:
                    self._pull_seq.exclude(packet.header['sequence'])

            elif packet.header.get('type') == 'ack' and \
                    packet.header.get('sender') == self._master_guid and \
                    packet.header.get('receiver') == self._node_guid:
                _logger.debug('Processing %r ACK packet', packet)
                self._push_seq.exclude(packet.header['push_sequence'])
                self._pull_seq.exclude(packet.header['pull_sequence'])
                _logger.debug('Remove processed %r ACK packet', packet)
                os.unlink(packet.path)

            elif packet.header.get('type') in ('push', 'pull') and \
                    packet.header.get('sender') == self._node_guid:
                if packet.header.get('session') == session:
                    _logger.debug('Keep current session %r packet', packet)
                else:
                    _logger.debug('Remove our previous %r packet', packet)
                    os.unlink(packet.path)

            else:
                _logger.debug('No need to process %r packet', packet)

    def _export(self, to_push_seq, pushed_seq, packet):
        _logger.debug('Generating %r PUSH packet', packet)

        for document, directory in self.volume.items():
            seq, diff = directory.diff(
                    to_push_seq[document], limit=_DIFF_CHUNK)

            def patch(diff):
                for header, data in diff:
                    coroutine.dispatch()
                    if hasattr(data, 'fileno'):
                        packet.push_blob(data, document=document, **header)
                    else:
                        header['diff'] = data
                        yield header

            try:
                directory.commit()
                packet.push_messages(patch(diff), document=document)
            finally:
                if seq:
                    to_push_seq[document].exclude(*seq)
                    pushed_seq[document].include(*seq)
