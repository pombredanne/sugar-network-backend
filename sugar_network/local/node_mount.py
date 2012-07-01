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
import json
import logging
import hashlib
from os.path import join, exists

import active_document as ad
from sugar_network import local
from sugar_network.toolkit import crypto, sugar, sneakernet
from sugar_network.local.mounts import ProxyCommands, LocalMount
from sugar_network.toolkit.collection import Sequences, PersistentSequences
from active_toolkit import coroutine


_DEFAULT_MASTER = '67bc1da07c5642b5dfd1ec713de2cce811e1b0ec'

_logger = logging.getLogger('local.node_mount')


class NodeMount(ProxyCommands, LocalMount):

    def __init__(self, volume, home_volume):
        ProxyCommands.__init__(self, home_volume)
        LocalMount.__init__(self, volume)

        self.volume = volume
        self._proxy = ad.VolumeCommands(volume)
        self._push_seq = PersistentSequences(
                local.path('push.sequence'), [1, None])
        self._pull_seq = PersistentSequences(
                local.path('pull.sequence'), [1, None])

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

    def super_call(self, request, response):
        return self._proxy.call(request, response)

    def sync(self, path, accept_length=None, push_sequence=None, session=None):
        to_push_seq = Sequences(empty_value=[1, None])
        if push_sequence is None:
            to_push_seq.update(self._push_seq)
        else:
            to_push_seq.update(push_sequence)

        if session is None:
            session_is_new = True
            session = _volume_hash(self.volume)
        else:
            session_is_new = False

        while True:
            self._import(path, session)

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
        pass

    def _import(self, path, session):
        for packet in sneakernet.walk(path):
            if packet.header.get('type') == 'push':
                if packet.header.get('sender') != self._node_guid:
                    _logger.debug('Processing %r PUSH packet', packet)
                    for msg in packet:
                        directory = self.volume[msg['document']]
                        directory.merge(msg['guid'], msg['diff'])
                    if packet.header.get('sender') == self._master_guid:
                        self._pull_seq.exclude(packet.header['sequence'])
                else:
                    if packet.header.get('session') == session:
                        _logger.debug('Preserve %r PUSH packet ' \
                                'from current session', packet)
                    else:
                        _logger.debug('Remove our previous %r PUSH packet',
                                packet)
                        os.unlink(packet.path)
            elif packet.header.get('type') == 'ack':
                if packet.header.get('sender') == self._master_guid and \
                        packet.header.get('receiver') == self._node_guid:
                    _logger.debug('Processing %r ACK packet', packet)
                    self._push_seq.exclude(packet.header['push_sequence'])
                    self._pull_seq.exclude(packet.header['pull_sequence'])
                    _logger.debug('Remove processed %r ACK packet', packet)
                    os.unlink(packet.path)
                else:
                    _logger.debug('Ignore misaddressed %r ACK packet', packet)
            else:
                _logger.debug('No need to process %r packet', packet)

    def _export(self, to_push_seq, pushed_seq, packet):
        _logger.debug('Generating %r PUSH packet', packet)
        for document, directory in self.volume.items():

            def patch():
                for seq, guid, diff in directory.diff(to_push_seq[document]):
                    coroutine.dispatch()
                    yield {'guid': guid, 'diff': diff}
                    to_push_seq[document].exclude(seq)
                    pushed_seq[document].include(seq)

            directory.commit()
            packet.push_messages(patch(), document=document)


def _volume_hash(volume):
    stamp = []
    for name, directory in volume.items():
        stamp.append((name, directory.seqno))
    return str(hashlib.sha1(json.dumps(stamp)).hexdigest())
