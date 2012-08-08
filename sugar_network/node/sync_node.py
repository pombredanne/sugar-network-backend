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
import sys
import shutil
import logging
from os.path import join, dirname, exists, basename
from gettext import gettext as _

import active_document as ad
from sugar_network import node, local
from sugar_network.toolkit import mounts_monitor, sneakernet
from sugar_network.toolkit.collection import MutableStack
from sugar_network.toolkit.files_sync import Leechers
from sugar_network.toolkit.collection import Sequence, PersistentSequence
from sugar_network.toolkit.sneakernet import OutFilePacket, DiskFull
from active_toolkit import coroutine, util, enforce


_SYNC_DIRNAME = '.sugar-network-sync'

_logger = logging.getLogger('node.sync_node')


class SyncCommands(object):

    def __init__(self, sequences_path):
        self._sync = coroutine.Pool()
        self._sync_mounts = MutableStack()
        self._file_syncs = Leechers(node.sync_dirs.value, sequences_path)
        self._sync_session = None
        self._push_seq = PersistentSequence(
                join(sequences_path, 'push'), [1, None])
        self._pull_seq = PersistentSequence(
                join(sequences_path, 'pull'), [1, None])
        self._sync_script = join(dirname(sys.argv[0]), 'sugar-network-sync')
        self._mount = None

        mounts_monitor.connect(_SYNC_DIRNAME,
                self.__found_mountcb, self.__lost_mount_cb)

    @property
    def node_mount(self):
        return self._mount

    @node_mount.setter
    def node_mount(self, value):
        if self._mount is value:
            return
        self._mount = value
        if self._sync_mounts:
            self.start_sync()

    @ad.volume_command(method='POST', cmd='start_sync')
    def start_sync(self, rewind=False, path=None):
        enforce(self._mount is not None, 'No server to sync')

        if self._sync:
            return

        enforce(self._mount is not None, 'No server to synchronize')
        enforce(path or self._sync_mounts, 'No mounts to synchronize with')

        if rewind:
            self._sync_mounts.rewind()
        self._sync.spawn(self.sync_session, path)

    @ad.volume_command(method='POST', cmd='break_sync')
    def break_sync(self):
        self._sync.kill()

    def sync(self, path, accept_length=None, push_sequence=None, session=None):
        enforce(self._mount is not None, 'No server to sync')

        to_push_seq = Sequence(empty_value=[1, None])
        if push_sequence is None:
            to_push_seq.include(self._push_seq)
        else:
            to_push_seq = Sequence(push_sequence)

        if session is None:
            session_is_new = True
            session = ad.uuid()
        else:
            session_is_new = False

        while True:
            for packet in sneakernet.walk(path):
                if packet.header.get('src') == self._mount.node_guid:
                    if packet.header.get('session') == session:
                        _logger.debug('Keep current session %r packet', packet)
                    else:
                        _logger.debug('Remove our previous %r packet', packet)
                        os.unlink(packet.path)
                else:
                    self._import(packet, to_push_seq)
                    self._push_seq.commit()
                    self._pull_seq.commit()

            if exists(self._sync_script):
                shutil.copy(self._sync_script, path)

            with OutFilePacket(path, limit=accept_length,
                    src=self._mount.node_guid, dst=self._mount.master_guid,
                    session=session, seqno=self._mount.volume.seqno.value,
                    api_url=local.api_url.value) as packet:
                if session_is_new:
                    for directory, sync in self._file_syncs.items():
                        packet.push(cmd='files_pull', directory=directory,
                                sequence=sync.sequence)
                    packet.push(cmd='sn_pull', sequence=self._pull_seq)

                _logger.debug('Generating %r PUSH packet to %r',
                        packet, packet.path)
                self._mount.publish({
                    'event': 'sync_progress',
                    'progress': _('Generating %r packet') % packet.basename,
                    })

                try:
                    self._mount.volume.diff(to_push_seq, packet)
                except DiskFull:
                    return {'push_sequence': to_push_seq, 'session': session}
                else:
                    break

    def sync_session(self, path=None):
        enforce(self._mount is not None, 'No server to sync')

        _logger.debug('Start synchronization session with %r session '
                'for %r mounts', self._sync_session, self._sync_mounts)

        def sync(path):
            self._mount.publish({'event': 'sync_start', 'path': path})
            self._sync_session = self.sync(path, **(self._sync_session or {}))
            return self._sync_session is None

        try:
            while True:
                if path and sync(path):
                    break
                for mountpoint in self._sync_mounts:
                    if sync(mountpoint):
                        break
                break
        except Exception, error:
            util.exception(_logger, 'Failed to complete synchronization')
            self._mount.publish({'event': 'sync_error', 'error': str(error)})
            self._sync_session = None

        if self._sync_session is None:
            _logger.debug('Synchronization completed')
            self._mount.publish({'event': 'sync_complete'})
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._sync_session)
            self._mount.publish({'event': 'sync_continue'})

    def _import(self, packet, to_push_seq):
        self._mount.publish({
            'event': 'sync_progress',
            'progress': _('Reading %r packet') % basename(packet.path),
            })
        _logger.debug('Processing %r PUSH packet from %r', packet, packet.path)

        from_master = (packet.header.get('src') == self._mount.master_guid)

        for record in packet.records():
            cmd = record.get('cmd')
            if cmd == 'sn_push':
                self._mount.volume.merge(record, increment_seqno=False)
            elif from_master:
                if cmd == 'sn_commit':
                    _logger.debug('Processing %r COMMIT from %r',
                            record, packet)
                    self._pull_seq.exclude(record['sequence'])
                elif cmd == 'sn_ack' and \
                        record['dst'] == self._mount.node_guid:
                    _logger.debug('Processing %r ACK from %r', record, packet)
                    self._push_seq.exclude(record['sequence'])
                    self._pull_seq.exclude(record['merged'])
                    to_push_seq.exclude(record['sequence'])
                    self._mount.volume.seqno.next()
                    self._mount.volume.seqno.commit()
                elif record.get('directory') in self._file_syncs:
                    self._file_syncs[record['directory']].push(record)

    def __found_mountcb(self, path):
        self._sync_mounts.add(path)
        if self._mount is not None:
            _logger.debug('Found %r sync mount', path)
            self.start_sync()
        else:
            _logger.debug('Found %r sync mount but no servers', path)

    def __lost_mount_cb(self, path):
        self._sync_mounts.remove(join(path, _SYNC_DIRNAME))
        if not self._sync_mounts:
            self.break_sync()
