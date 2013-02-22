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

import logging
from os.path import join

from sugar_network import db
from sugar_network.client import Client
from sugar_network.node import sync, stats_user, files, files_root
from sugar_network.node.commands import NodeCommands
from sugar_network.toolkit import util


_SYNC_DIRNAME = '.sugar-network-sync'

_logger = logging.getLogger('node.slave')


class SlaveCommands(NodeCommands):

    def __init__(self, guid, volume):
        NodeCommands.__init__(self, False, guid, volume)

        self._push_seq = util.PersistentSequence(
                join(volume.root, 'push.sequence'), [1, None])
        self._pull_seq = util.PersistentSequence(
                join(volume.root, 'pull.sequence'), [1, None])
        self._files_seq = util.PersistentSequence(
                join(volume.root, 'files.sequence'), [1, None])

    @db.volume_command(method='POST', cmd='online_sync',
            permissions=db.ACCESS_LOCAL)
    def online_sync(self):
        push = [('diff', None, sync.diff(self.volume, self._push_seq)),
                ('pull', {'sequence': self._pull_seq}, None),
                ('files_pull', {'sequence': self._files_seq}, None),
                ]
        if stats_user.stats_user.value:
            push.append(('stats_diff', None, stats_user.diff()))
        response = Client().request('POST',
                data=sync.chunked_encode(*push), params={'cmd': 'sync'},
                headers={'Transfer-Encoding': 'chunked'})

        for packet in sync.decode(response.raw):
            if packet.name == 'diff':
                seq, __ = sync.merge(self.volume, packet, shift_seqno=False)
                if seq:
                    self._pull_seq.exclude(seq)
                    self._pull_seq.commit()
            elif packet.name == 'ack':
                self._pull_seq.exclude(packet['ack'])
                self._pull_seq.commit()
                self._push_seq.exclude(packet['sequence'])
                self._push_seq.commit()
            elif packet.name == 'stats_ack':
                stats_user.commit(packet['sequence'])
            elif packet.name == 'files_diff':
                seq = files.merge(files_root.value, packet)
                if seq:
                    self._files_seq.exclude(seq)
                    self._files_seq.commit()


"""
class SlaveCommands(NodeCommands):

    def __init__(self, guid, volume, stats=None):
        NodeCommands.__init__(self, False, guid, volume, stats)

        self._jobs = coroutine.Pool()
        self._mounts = util.MutableStack()
        self._offline_script = join(dirname(sys.argv[0]), 'sugar-network-sync')
        self._file_syncs = \
                files_sync.Leechers(sync_dirs.value, volume.root)
        self._offline_session = None

        mountpoints.connect(_SYNC_DIRNAME,
                self.__found_mountcb, self.__lost_mount_cb)

    @db.volume_command(method='POST', cmd='start_offline_sync')
    def start_offline_sync(self, rewind=False, path=None):
        if self._jobs:
            return
        enforce(path or self._mounts, 'No mounts to synchronize with')
        if rewind:
            self._mounts.rewind()
        self._jobs.spawn(self._offline_sync, path)

    @db.volume_command(method='POST', cmd='break_offline_sync')
    def break_offline_sync(self):
        self._jobs.kill()

    def _offline_sync(self, path=None):
        _logger.debug('Start synchronization session with %r session '
                'for %r mounts', self._offline_session, self._mounts)

        def sync(path):
            self.broadcast({'event': 'sync_start', 'path': path})
            self._offline_session = self._offline_sync_session(path,
                    **(self._offline_session or {}))
            return self._offline_session is None

        try:
            while True:
                if path and sync(path):
                    break
                for mountpoint in self._mounts:
                    if sync(mountpoint):
                        break
                break
        except Exception, error:
            util.exception(_logger, 'Failed to complete synchronization')
            self.broadcast({'event': 'sync_error', 'error': str(error)})
            self._offline_session = None

        if self._offline_session is None:
            _logger.debug('Synchronization completed')
            self.broadcast({'event': 'sync_complete'})
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._offline_session)
            self.broadcast({'event': 'sync_continue'})






    def _offline_sync_session(self, path, accept_length=None,
            diff_sequence=None, stats_sequence=None, session=None):
        to_push_seq = util.Sequence(empty_value=[1, None])
        if diff_sequence is None:
            to_push_seq.include(self._push_seq)
        else:
            to_push_seq = util.Sequence(diff_sequence)

        if stats_sequence is None:
            stats_sequence = {}

        if session is None:
            session_is_new = True
            session = util.uuid()
        else:
            session_is_new = False

        while True:
            for packet in sneakernet.walk(path):
                if packet.header.get('src') == self.guid:
                    if packet.header.get('session') == session:
                        _logger.debug('Keep current session %r packet', packet)
                    else:
                        _logger.debug('Remove our previous %r packet', packet)
                        os.unlink(packet.path)
                else:
                    self._import(packet, to_push_seq)
                    self._push_seq.commit()
                    self._pull_seq.commit()

            if exists(self._offline_script):
                shutil.copy(self._offline_script, path)

            with OutFilePacket(path, limit=accept_length,
                    src=self.guid, dst=api_url.value,
                    session=session, seqno=self.volume.seqno.value,
                    api_url=client.api_url.value) as packet:







    def _export(self, packet):
        if session_is_new:
            for directory, sync in self._file_syncs.items():
                packet.push(cmd='files_pull', directory=directory,
                        sequence=sync.sequence)
            packet.push(cmd='sn_pull', sequence=self._pull_seq)

        _logger.debug('Generating %r PUSH packet to %r', packet, packet.path)
        self.broadcast({
            'event': 'sync_progress',
            'progress': _('Generating %r packet') % packet.basename,
            })

        try:
            self.volume.diff(to_push_seq, packet)
            stats.pull(stats_sequence, packet)
        except DiskFull:
            return {'diff_sequence': to_push_seq,
                    'stats_sequence': stats_sequence,
                    'session': session,
                    }
        else:
            break




    def _import(self, packet, to_push_seq):
        self.broadcast({
            'event': 'sync_progress',
            'progress': _('Reading %r packet') % basename(packet.path),
            })
        _logger.debug('Processing %r PUSH packet from %r', packet, packet.path)

        from_master = (packet.header.get('src') == self._master_guid)

        for record in packet.records():
            cmd = record.get('cmd')
            if cmd == 'sn_push':
                self.volume.merge(record, increment_seqno=False)
            elif from_master:
                if cmd == 'sn_commit':
                    _logger.debug('Processing %r COMMIT from %r',
                            record, packet)
                    self._pull_seq.exclude(record['sequence'])
                elif cmd == 'sn_ack' and \
                        record['dst'] == self.guid:
                    _logger.debug('Processing %r ACK from %r', record, packet)
                    self._push_seq.exclude(record['sequence'])
                    self._pull_seq.exclude(record['merged'])
                    to_push_seq.exclude(record['sequence'])
                    self.volume.seqno.next()
                    self.volume.seqno.commit()
                elif cmd == 'stats_ack' and record['dst'] == self.guid:
                    _logger.debug('Processing %r stats ACK from %r',
                            record, packet)
                    stats.commit(record['sequence'])
                elif record.get('directory') in self._file_syncs:
                    self._file_syncs[record['directory']].push(record)

    def __found_mountcb(self, path):
        self._mounts.add(path)
        _logger.debug('Found %r sync mount', path)
        self.start_offline_sync()

    def __lost_mount_cb(self, path):
        self._mounts.remove(path)
        if not self._mounts:
            self.break_offline_sync()
"""
