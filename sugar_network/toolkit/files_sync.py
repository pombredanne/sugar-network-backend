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
from bisect import bisect_left
from os.path import join, exists, relpath, lexists

from sugar_network.toolkit.collection import Sequence
from active_toolkit import util, coroutine


_logger = logging.getLogger('files_sync')


class Seeder(object):

    def __init__(self, files_path, index_path, seqno):
        self._files_path = files_path.rstrip(os.sep)
        self._index_path = index_path
        self._seqno = seqno
        self._index = []
        self._stamp = 0
        self._mutex = coroutine.Lock()

        if exists(self._index_path):
            with file(self._index_path) as f:
                self._index, self._stamp = json.load(f)

        if not exists(self._files_path):
            os.makedirs(self._files_path)

    def pull(self, sequence, packet):
        with self._mutex:
            self._sync()
            packet.header['sequence'] = out_seq = Sequence()
            packet.header['deleted'] = deleted = []
            self._pull(sequence, packet, out_seq, deleted)

    def _pull(self, in_seq, packet, out_seq, deleted):
        pos = 0
        for start, end in in_seq:
            pos = bisect_left(self._index, [start, None, None], pos)
            for pos, (seqno, path, mtime) in enumerate(self._index[pos:]):
                if end is not None and seqno > end:
                    break
                if mtime < 0:
                    deleted.append(path)
                else:
                    packet.push_file(join(self._files_path, path),
                            arcname=join('files', path))
                out_seq.include(start, seqno)
                start = seqno

    def _sync(self):
        if os.stat(self._files_path).st_mtime <= self._stamp:
            return

        _logger.debug('Sync index with %r directory', self._files_path)
        new_files = set()

        # Populate list of new files at first
        for root, __, files in os.walk(self._files_path):
            rel_root = relpath(root, self._files_path)
            if rel_root == '.':
                rel_root = ''
            else:
                rel_root += os.sep
            for filename in files:
                path = join(root, filename)
                if os.lstat(path).st_mtime > self._stamp:
                    new_files.add(rel_root + filename)

        # Check for updates for already tracked files
        tail = []
        for pos, (__, rel_path, mtime) in enumerate(self._index[:]):
            path = join(self._files_path, rel_path)
            existing = lexists(path)
            if existing == (mtime >= 0) and \
                    (not existing or os.lstat(path).st_mtime == mtime):
                continue
            if existing:
                new_files.discard(rel_path)
            pos -= len(tail)
            self._index = self._index[:pos] + self._index[pos + 1:]
            tail.append([
                self._seqno.next(),
                rel_path,
                int(os.lstat(path).st_mtime) if existing else -1,
                ])
        self._index.extend(tail)

        # Finally, add new files
        for rel_path in sorted(new_files):
            mtime = os.lstat(join(self._files_path, rel_path)).st_mtime
            self._index.append([self._seqno.next(), rel_path, mtime])

        self._stamp = os.stat(self._files_path).st_mtime
        if self._seqno.commit():
            with util.new_file(self._index_path) as f:
                json.dump((self._index, self._stamp), f)


class Leecher(object):

    def __init__(self, files_path, sequence_path):
        pass

    def push(self, packet):
        pass

    def pull(self):
        pass
