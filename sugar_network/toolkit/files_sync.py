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
from os.path import join, exists, relpath, lexists, basename

from sugar_network.toolkit.sneakernet import DiskFull
from sugar_network.toolkit.collection import Sequence
from active_toolkit import util, coroutine


_logger = logging.getLogger('files_sync')


class Seeder(object):

    def __init__(self, files_path, index_path, seqno):
        self._files_path = files_path.rstrip(os.sep)
        self._directory = basename(self._files_path)
        self._index_path = index_path
        self._seqno = seqno
        self._index = []
        self._stamp = 0
        # Below calls will mutate `self._index` and trigger coroutine switches.
        # Thus, avoid chnaing `self._index` by different coroutines.
        self._mutex = coroutine.Lock()

        if exists(self._index_path):
            with file(self._index_path) as f:
                self._index, self._stamp = json.load(f)

        if not exists(self._files_path):
            os.makedirs(self._files_path)

    def pull(self, in_seq, packet):
        with self._mutex:
            self._sync()
            orig_seq = Sequence(in_seq)
            out_seq = Sequence()

            try:
                self._pull(in_seq, packet, out_seq, False)
            except DiskFull:
                if out_seq:
                    packet.push(force=True, cmd='files_commit',
                            directory=self._directory, sequence=out_seq)
                raise

            if out_seq:
                orig_seq.floor(out_seq.last)
                packet.push(force=True, cmd='files_commit',
                        directory=self._directory, sequence=orig_seq)

    def pending(self, in_seq):
        with self._mutex:
            self._sync()
            return self._pull(in_seq, None, None, True)

    def _pull(self, in_seq, packet, out_seq, dry_run):
        _logger.debug('Start sync: in_seq=%r', in_seq)

        files = 0
        deleted = 0
        pos = 0

        for start, end in in_seq[:]:
            pos = bisect_left(self._index, [start, None, None], pos)
            for pos, (seqno, path, mtime) in enumerate(self._index[pos:]):
                if end is not None and seqno > end:
                    break
                if dry_run:
                    return True

                coroutine.dispatch()
                if mtime < 0:
                    packet.push(arcname=join('files', path),
                            cmd='files_delete', directory=self._directory,
                            path=path)
                    deleted += 1
                else:
                    packet.push_file(join(self._files_path, path),
                            arcname=join('files', path), cmd='files_push',
                            directory=self._directory, path=path)
                in_seq.exclude(seqno, seqno)
                out_seq.include(start, seqno)
                start = seqno
                files += 1

        if dry_run:
            return False

        _logger.debug('Stop sync: in_seq=%r out_seq=%r updates=%r deletes=%r',
                in_seq, out_seq, files, deleted)

    def _sync(self):
        if os.stat(self._files_path).st_mtime <= self._stamp:
            return

        new_files = set()
        updates = 0
        deletes = 0

        # Populate list of new files at first
        for root, __, files in os.walk(self._files_path):
            coroutine.dispatch()
            rel_root = relpath(root, self._files_path)
            if rel_root == '.':
                rel_root = ''
            else:
                rel_root += os.sep
            for filename in files:
                coroutine.dispatch()
                path = join(root, filename)
                if os.lstat(path).st_mtime > self._stamp:
                    new_files.add(rel_root + filename)

        # Check for updates for already tracked files
        tail = []
        for pos, (__, rel_path, mtime) in enumerate(self._index[:]):
            coroutine.dispatch()
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
            if existing:
                updates += 1
            else:
                deletes += 1
        self._index.extend(tail)

        _logger.debug('Updated %r index: new=%r updates=%r deletes=%r',
                self._files_path, len(self._files_path), updates, deletes)

        # Finally, add new files
        for rel_path in sorted(new_files):
            coroutine.dispatch()
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
