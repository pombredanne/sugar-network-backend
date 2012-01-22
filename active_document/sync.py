# Copyright (C) 2012, Aleksey Lim
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
from os.path import exists

from active_document.util import enforce


_logger = logging.getLogger('ad.storage')


class Seqno(object):
    """."""

    def __init__(self, metadata):
        """
        :param name:
            document name

        """
        self.metadata = metadata
        self._root = metadata.ensure_path('')

    def next(self):
        return 1


class Synchronizer(object):

    def __init__(self, metadata):
        self.metadata = metadata

    def create_syn(self):
        return None, None

    def process_ack(self, ack):
        pass

    def merge(self, row):
        pass


class _Timeline(list):

    def __init__(self, path):
        self._path = path

        if exists(self._path):
            f = file(self._path)
            self.extend(json.load(f))
            f.close()
        else:
            self.append([1, None])

    def exclude(self, exclude_start, exclude_end=None):
        if exclude_end is None:
            exclude_end = exclude_start
        enforce(exclude_start <= exclude_end and exclude_start > 0)

        for i, interval in enumerate(self):
            start, end = interval
            if end is not None and end < exclude_start:
                # Current `interval` is below than new one
                continue

            if end is None or end > exclude_end:
                # Current `interval` will exist after changing
                self[i] = [exclude_end + 1, end]
                if start < exclude_start:
                    self.insert(i, [start, exclude_start - 1])
            else:
                if start < exclude_start:
                    self[i] = [start, exclude_start - 1]
                else:
                    del self[i]

            if end is not None:
                exclude_start = end + 1
                if exclude_start < exclude_end:
                    self.exclude(exclude_start, exclude_end)
            break

    def flush(self):
        f = file(self._path, 'w')
        json.dump(self, f)
        f.flush()
        os.fsync(f.fileno())
        f.close()
