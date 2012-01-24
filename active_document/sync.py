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
import time
import logging
from datetime import datetime
from os.path import exists, join

from active_document import env
from active_document.util import enforce


_PAGE_SIZE = 64

_logger = logging.getLogger('ad.storage')


class NodeSeqno(object):
    """."""

    def __init__(self, metadata):
        self._time = 0
        self._ts = 0

    def next(self):
        cur_time = int(time.mktime(datetime.utcnow().timetuple()))
        if cur_time == self._time:
            self._ts += 1
        else:
            self._time = cur_time
            self._ts = cur_time * 1000
        return self._ts


class Sync(object):

    def __init__(self, document_cls):
        self._document_cls = document_cls
        self._to_send = _Timeline('send')
        self._to_receive = _Timeline('receive')
        self._pending_flush = False

    def create_syn(self):

        def patch():
            for start, end in self._to_send[:]:
                query = {'query': 'seqno:%s..%s' % (start, end or ''),
                         'order_by': 'seqno',
                         'reply': ['seqno'],
                         'limit': _PAGE_SIZE,
                         }
                offset = 0

                while True:
                    documents, total = self._document_cls.find(
                            offset=offset, **query)
                    for i in documents:
                        seqno = i.get('seqno', raw=True)
                        yield seqno, i.guid, i.diff(start, end)
                    offset += _PAGE_SIZE
                    if offset >= total.value:
                        break

                if not total.value:
                    self._to_send.exclude(start, end)
                    self._pending_flush = True

        return self._to_receive, patch()

    def process_ack(self, ack):
        for master_range, node_range in ack:
            self._to_send.exclude(*node_range)
            self._to_receive.exclude(*master_range)
            self._pending_flush = True

    def merge(self, patch):
        seqno_start = None
        seqno_end = None

        for seqno, guid, diff in patch:
            if seqno:
                if not seqno_start or seqno < seqno_start:
                    seqno_start = seqno
                if not seqno_end or seqno > seqno_end:
                    seqno_end = seqno
            self._document_cls(guid).merge(diff)

        if seqno_start:
            self._to_receive.exclude(seqno_start, seqno_end)
            self._pending_flush = True

    def flush(self):
        if not self._pending_flush:
            return
        self._to_send.flush()
        self._to_receive.flush()
        self._pending_flush = False


class _Timeline(list):

    def __init__(self, name):
        self._path = join(env.data_root.value, name + '.timeline')

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
