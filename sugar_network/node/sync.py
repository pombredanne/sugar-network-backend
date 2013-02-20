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
import cPickle as pickle

from sugar_network.toolkit import util, coroutine, enforce


_EOF = object()

_logger = logging.getLogger('node.sync')


def decode(stream):
    if not hasattr(stream, 'readline'):
        stream.readline = lambda: util.readline(stream)
    record = {}
    while 'commit' not in record:
        record = pickle.load(stream)
        yield record


def encode(*args):
    for sequence in args:
        for record in sequence:
            yield pickle.dumps(record)


def chunked_encode(*args):
    return _ContentOutput(encode(*args))


def diff(volume, in_seq):
    out_seq = util.Sequence([])
    try:
        for document, directory in volume.items():
            coroutine.dispatch()
            directory.commit()
            yield {'document': document}

            for guid, patch in directory.diff(in_seq, out_seq):
                coroutine.dispatch()
                if (yield {'diff': patch, 'guid': guid}) is _EOF:
                    raise StopIteration()
        if out_seq:
            # We processed all documents till `out_seq.last`, thus,
            # it is possible to collapse the sequence to avoid possible holes
            out_seq = [[out_seq.first, out_seq.last]]
    finally:
        yield {'commit': out_seq}


def merge(volume, records, increment_seqno=True):
    directory = None
    merged_seq = util.Sequence()

    for record in records:
        document = record.get('document')
        if document is not None:
            directory = volume[document]
            continue

        patch = record.get('diff')
        if patch is not None:
            enforce(directory is not None,
                    'Invalid merge, no document')
            seqno = directory.merge(record['guid'], patch, increment_seqno)
            if seqno is not None:
                merged_seq.include(seqno, seqno)
            continue

        commit = record.get('commit')
        if commit is not None:
            return commit, merged_seq


class _ContentOutput(object):

    def __init__(self, iterator):
        self._iterator = iterator
        self._buffer = ''
        self._buffer_start = 0
        self._buffer_end = 0

    def read(self, size):
        if self._iterator is None:
            return ''

        def buffer_read():
            result = self._buffer[self._buffer_start:self._buffer_start + size]
            self._buffer_start += size
            return '%X\r\n%s\r\n' % (len(result), result)

        if self._buffer_start < self._buffer_end:
            return buffer_read()

        try:
            self._buffer = next(self._iterator)
        except StopIteration:
            self._iterator = None
            return '0\r\n\r\n'

        self._buffer_start = 0
        self._buffer_end = len(self._buffer)
        return buffer_read()
