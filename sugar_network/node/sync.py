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


EOF = object()

_logger = logging.getLogger('node.sync')


def decode(stream):
    packet = _PacketsIterator(stream)
    while True:
        packet.next()
        if packet.name == 'last':
            break
        yield packet


def encode(*args):
    for packet, props, content in args:
        if props is None:
            props = {}
        props['packet'] = packet
        yield pickle.dumps(props)
        for record in content or []:
            yield pickle.dumps(record)
    yield pickle.dumps({'packet': 'last'})


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
                if (yield {'diff': patch, 'guid': guid}) is EOF:
                    raise StopIteration()
        if out_seq:
            # We processed all documents till `out_seq.last`, thus,
            # it is possible to collapse the sequence to avoid possible holes
            out_seq = [[out_seq.first, out_seq.last]]
    finally:
        yield {'commit': out_seq}


def merge(volume, records, shift_seqno=True):
    directory = None
    commit_seq = util.Sequence()
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
            seqno = directory.merge(record['guid'], patch, shift_seqno)
            if seqno is not None:
                merged_seq.include(seqno, seqno)
            continue

        commit = record.get('commit')
        if commit is not None:
            commit_seq.include(commit)
            continue

    return commit_seq, merged_seq


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


class _PacketsIterator(object):

    def __init__(self, stream):
        if not hasattr(stream, 'readline'):
            stream.readline = lambda: util.readline(stream)
        self._stream = stream
        self._props = {}
        self._name = None
        self._shift = True

    @property
    def name(self):
        return self._name

    def next(self):
        if self._shift:
            for __ in self:
                pass
        if self._name is None:
            raise EOFError()
        self._shift = True

    def __getitem__(self, key):
        return self._props.get(key)

    def __iter__(self):
        while True:
            try:
                record = pickle.load(self._stream)
            except EOFError:
                self._name = None
                raise
            packet = record.get('packet')
            if packet:
                self._name = packet
                self._props = record
                self._shift = False
                break
            yield record

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
