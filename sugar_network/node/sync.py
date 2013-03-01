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

import os
import gzip
import json
import logging
from types import GeneratorType
from os.path import exists, join, dirname

from sugar_network import db
from sugar_network.toolkit import BUFFER_SIZE, coroutine, util, enforce


# Leave at leat n bytes in fs whle calling `encode_to_file()`
_SNEAKERNET_RESERVED_SIZE = 1024 * 1024
_SNEAKERNET_SUFFIX = '.package'

_logger = logging.getLogger('node.sync')


def decode(stream):
    packet = _PacketsIterator(stream)
    while True:
        packet.next()
        if packet.name == 'last':
            break
        yield packet


def encode(*packets, **header):
    return _encode(None, packets, header, _EncodingStatus())


def limited_encode(limit, *packets, **header):
    return _encode(limit, packets, header, _EncodingStatus())


def chunked_encode(*packets, **header):
    return _ChunkedEncoder(encode(*packets, **header))


def sneakernet_decode(root, node=None, session=None):
    for root, __, files in os.walk(root):
        for filename in files:
            if not filename.endswith(_SNEAKERNET_SUFFIX):
                continue
            zipfile = gzip.open(join(root, filename), 'rb')
            try:
                package_props = json.loads(zipfile.readline())

                if node is not None and package_props.get('src') == node:
                    if package_props.get('session') == session:
                        _logger.debug('Skip session %r sneakernet package',
                                zipfile.name)
                    else:
                        _logger.debug('Remove outdate %r sneakernet package',
                                zipfile.name)
                        os.unlink(zipfile.name)
                    continue

                for packet in decode(zipfile):
                    packet.props.update(package_props)
                    yield packet
            finally:
                zipfile.close()


def sneakernet_encode(packets, root=None, limit=None, path=None, **header):
    if path is None:
        if not exists(root):
            os.makedirs(root)
        path = util.unique_filename(root, db.uuid() + _SNEAKERNET_SUFFIX)

    if limit <= 0:
        stat = os.statvfs(dirname(path))
        limit = stat.f_bfree * stat.f_frsize - _SNEAKERNET_RESERVED_SIZE

    _logger.debug('Creating %r sneakernet package, limit=%s header=%r',
            path, limit, header)

    status = _EncodingStatus()
    with file(path, 'wb') as package:
        zipfile = gzip.GzipFile(fileobj=package)
        try:
            json.dump(header, zipfile)
            zipfile.write('\n')

            pos = None
            encoder = _encode(limit, packets, None, status)
            while True:
                try:
                    chunk = encoder.send(pos)
                    zipfile.write(chunk)
                    pos = zipfile.fileobj.tell()
                    coroutine.dispatch()
                except StopIteration:
                    break

        except Exception:
            _logger.debug('Emergency removing %r package', path)
            package.close()
            os.unlink(path)
            raise
        else:
            zipfile.close()
            package.flush()
            os.fsync(package.fileno())

    return not status.aborted


class _EncodingStatus(object):

    aborted = False


def _encode(limit, packets, header, status):
    for packet, props, content in packets:
        if status.aborted:
            break

        if props is None:
            props = {}
        if header:
            props.update(header)
        props['packet'] = packet
        pos = (yield json.dumps(props) + '\n') or 0

        if content is None:
            continue

        content = iter(content)
        try:
            record = next(content)
            while True:
                blob = None
                blob_size = 0
                if 'blob' in record:
                    blob = record.pop('blob')
                    blob_size = record['blob_size'] = os.stat(blob).st_size

                dump = json.dumps(record) + '\n'
                if not status.aborted and limit is not None and \
                        pos + len(dump) + blob_size > limit:
                    status.aborted = True
                    if not isinstance(content, GeneratorType):
                        raise StopIteration()
                    record = content.throw(StopIteration())
                    continue
                pos = (yield dump) or 0

                if blob is not None:
                    with file(blob, 'rb') as f:
                        while blob_size:
                            chunk = f.read(min(BUFFER_SIZE, blob_size))
                            enforce(chunk, EOFError)
                            pos = (yield chunk) or 0
                            blob_size -= len(chunk)

                record = next(content)
        except StopIteration:
            pass

    yield json.dumps({'packet': 'last'}) + '\n'


class _ChunkedEncoder(object):

    def __init__(self, encoder):
        self._encoder = encoder
        self._buffer = ''
        self._buffer_start = 0
        self._buffer_end = 0

    def read(self, size):
        if self._encoder is None:
            return ''

        def buffer_read():
            result = self._buffer[self._buffer_start:self._buffer_start + size]
            self._buffer_start += size
            return '%X\r\n%s\r\n' % (len(result), result)

        if self._buffer_start < self._buffer_end:
            return buffer_read()

        try:
            self._buffer = next(self._encoder)
        except StopIteration:
            self._encoder = None
            return '0\r\n\r\n'

        self._buffer_start = 0
        self._buffer_end = len(self._buffer)
        return buffer_read()


class _PacketsIterator(object):

    def __init__(self, stream):
        if not hasattr(stream, 'readline'):
            stream.readline = lambda: util.readline(stream)
        self._stream = stream
        self.props = {}
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

    def __repr__(self):
        return '<SyncPacket %r>' % self.props

    def __getitem__(self, key):
        return self.props.get(key)

    def __iter__(self):
        blob = None
        while True:
            if blob is not None and blob.size_to_read:
                self._stream.seek(blob.size_to_read, 1)
                blob = None
            record = self._stream.readline()
            if not record:
                self._name = None
                raise EOFError()
            record = json.loads(record)
            packet = record.get('packet')
            if packet:
                self._name = packet
                self.props = record
                self._shift = False
                break
            blob_size = record.get('blob_size')
            if blob_size:
                blob = record['blob'] = _Blob(self._stream, blob_size)
            yield record

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class _Blob(object):

    def __init__(self, stream, size):
        self._stream = stream
        self.size_to_read = size

    def read(self, size=BUFFER_SIZE):
        chunk = self._stream.read(min(size, self.size_to_read))
        self.size_to_read -= len(chunk)
        return chunk
