# -*- coding: utf-8 -*-
#
# Copyright (C) 2012-2014 Aleksey Lim
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
import zlib
import time
import json
import struct
import hashlib
import logging
from types import GeneratorType
from os.path import dirname, exists, join

from sugar_network import toolkit
from sugar_network.toolkit.router import File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine, BUFFER_SIZE, enforce


DEFAULT_COMPRESSLEVEL = 6

_FILENAME_SUFFIX = '.packet'
_RESERVED_DISK_SPACE = 1024 * 1024

_ZLIB_WBITS = 15
_ZLIB_WBITS_SIZE = 32768    # 2 ** 15

_logger = logging.getLogger('packets')


def decode(stream, limit=None):
    _logger.debug('Decode %r stream limit=%r', stream, limit)

    if limit is not None:
        limit -= 2
    magic = stream.read(2)
    enforce(len(magic) == 2, http.BadRequest, 'Malformed packet')
    if magic == '\037\213':
        stream = _ZippedDecoder(stream, limit)
    else:
        stream = _Decoder(magic, stream, limit)
    header = stream.read_record()

    return _DecodeIterator(stream, header)


def encode(items, limit=None, header=None, compresslevel=None,
        on_complete=None, **kwargs):
    _logger.debug('Encode %r limit=%r header=%r', items, limit, header)

    if compresslevel is 0:
        ostream = _Encoder()
    else:
        ostream = _ZippedEncoder(compresslevel)

    # In case of downloading blobs
    # (?) reuse current `this.http`
    this.http = http.Connection()

    if limit is None:
        limit = sys.maxint
    if header is None:
        header = kwargs
    else:
        header.update(kwargs)
    chunk = ostream.write_record(header)
    if chunk:
        yield chunk

    try:
        items = iter(items)
        record = next(items)
        multisegments = type(record) in (tuple, list)

        while True:
            if multisegments:
                packet, props, content = record
                if props is None:
                    props = {}
                props['segment'] = packet
                chunk = ostream.write_record(props)
                if chunk:
                    yield chunk
                if content:
                    content = iter(content)
                    record = next(content)
                else:
                    content = iter([])
                    record = None
            else:
                content = items

            try:
                finalizing = False
                while True:
                    if record is None:
                        finalizing = True
                        record = next(content)
                        continue
                    blob_len = 0
                    if isinstance(record, File):
                        blob_len = record.size
                        chunk = record.meta
                    else:
                        chunk = record
                    chunk = ostream.write_record(chunk,
                            None if finalizing else limit - blob_len)
                    if chunk is None:
                        _logger.debug('Reach the encoding limit')
                        on_complete = None
                        if not isinstance(content, GeneratorType):
                            raise StopIteration()
                        finalizing = True
                        record = content.throw(StopIteration())
                        continue
                    if chunk:
                        yield chunk
                    if blob_len:
                        for chunk in record.iter_content():
                            blob_len -= len(chunk)
                            if not blob_len:
                                chunk += '\n'
                            chunk = ostream.write(chunk)
                            if chunk:
                                yield chunk
                        enforce(blob_len == 0, EOFError, 'Blob size mismatch')
                    record = next(content)
            except StopIteration:
                pass

            if multisegments:
                record = next(items)
                continue
            break
    finally:
        if on_complete is not None:
            on_complete()
        chunk = ostream.flush()
        if chunk:
            yield chunk


def decode_dir(root, recipient=None, session=None):
    for root, __, files in os.walk(root):
        for filename in files:
            if not filename.endswith(_FILENAME_SUFFIX):
                continue
            with file(join(root, filename), 'rb') as packets:
                packet = decode(packets)
                if recipient is not None and packet['from'] == recipient:
                    if session and packet['session'] == session:
                        _logger.debug('Skip the same session %r packet',
                                packets.name)
                    else:
                        _logger.debug('Remove outdated %r packet',
                                packets.name)
                        os.unlink(packets.name)
                    continue
                for i in packet:
                    yield i


def encode_dir(packets, root=None, limit=None, path=None, sender=None,
        header=None):
    if path is None:
        if not exists(root):
            os.makedirs(root)
        path = toolkit.unique_filename(root, toolkit.uuid() + _FILENAME_SUFFIX)
    if limit <= 0:
        stat = os.statvfs(dirname(path))
        limit = stat.f_bfree * stat.f_frsize - _RESERVED_DISK_SPACE
    if header is None:
        header = {}
    if sender is not None:
        header['from'] = sender

    _logger.debug('Creating %r packet limit=%s header=%r', path, limit, header)

    with toolkit.NamedTemporaryFile(dir=dirname(path)) as f:
        for chunk in encode(packets, limit, header):
            f.write(chunk)
            coroutine.dispatch()
        f.flush()
        os.fsync(f.fileno())
        os.rename(f.name, path)


class _DecodeIterator(object):

    def __init__(self, stream, header):
        self._stream = stream
        self.header = header

    def __repr__(self):
        return '<Packet %r>' % self.header

    def __getitem__(self, key):
        return self.header.get(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __iter__(self):
        while True:
            record = self._stream.read_record()
            if record is None:
                break
            if 'segment' in record:
                while record is not None:
                    record.update(self.header)
                    segment = _SegmentIterator(self._stream, record)
                    yield segment
                    record = segment.next_segment
                    if record is not None:
                        continue
                    while True:
                        record = self._stream.read_record()
                        if record is None or 'segment' in record:
                            break
                break
            for i in self._process_record(record):
                yield i

    def _process_record(self, record):
        blob_len = record.get('content-length')
        if blob_len is None:
            yield record
            return

        blob_len = int(blob_len)
        with toolkit.NamedTemporaryFile() as blob:
            digest = hashlib.sha1()
            while blob_len:
                chunk = self._stream.read(min(blob_len, BUFFER_SIZE))
                enforce(chunk, 'Blob size mismatch')
                blob.write(chunk)
                blob_len -= len(chunk)
                digest.update(chunk)
            blob.flush()
            yield File(blob.name, digest=digest.hexdigest(), meta=record)


class _SegmentIterator(_DecodeIterator):

    next_segment = None

    @property
    def name(self):
        return self.header['segment']

    def __iter__(self):
        while True:
            record = self._stream.read_record()
            if record is None:
                break
            if 'segment' in record:
                self.next_segment = record
                break
            for i in self._process_record(record):
                yield i


class _Encoder(object):

    def __init__(self):
        self._offset = 0

    def write_record(self, record, limit=None):
        chunk = json.dumps(record) + '\n'
        if limit is not None and self._offset + len(chunk) > limit:
            return None
        return self.write(chunk)

    def write(self, chunk):
        chunk = self._encode(chunk)
        if chunk:
            self._offset += len(chunk)
        return chunk

    def flush(self):
        chunk = self._flush()
        self._offset += len(chunk)
        return chunk

    def _encode(self, chunk):
        return chunk

    def _flush(self):
        return ''


class _ZippedEncoder(_Encoder):

    def __init__(self, compresslevel=None):
        _Encoder.__init__(self)
        if compresslevel is None:
            compresslevel = DEFAULT_COMPRESSLEVEL
        self._zipper = zlib.compressobj(compresslevel,
                zlib.DEFLATED, -_ZLIB_WBITS, zlib.DEF_MEM_LEVEL, 0)
        self._size = 0
        self._crc = zlib.crc32('') & 0xffffffffL

    def _encode(self, chunk):
        self._size += len(chunk)
        self._crc = zlib.crc32(chunk, self._crc) & 0xffffffffL
        chunk = self._zipper.compress(chunk)
        if self._offset == 0:
            chunk = '\037\213' + '\010' + chr(0) + \
                    struct.pack('<L', long(time.time())) + \
                    '\002' + '\377' + \
                    chunk
            self._offset = _ZLIB_WBITS_SIZE
        return chunk

    def _flush(self):
        return self._zipper.flush() + \
                struct.pack('<L', self._crc) + \
                struct.pack('<L', self._size & 0xffffffffL)


class _Decoder(object):

    def __init__(self, prefix, stream, limit):
        self._buffer = prefix
        self._stream = stream
        self._limit = limit
        self._eof = False

    def read_record(self):
        while True:
            parts = self._buffer.split('\n', 1)
            if len(parts) == 1:
                if self._read(BUFFER_SIZE) and not self._eof:
                    continue
                result = parts[0]
                self._buffer = ''
            else:
                result, self._buffer = parts
            if not result:
                if self._eof:
                    return None
                continue
            return json.loads(result)

    def read(self, size):
        while len(self._buffer) == 0 and self._read(size):
            pass
        size = min(size, len(self._buffer))
        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result

    def _read(self, size):
        if self._limit is not None:
            size = min(size, self._limit)
        chunk = self._stream.read(size)
        if not chunk:
            self._eof = True
        elif self._limit is not None:
            self._limit -= len(chunk)
        return self._decode(chunk)

    def _decode(self, chunk):
        self._buffer += chunk
        return bool(self._buffer)


class _ZippedDecoder(_Decoder):

    def __init__(self, stream, limit):
        _Decoder.__init__(self, '', stream, limit)
        self._unzipper = zlib.decompressobj(-_ZLIB_WBITS)
        self._crc = zlib.crc32('') & 0xffffffffL
        self._size = 0

        if self._limit is not None:
            self._limit -= 8
        enforce(ord(stream.read(1)) == 8, http.BadRequest,
                'Unknown compression method')
        enforce(ord(stream.read(1)) == 0, http.BadRequest,
                'Gzip flags should be empty')
        stream.read(6)  # Ignore the rest of ZIP header

    def _decode(self, chunk):
        if chunk:
            self._add_to_buffer(self._unzipper.decompress(chunk))
            return True
        enforce(len(self._unzipper.unused_data) >= 8, http.BadRequest,
                'Malformed gzipped file')
        crc = struct.unpack('<I', self._unzipper.unused_data[:4])[0]
        enforce(crc == self._crc, http.BadRequest, 'CRC check failed')
        size = struct.unpack('<I', self._unzipper.unused_data[4:8])[0]
        enforce(size == self._size, http.BadRequest, 'Incorrect length')
        return self._add_to_buffer(self._unzipper.flush())

    def _add_to_buffer(self, chunk):
        if not chunk:
            return False
        self._buffer += chunk
        self._crc = zlib.crc32(chunk, self._crc) & 0xffffffffL
        self._size += len(chunk)
        return True
