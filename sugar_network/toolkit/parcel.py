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

_FILENAME_SUFFIX = '.parcel'
_RESERVED_DISK_SPACE = 1024 * 1024

_ZLIB_WBITS = 15
_ZLIB_WBITS_SIZE = 32768    # 2 ** 15

_logger = logging.getLogger('parcel')


def decode(stream, limit=None):
    _logger.debug('Decode %r stream limit=%r', stream, limit)

    stream = _UnzipStream(stream, limit)
    header = stream.read_record()

    packet = _DecodeIterator(stream)
    while True:
        packet.next()
        if packet.name == 'last':
            break
        packet.header.update(header)
        yield packet


def encode(packets, limit=None, header=None, compresslevel=None,
        on_complete=None):
    _logger.debug('Encode %r packets limit=%r header=%r',
            packets, limit, header)

    ostream = _ZipStream(compresslevel)
    # In case of downloading blobs
    # (?) reuse current `this.http`
    this.http = http.Connection()

    if limit is None:
        limit = sys.maxint
    if header is None:
        header = {}
    chunk = ostream.write_record(header)
    if chunk:
        yield chunk

    for packet, props, content in packets:
        if props is None:
            props = {}
        props['packet'] = packet
        chunk = ostream.write_record(props)
        if chunk:
            yield chunk

        if content is None:
            continue

        content = iter(content)
        try:
            finalizing = False
            record = next(content)
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

        if on_complete is not None:
            on_complete()

    chunk = ostream.write_record({'packet': 'last'})
    if chunk:
        yield chunk
    chunk = ostream.flush()
    if chunk:
        yield chunk


def decode_dir(root, recipient=None, session=None):
    for root, __, files in os.walk(root):
        for filename in files:
            if not filename.endswith(_FILENAME_SUFFIX):
                continue
            with file(join(root, filename), 'rb') as parcel:
                for packet in decode(parcel):
                    if recipient is not None and packet['from'] == recipient:
                        if session and packet['session'] == session:
                            _logger.debug('Skip the same session %r parcel',
                                    parcel.name)
                        else:
                            _logger.debug('Remove outdated %r parcel',
                                    parcel.name)
                            os.unlink(parcel.name)
                        break
                    yield packet


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

    _logger.debug('Creating %r parcel limit=%s header=%r', path, limit, header)

    with toolkit.NamedTemporaryFile(dir=dirname(path)) as parcel:
        for chunk in encode(packets, limit, header):
            parcel.write(chunk)
            coroutine.dispatch()
        parcel.flush()
        os.fsync(parcel.fileno())
        os.rename(parcel.name, path)


class _DecodeIterator(object):

    def __init__(self, stream):
        self._stream = stream
        self.header = {}
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
        return '<Packet %r>' % self.header

    def __getitem__(self, key):
        return self.header.get(key)

    def __iter__(self):
        while True:
            record = self._stream.read_record()
            if record is None:
                self._name = None
                raise EOFError()
            if 'packet' in record:
                self._name = record['packet'] or ''
                self.header = record
                self._shift = False
                break
            blob_len = record.get('content-length')
            if blob_len is None:
                yield record
                continue
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class _ZipStream(object):

    def __init__(self, compresslevel=None):
        if compresslevel is None:
            compresslevel = DEFAULT_COMPRESSLEVEL
        self._zipper = zlib.compressobj(compresslevel,
                zlib.DEFLATED, -_ZLIB_WBITS, zlib.DEF_MEM_LEVEL, 0)
        self._offset = 0
        self._size = 0
        self._crc = zlib.crc32('') & 0xffffffffL

    def write_record(self, record, limit=None):
        chunk = json.dumps(record) + '\n'
        if limit is not None and self._offset + len(chunk) > limit:
            return None
        return self.write(chunk)

    def write(self, chunk):
        self._size += len(chunk)
        self._crc = zlib.crc32(chunk, self._crc) & 0xffffffffL
        chunk = self._zipper.compress(chunk)

        if self._offset == 0:
            chunk = '\037\213' + '\010' + chr(0) + \
                    struct.pack('<L', long(time.time())) + \
                    '\002' + '\377' + \
                    chunk
            self._offset = _ZLIB_WBITS_SIZE
        if chunk:
            self._offset += len(chunk)

        return chunk

    def flush(self):
        chunk = self._zipper.flush() + \
                struct.pack('<L', self._crc) + \
                struct.pack('<L', self._size & 0xffffffffL)
        self._offset += len(chunk)
        return chunk


class _UnzipStream(object):

    def __init__(self, stream, limit):
        self._stream = stream
        self._limit = limit
        self._unzipper = zlib.decompressobj(-_ZLIB_WBITS)
        self._crc = zlib.crc32('') & 0xffffffffL
        self._size = 0
        self._buffer = ''

        if self._limit is not None:
            self._limit -= 10
        magic = stream.read(2)
        enforce(magic == '\037\213', http.BadRequest,
                'Not a gzipped file')
        enforce(ord(stream.read(1)) == 8, http.BadRequest,
                'Unknown compression method')
        enforce(ord(stream.read(1)) == 0, http.BadRequest,
                'Gzip flags should be empty')
        stream.read(6)  # Ignore the rest of header

    def read_record(self):
        while True:
            parts = self._buffer.split('\n', 1)
            if len(parts) == 1:
                if self._read(BUFFER_SIZE):
                    continue
                return None
            result, self._buffer = parts
            if not result:
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

        if chunk:
            if self._limit is not None:
                self._limit -= len(chunk)
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
