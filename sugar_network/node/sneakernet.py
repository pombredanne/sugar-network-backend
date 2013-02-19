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
import time
import gzip
import tarfile
import logging
from cStringIO import StringIO
from contextlib import contextmanager
from os.path import join, exists

from sugar_network import db
from sugar_network.toolkit import BUFFER_SIZE, util, exception, enforce


_RESERVED_SIZE = 1024 * 1024
_MAX_PACKET_SIZE = 1024 * 1024 * 100
_PACKET_COMPRESS_MODE = 'gz'
_RECORD_SUFFIX = '.record'
_PACKET_SUFFIX = '.packet'

_logger = logging.getLogger('sneakernet')


def walk(path):
    for root, __, files in os.walk(path):
        for filename in files:
            if not filename.endswith(_PACKET_SUFFIX):
                continue
            with InPacket(join(root, filename)) as packet:
                yield packet


class DiskFull(Exception):
    pass


class InPacket(object):

    def __init__(self, path=None, stream=None):
        self._file = None
        self._tarball = None
        self.header = {}
        self.content_type = None

        try:
            if stream is None:
                self._file = stream = file(path, 'rb')
            elif not hasattr(stream, 'seek'):
                # tarfile/gzip/zip might require seeking
                self._file = util.NamedTemporaryFile()

                if hasattr(stream, 'read'):
                    while True:
                        chunk = stream.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        self._file.write(chunk)
                else:
                    for chunk in stream:
                        self._file.write(chunk)

                self._file.flush()
                self._file.seek(0)
                stream = self._file

            self._tarball = tarfile.open('r', fileobj=stream)
            with self._extract('header') as f:
                self.header = json.load(f)
            enforce(type(self.header) is dict, 'Incorrect header')

            if isinstance(self._tarball.fileobj, file):
                self.content_type = 'application/x-tar'
            elif isinstance(self._tarball.fileobj, gzip.GzipFile):
                self.content_type = 'application/x-compressed-tar'
            else:
                self.content_type = 'application/x-bzip-compressed-tar'

        except Exception, error:
            self.close()
            exception()
            raise RuntimeError('Malformed %r packet: %s' % (self, error))

        _logger.trace('Reading %r input packet', self)

    @property
    def path(self):
        if self._file is not None:
            return self._file.name

    def records(self, **filters):
        for info in self._tarball:
            if not info.isfile():
                continue

            if info.name.endswith(_PACKET_SUFFIX):
                _logger.trace('Reading %r sub packet from %r', info.name, self)
                with self._extract(info) as f:
                    with InPacket(stream=f) as sub_packet:
                        for sub_record in sub_packet.records(**filters):
                            yield sub_record
                continue
            elif not info.name.endswith(_RECORD_SUFFIX):
                continue

            with self._extract(info) as f:
                meta = json.load(f)
            meta.update(self.header)

            skip = False
            for key, value in filters.items():
                if meta.get(key) != value:
                    skip = True
                    break
            if skip:
                continue

            if meta.get('content_type') == 'records':
                _logger.trace('Reading %r records from %r', info.name, self)
                with self._extract(info.name[: - len(_RECORD_SUFFIX)]) as f:
                    for line in f:
                        item = json.loads(line)
                        item.update(meta)
                        yield item
            elif meta.get('content_type') == 'blob':
                _logger.trace('Reading %r blob from %r', info.name, self)
                with self._extract(info.name[: - len(_RECORD_SUFFIX)]) as f:
                    meta['blob'] = f
                    yield meta
            else:
                yield meta

    def close(self):
        if self._tarball is not None:
            self._tarball.close()
            self._tarball = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def __repr__(self):
        header = ['%s=%r' % i for i in self.header.items()]
        return '<InPacket %s>' % (' '.join(['path=%r' % self.path] + header))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        return self.records()

    @contextmanager
    def _extract(self, arcname):
        f = self._tarball.extractfile(arcname)
        try:
            yield f
        finally:
            f.close()


class OutPacket(object):

    def __init__(self, root=None, stream=None, limit=None, compress_mode=None,
            seqno=None, filename=None, **kwargs):
        if compress_mode is None:
            compress_mode = _PACKET_COMPRESS_MODE

        self._stream = None
        self._file = None
        self._path = None
        self._tarball = None
        self.header = kwargs
        self._size_to_flush = 0
        self._file_num = 0
        self._empty = True

        if filename:
            self._basename = filename
        else:
            if 'src' in kwargs:
                self._basename = kwargs['src']
                if seqno is not None:
                    self._basename += '-%s' % seqno
            else:
                self._basename = db.uuid()
            self._basename += _PACKET_SUFFIX
        kwargs['filename'] = self._basename

        if root is not None:
            if not exists(root):
                os.makedirs(root)
            self._path = util.unique_filename(root, self._basename)
            self._file = stream = file(self._path, 'wb+')
        elif hasattr(stream, 'fileno'):
            self._file = stream
            self._path = stream.name
        else:
            limit = limit or _MAX_PACKET_SIZE
        self._limit = limit

        enforce(stream is not None)
        self._tarball = tarfile.open(mode='w:' + compress_mode, fileobj=stream)
        self._stream = stream

        if compress_mode == 'gz':
            self.content_type = 'application/x-compressed-tar'
        elif compress_mode == 'bz2':
            self.content_type = 'application/x-bzip-compressed-tar'
        else:
            self.content_type = 'application/x-tar'

        _logger.trace('Writing %r output packet', self)

    @property
    def basename(self):
        return self._basename

    @property
    def path(self):
        return self._path or self._basename

    @property
    def closed(self):
        return self._tarball is None

    @property
    def empty(self):
        return self._empty

    def __repr__(self):
        header = ['%s=%r' % i for i in self.header.items()]
        return '<OutPacket %s>' % (' '.join(['path=%r' % self.path] + header))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            if exc_type is not DiskFull:
                self.clear()
        self.close()

    def close(self, clear=False):
        if self.empty:
            clear = True
        self._commit(clear)
        if self._file is not None:
            if not clear:
                self._file.flush()
                os.fsync(self._file.fileno())
            self._file.close()
            if clear:
                if exists(self._file.name):
                    os.unlink(self._file.name)
            self._file = None

    def clear(self):
        self.close(clear=True)

    def pop(self):
        self._commit(False)
        if self._file is not None:
            stream = self._file
            self._file = None
        else:
            stream = self._stream
        self._stream = None
        if stream is not None:
            stream.seek(0)
        return stream

    def push(self, data=None, arcname=None, force=False, **meta):
        if isinstance(data, OutPacket):
            _logger.trace('Writing %r sub packet to %r', data.path, self)
            if not arcname:
                arcname = data.basename
            f = data.pop()
            try:
                self._addfile(arcname, f, force)
            finally:
                f.close()
            return
        elif data is None:
            _logger.trace('Writing %r record to %r', meta, self)
            self._add(arcname, None, meta)
            return
        elif hasattr(data, 'fileno'):
            _logger.trace('Writing %r blob to %r', data.name, self)
            meta['content_type'] = 'blob'
            self._add(arcname, data, meta)
            return

        _logger.trace('Writing %r records to %r', data, self)

        if not hasattr(data, 'next'):
            data = iter(data)
        try:
            chunk = json.dumps(data.next())
        except StopIteration:
            return

        meta['content_type'] = 'records'

        while chunk is not None:
            self._flush(0, True)
            limit = self._enforce_limit()

            with util.NamedTemporaryFile() as arcfile:
                while True:
                    limit -= len(chunk)
                    if limit <= 0:
                        break
                    arcfile.write(chunk)
                    arcfile.write('\n')

                    try:
                        chunk = json.dumps(data.next())
                    except StopIteration:
                        chunk = None
                        break

                if not arcfile.tell():
                    if chunk is not None:
                        _logger.trace('Reach size limit for %r packet', self)
                        raise DiskFull()
                    break

                arcfile.seek(0)
                self._add(arcname, arcfile, meta)

    def push_file(self, path_, **kwargs):
        with file(path_, 'rb') as f:
            self.push(f, **kwargs)

    def _add(self, arcname, data, meta):
        if not arcname:
            self._file_num += 1
            arcname = '%08d' % self._file_num
        if data is not None:
            self._addfile(arcname, data, False)
        self._addfile(arcname + _RECORD_SUFFIX, meta, True)

    def _addfile(self, arcname, data, force):
        info = tarfile.TarInfo(arcname)
        info.mtime = time.time()

        if hasattr(data, 'fileno'):
            info.size = os.fstat(data.fileno()).st_size
            fileobj = data
        elif hasattr(data, 'seek'):
            data.seek(0, 2)
            info.size = data.tell()
            data.seek(0)
            fileobj = data
        else:
            data = json.dumps(data)
            info.size = len(data)
            fileobj = StringIO(data)

        self._flush(info.size, False)
        if not force:
            self._enforce_limit(info.size)

        self._tarball.addfile(info, fileobj=fileobj)
        self._empty = False

    def _flush(self, size, force):
        if force or self._size_to_flush >= _RESERVED_SIZE:
            self._tarball.fileobj.flush()
            self._size_to_flush = 0
        self._size_to_flush += size

    def _enforce_limit(self, size=0):
        if self._limit is None:
            stat = os.statvfs(self.path)
            free = stat.f_bfree * stat.f_frsize
        else:
            free = self._limit - self._stream.tell()
        free -= _RESERVED_SIZE
        if free - size <= 0:
            _logger.trace('Reach size limit for %r packet', self)
            raise DiskFull()
        return free

    def _commit(self, clear):
        if self._tarball is None:
            return
        _logger.trace('Closing %r output packet, clear=%r', self, clear)
        if not clear:
            self._addfile('header', self.header, True)
        self._tarball.close()
        self._tarball = None
        self._empty = True


class OutFilePacket(OutPacket):

    def __init__(self, root=None, **kwargs):
        stream = None
        if root is None:
            stream = util.NamedTemporaryFile()
        OutPacket.__init__(self, root=root, stream=stream, **kwargs)


class OutBufferPacket(OutPacket):

    def __init__(self, **kwargs):
        OutPacket.__init__(self, root=None, stream=StringIO(), **kwargs)
