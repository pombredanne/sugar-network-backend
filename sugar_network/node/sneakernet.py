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
import tarfile
import logging
import tempfile
from glob import glob
from cStringIO import StringIO
from contextlib import contextmanager
from os.path import join
from gettext import gettext as _

import active_document as ad
from active_toolkit import sockets, util, enforce


_RESERVED_SIZE = 1024 * 1024
_MAX_PACKET_SIZE = 1024 * 1024 * 100
_PACKET_COMPRESS_MODE = 'gz'

_logger = logging.getLogger('node.sneakernet')


def walk(path):
    for path in glob(join(path, '*.packet')):
        with InPacket(path) as packet:
            yield packet


def switch_disk(path):
    return path


class DiskFull(Exception):
    pass


class InPacket(object):

    def __init__(self, path=None, stream=None):
        self._file = None
        self._tarball = None
        self._header = None

        try:
            if stream is None:
                self._file = stream = file(path, 'rb')
            elif not hasattr(stream, 'seek'):
                # tarfile/gzip/zip might require seeking
                self._file = tempfile.TemporaryFile()
                while True:
                    chunk = stream.read(sockets.BUFFER_SIZE)
                    if not chunk:
                        self._file.flush()
                        self._file.seek(0)
                        break
                    self._file.write(chunk)
                stream = self._file

            self._tarball = tarfile.open('r', fileobj=stream)
            with self._extractfile('header') as f:
                self._header = json.load(f)
            enforce(type(self._header) is dict, _('Incorrect header'))
        except Exception, error:
            self.close()
            util.exception()
            raise RuntimeError(_('Malformed packet: %s') % error)

    @property
    def path(self):
        if self._file is None:
            return None
        else:
            return self._file.name

    def __repr__(self):
        return str(self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, key):
        return self._header.get(key)

    def __iter__(self):
        for info in self._tarball:
            if not info.isfile() or info.name == 'header' or \
                    info.name.endswith('.meta'):
                continue

            try:
                with self._extractfile(info.name + '.meta') as f:
                    meta = json.load(f)
            except KeyError:
                _logger.debug('No .meta file for %r', info.name)
                continue

            if meta.get('type') == 'messages':
                with self._extractfile(info) as f:
                    for line in f:
                        item = json.loads(line)
                        item.update(meta)
                        yield item
            elif meta.get('type') == 'blob':
                with self._extractfile(info) as f:
                    meta['blob'] = f
                    yield meta
            else:
                _logger.info(_('Ignore unknown %r record'), meta)

    def close(self):
        if self._tarball is not None:
            self._tarball.close()
            self._tarball = None
        if self._file is not None:
            self._file.close()
            self._file = None

    @contextmanager
    def _extractfile(self, arcname):
        f = self._tarball.extractfile(arcname)
        try:
            yield f
        finally:
            f.close()


class OutPacket(object):

    def __init__(self, packet_type, root=None, stream=None, limit=None,
            **kwargs):
        if not limit:
            limit = _MAX_PACKET_SIZE
        self._limit = min(_MAX_PACKET_SIZE, limit) - _RESERVED_SIZE
        self._stream = None
        self._file = None
        self._tarball = None
        self._header = kwargs
        self._header['type'] = packet_type
        self._path = None
        self._size_to_flush = 0

        if root is not None:
            self._path = join(root, '%s-%s.packet' % (packet_type, ad.uuid()))
            self._file = stream = file(self._path, 'w')
        if stream is None:
            stream = StringIO()

        self._tarball = tarfile.open(
                mode='w:' + _PACKET_COMPRESS_MODE, fileobj=stream)
        self._stream = stream

    @property
    def path(self):
        return self._path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __setitem__(self, key, value):
        self._header[key] = value

    def close(self):
        if self._tarball is not None:
            self._commit()
            self._tarball = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def clear(self):
        if self._tarball is not None:
            self.close()
            self._tarball = None
        if self._file is not None:
            self._file.close()
            os.unlink(self._file.name)
            self._file = None

    @contextmanager
    def push_messages(self, items, **meta):
        if not hasattr(items, 'next'):
            items = iter(items)
        try:
            chunk = json.dumps(items.next())
        except StopIteration:
            return

        meta['type'] = 'messages'

        while chunk is not None:
            self._flush(0, True)
            limit = self._limit - self._stream.tell()
            enforce(limit > 0, DiskFull)

            with tempfile.TemporaryFile() as arcfile:
                while True:
                    limit -= len(chunk)
                    if limit <= 0:
                        break
                    arcfile.write(chunk)
                    arcfile.write('\n')

                    try:
                        chunk = json.dumps(items.next())
                    except StopIteration:
                        chunk = None
                        break

                if not arcfile.tell():
                    enforce(chunk is None, DiskFull)
                    break

                arcfile.seek(0)
                arcname = ad.uuid()
                self._addfile(arcname, arcfile, False)
                self._addfile(arcname + '.meta', meta, True)

    def push_blob(self, stream, **meta):
        meta['type'] = 'blob'
        arcname = ad.uuid()
        self._addfile(arcname, stream, False)
        self._addfile(arcname + '.meta', meta, True)

    def pop_content(self):
        self._commit()
        content = self._stream
        length = content.tell()
        content.seek(0)
        self.close()
        return content, length

    def _commit(self):
        self._addfile('header', self._header, True)
        self._tarball.close()
        self._tarball = None

    def _addfile(self, arcname, data, force):
        info = tarfile.TarInfo(arcname)

        if hasattr(data, 'fileno'):
            info.size = os.fstat(data.fileno()).st_size
            fileobj = data
        else:
            data = json.dumps(data)
            info.size = len(data)
            fileobj = StringIO(data)

        self._flush(info.size, False)
        if not force:
            enforce(self._stream.tell() + info.size < self._limit, DiskFull)

        self._tarball.addfile(info, fileobj=fileobj)

    def _flush(self, size, force):
        if force or self._size_to_flush >= _RESERVED_SIZE:
            self._tarball.fileobj.flush()
            self._size_to_flush = 0
        self._size_to_flush += size
