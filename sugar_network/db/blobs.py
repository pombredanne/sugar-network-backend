# Copyright (C) 2014 Aleksey Lim
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
import logging
import hashlib
import mimetypes
from contextlib import contextmanager
from os.path import exists, abspath, join, dirname

from sugar_network import toolkit
from sugar_network.toolkit.router import File
from sugar_network.toolkit import http, ranges, enforce


_META_SUFFIX = '.meta'

_logger = logging.getLogger('db.blobs')


class Blobs(object):

    def __init__(self, root, seqno):
        self._root = abspath(root)
        self._seqno = seqno

    def path(self, *args):
        if len(args) == 1 and len(args[0]) == 40 and '.' not in args[0]:
            return self._blob_path(args[0])
        else:
            return join(self._root, 'files', *args)

    def post(self, content, mime_type=None, digest_to_assert=None, meta=None):
        if meta is None:
            meta = []
            meta.append(('content-type',
                mime_type or 'application/octet-stream'))
        else:
            meta = meta.items()
            if mime_type:
                meta.append(('content-type', mime_type))

        @contextmanager
        def write_blob():
            tmp_path = join(self._blob_path(), 'post')
            if hasattr(content, 'read'):
                with toolkit.new_file(tmp_path) as blob:
                    digest = hashlib.sha1()
                    while True:
                        chunk = content.read(toolkit.BUFFER_SIZE)
                        if not chunk:
                            break
                        blob.write(chunk)
                        digest.update(chunk)
                    yield blob, digest.hexdigest()
            elif isinstance(content, dict):
                enforce('location' in content, http.BadRequest, 'No location')
                enforce('digest' in content, http.BadRequest, 'No digest')
                meta.append(('status', '301 Moved Permanently'))
                meta.append(('location', content['location']))
                with toolkit.new_file(tmp_path) as blob:
                    yield blob, content['digest']
            else:
                with toolkit.new_file(tmp_path) as blob:
                    blob.write(content)
                    yield blob, hashlib.sha1(content).hexdigest()

        with write_blob() as (blob, digest):
            if digest_to_assert and digest != digest_to_assert:
                blob.unlink()
                raise http.BadRequest('Digest mismatch')
            path = self._blob_path(digest)
            seqno = self._seqno.next()
            meta.append(('content-length', str(blob.tell())))
            meta.append(('x-seqno', str(seqno)))
            _write_meta(path, meta, seqno)
            blob.name = path
        os.utime(path, (seqno, seqno))

        _logger.debug('Post %r file', path)

        return File(path, digest, meta)

    def update(self, path, meta):
        path = self.path(path)
        enforce(exists(path), http.NotFound, 'No such blob')
        orig_meta = _read_meta(path)
        orig_meta.update(meta)
        _write_meta(path, orig_meta, None)

    def get(self, digest):
        path = self.path(digest)
        if exists(path + _META_SUFFIX):
            return File(path, digest, _read_meta(path))

    def delete(self, path):
        self._delete(path, None)

    def diff(self, r, path=None, recursive=True):
        if path is None:
            is_files = False
            root = self._blob_path()
        else:
            path = path.strip('/').split('/')
            enforce(not [i for i in path if i == '..'],
                    http.BadRequest, 'Relative paths are not allowed')
            is_files = True
            root = self.path(*path)
        checkin_seqno = None

        for root, __, files in os.walk(root):
            if not ranges.contains(r, int(os.stat(root).st_mtime)):
                continue
            rel_root = root[len(self._root) + 7:] if is_files else None
            for filename in files:
                path = join(root, filename)
                if filename.endswith(_META_SUFFIX):
                    seqno = int(os.stat(path).st_mtime)
                    path = path[:-len(_META_SUFFIX)]
                    meta = None
                    if exists(path):
                        stat = os.stat(path)
                        if seqno != int(stat.st_mtime):
                            _logger.debug('Found updated %r file', path)
                            seqno = self._seqno.next()
                            meta = _read_meta(path)
                            meta['x-seqno'] = str(seqno)
                            meta['content-length'] = str(stat.st_size)
                            _write_meta(path, meta, seqno)
                            os.utime(path, (seqno, seqno))
                    if not ranges.contains(r, seqno):
                        continue
                    if meta is None:
                        meta = _read_meta(path)
                    if is_files:
                        digest = join(rel_root, filename[:-len(_META_SUFFIX)])
                        meta['path'] = digest
                    else:
                        digest = filename[:-len(_META_SUFFIX)]
                elif not is_files or exists(path + _META_SUFFIX):
                    continue
                else:
                    _logger.debug('Found new %r file', path)
                    mime_type = mimetypes.guess_type(filename)[0] or \
                            'application/octet-stream'
                    if checkin_seqno is None:
                        checkin_seqno = self._seqno.next()
                    seqno = checkin_seqno
                    meta = [('content-type', mime_type),
                            ('content-length', str(os.stat(path).st_size)),
                            ('x-seqno', str(seqno)),
                            ]
                    _write_meta(path, meta, seqno)
                    os.utime(path, (seqno, seqno))
                    if not ranges.contains(r, seqno):
                        continue
                    digest = join(rel_root, filename)
                    meta.append(('path', digest))
                yield File(path, digest, meta)
            if not recursive:
                break

    def patch(self, patch, seqno):
        if 'path' in patch:
            path = self.path(patch.pop('path'))
        else:
            path = self._blob_path(patch.digest)
        if not patch.size:
            self._delete(path, seqno)
            return
        if not exists(dirname(path)):
            os.makedirs(dirname(path))
        os.rename(patch.path, path)
        if exists(path + _META_SUFFIX):
            meta = _read_meta(path)
            meta.update(patch)
        else:
            meta = patch
        meta['x-seqno'] = str(seqno)
        _write_meta(path, meta, seqno)
        os.utime(path, (seqno, seqno))

    def _delete(self, path, seqno):
        path = self.path(path)
        if exists(path + _META_SUFFIX):
            if seqno is None:
                seqno = self._seqno.next()
            meta = _read_meta(path)
            meta['status'] = '410 Gone'
            meta['x-seqno'] = str(seqno)
            _write_meta(path, meta, seqno)
        if exists(path):
            _logger.debug('Delete %r file', path)
            os.unlink(path)

    def _blob_path(self, digest=None):
        if not digest:
            return join(self._root, 'blobs')
        return join(self._root, 'blobs', digest[:3], digest)


def _write_meta(path, meta, seqno):
    path += _META_SUFFIX
    with toolkit.new_file(path) as f:
        for key, value in meta.items() if isinstance(meta, dict) else meta:
            if seqno is None and key == 'x-seqno':
                seqno = int(value)
            f.write('%s: %s\n' % (key, value))
    os.utime(path, (seqno, seqno))


def _read_meta(path):
    meta = {}
    with file(path + _META_SUFFIX) as f:
        for line in f:
            key, value = line.split(':', 1)
            meta[key] = value.strip()
    return meta
