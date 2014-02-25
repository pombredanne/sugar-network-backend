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
from contextlib import contextmanager
from os.path import exists, abspath, join, isdir, isfile

from sugar_network import toolkit
from sugar_network.toolkit.router import File
from sugar_network.toolkit import http, enforce


_META_SUFFIX = '.meta'

_logger = logging.getLogger('db.blobs')
_root = None


def init(path):
    global _root
    _root = abspath(path)
    if not exists(_root):
        os.makedirs(_root)


def post(content, mime_type=None, digest_to_assert=None):
    meta = []

    @contextmanager
    def write_blob():
        tmp_path = join(_path(), 'post')
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
        path = _path(digest)
        meta.append(('content-type', mime_type or 'application/octet-stream'))
        with toolkit.new_file(path + _META_SUFFIX) as f:
            for key, value in meta:
                f.write('%s: %s\n' % (key, value))
        blob.name = path

    return File(path, digest, meta)


def update(digest, meta):
    path = _path(digest) + _META_SUFFIX
    enforce(exists(path), http.NotFound, 'No such blob')
    meta_content = ''
    for key, value in meta.items():
        meta_content += '%s: %s\n' % (key, value)
    with toolkit.new_file(path) as f:
        f.write(meta_content)


def get(digest):
    path = _path(digest)
    if not exists(path) or not exists(path + _META_SUFFIX):
        return None
    meta = []
    with file(path + _META_SUFFIX) as f:
        for line in f:
            key, value = line.split(':', 1)
            meta.append((key, value.strip()))
    return File(path, digest, meta)


def delete(digest):
    path = _path(digest)
    if exists(path + _META_SUFFIX):
        os.unlink(path + _META_SUFFIX)
    if exists(path):
        os.unlink(path)


def diff(in_seq, out_seq=None):
    if out_seq is None:
        out_seq = toolkit.Sequence([])
    is_the_only_seq = not out_seq

    try:
        root = _path()
        for name in os.listdir(root):
            dirpath = join(root, name)
            if not isdir(dirpath) or os.stat(dirpath).st_ctime not in in_seq:
                continue
            for digest in os.listdir(dirpath):
                if len(digest) != 40:
                    continue
                path = join(dirpath, digest)
                if not isfile(path):
                    continue
                ctime = int(os.stat(path).st_ctime)
                if ctime not in in_seq:
                    continue
                blob = get(digest)
                if blob is None:
                    continue
                yield blob
                out_seq.include(ctime, ctime)
        if is_the_only_seq:
            # There is only one diff, so, we can stretch it to remove all holes
            out_seq.stretch()
    except StopIteration:
        pass


def _path(digest=None):
    enforce(_root is not None, 'Blobs storage is not initialized')
    return join(_root, digest[:3], digest) if digest else _root
