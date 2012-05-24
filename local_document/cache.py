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
import cgi
import shutil
import logging
import tempfile
from os.path import isdir, exists, dirname, join

from sweets_recipe import Bundle
from active_toolkit.sockets import decode_multipart, BUFFER_SIZE
from local_document import env, http


_logger = logging.getLogger('local_document.cache')


def get_cached_blob(document, guid, prop):
    path = _path(document, guid, prop)
    mime_path = path + '.mime'

    if not exists(path) or not exists(mime_path):
        return None

    with file(mime_path) as f:
        mime_type = f.read().strip()

    if not isdir(path) and os.stat(path).st_size == 0:
        path = None

    return path, mime_type


def get_blob(document, guid, prop):
    cache = get_cached_blob(document, guid, prop)
    if cache is not None:
        return cache

    path = _ensure_path(document, guid, prop)
    mime_path = path + '.mime'

    if isdir(path):
        shutil.rmtree(path)

    response = http.raw_request('GET', [document, guid, prop],
            allow_redirects=True)

    if not exists(dirname(path)):
        os.makedirs(dirname(path))

    mime_type = response.headers.get('Content-Type') or \
            'application/octet-stream'
    with file(mime_path, 'w') as f:
        f.write(mime_type)

    def download(f):
        _logger.debug('Download %s/%s/%s BLOB to %r file',
                document, guid, prop, path)

        length = int(response.headers.get('Content-Length', BUFFER_SIZE))
        chunk_size = min(length, BUFFER_SIZE)
        empty = True

        for chunk in response.iter_content(chunk_size=chunk_size):
            empty = False
            f.write(chunk)

        return not empty

    def download_multipart(stream, size, boundary):
        stream.readline = None
        for filename, content in decode_multipart(stream, size, boundary):
            dst_path = join(path, filename)
            if not exists(dirname(dst_path)):
                os.makedirs(dirname(dst_path))
            shutil.move(content.name, dst_path)

    content_type, params = cgi.parse_header(mime_type)
    if content_type.split('/', 1)[0] == 'multipart':
        try:
            download_multipart(response.raw,
                    int(response.headers['content-length']),
                    params['boundary'])
        except Exception:
            shutil.rmtree(path, ignore_errors=True)
            raise
    elif document == 'implementation' and prop == 'bundle':
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if download(tmp_file):
                tmp_file.close()
                with Bundle(tmp_file.name, 'application/zip') as bundle:
                    bundle.extractall(path)
            else:
                path = None
        finally:
            if exists(tmp_file.name):
                os.unlink(tmp_file.name)
    else:
        with file(path, 'w') as f:
            if not download(f):
                path = None

    return path, mime_type


def set_blob(document, guid, prop, stream,
        mime_type='application/octet-stream'):
    path = _ensure_path(document, guid, prop)
    mime_path = path + '.mime'

    with file(mime_path, 'w') as f:
        f.write(mime_type)

    with file(path, 'wb') as f:
        while True:
            chunk = stream.read(BUFFER_SIZE)
            if not chunk:
                break
            f.write(chunk)


def _path(document, guid, *args):
    return join(env.local_root.value, 'cache', document, guid[:2], guid, *args)


def _ensure_path(document, guid, *args):
    return env.ensure_path('cache', document, guid[:2], guid, *args)
