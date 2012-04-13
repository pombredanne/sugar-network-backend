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
import shutil
import logging
import tempfile
from os.path import isdir, exists, dirname, join

from sweets_recipe import Bundle

from sugar_network import sugar, env, http


_CHUNK_SIZE = 1024 * 10

_logger = logging.getLogger('client')
_missed_blobs = set()


def get_blob(resource, guid, prop):
    path = _path(resource, guid, prop)
    mime_path = path + '.mime'

    if exists(path) and exists(mime_path):
        with file(mime_path) as f:
            mime_type = f.read().strip()
        return path, mime_type

    if guid in _missed_blobs:
        return None, None

    if isdir(path):
        shutil.rmtree(path)

    response = http.raw_request('GET', [resource, guid, prop],
            allow_redirects=True)

    if not exists(dirname(path)):
        os.makedirs(dirname(path))

    mime_type = response.headers.get('Content-Type') or \
            'application/octet-stream'
    with file(mime_path, 'w') as f:
        f.write(mime_type)

    def download(f):
        _logger.debug('Download "%s" BLOB', path)

        length = int(response.headers.get('Content-Length', _CHUNK_SIZE))
        chunk_size = min(length, _CHUNK_SIZE)
        empty = True

        for chunk in response.iter_content(chunk_size=chunk_size):
            empty = False
            f.write(chunk)

        if empty:
            _missed_blobs.add(guid)
            os.unlink(f.name)
        else:
            return True

    if resource == 'implementation' and prop == 'bundle':
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if download(tmp_file):
                tmp_file.close()
                with Bundle(tmp_file.name, 'application/zip') as bundle:
                    bundle.extractall(path)
            else:
                return None, None
        finally:
            os.unlink(tmp_file.name)
    else:
        with file(path, 'w') as f:
            if not download(f):
                return None, None

    return path, mime_type


def _path(resource, guid, *args):
    cachedir = env.cachedir.value
    if not cachedir:
        cachedir = sugar.profile_path('cache')
    return join(cachedir, resource, guid[:2], guid, *args)
