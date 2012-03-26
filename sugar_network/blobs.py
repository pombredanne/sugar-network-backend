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
import json
import shutil
import logging
import tempfile
from os.path import isdir, exists

from sweets_recipe import Bundle

from sugar_network import sugar
from sugar_network.request import request, raw_request


_CHUNK_SIZE = 1024 * 10

_logger = logging.getLogger('client')


class Blob(object):

    def __init__(self, path):
        self._path = path

    @property
    def content(self):
        """Return entire BLOB value as a string."""
        path, mime_path = self._get()
        with file(mime_path) as f:
            mime_type = f.read().strip()
        with file(path) as f:
            if mime_type == 'application/json':
                return json.load(f)
            else:
                return f.read()

    @property
    def path(self):
        """Return file-system path to file that contain BLOB value."""
        path, __ = self._get()
        return path

    def iter_content(self):
        """Return BLOB value by poritons.

        :returns:
            generator that returns BLOB value by chunks

        """
        path, __ = self._get()
        with file(path) as f:
            while True:
                chunk = f.read(_CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    def _set_url(self, url):
        request('PUT', self._path, params={'url': url})

    #: Set BLOB value by url
    url = property(None, _set_url)

    def _get(self):
        path = sugar.profile_path('cache', 'blobs', *self._path)
        mime_path = path + '.mime'

        if exists(path) and exists(mime_path):
            return path, mime_path

        if isdir(path):
            shutil.rmtree(path)

        response = raw_request('GET', self._path, allow_redirects=True)

        with file(mime_path, 'w') as f:
            f.write(response.headers.get('Content-Type') or \
                    'application/octet-stream')

        def download(f):
            _logger.debug('Download "%s" BLOB to %s', self._path, path)

            length = int(response.headers.get('Content-Length', _CHUNK_SIZE))
            chunk_size = min(length, _CHUNK_SIZE)

            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

        if self._path[0] == 'implementation' and self._path[-1] == 'bundle':
            tmp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                download(tmp_file)
                tmp_file.close()
                with Bundle(tmp_file.name, 'application/zip') as bundle:
                    bundle.extractall(path)
            finally:
                os.unlink(tmp_file.name)
        else:
            with file(path, 'w') as f:
                download(f)

        return path, mime_path
