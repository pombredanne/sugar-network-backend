# Copyright (C) 2013 Aleksey Lim
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

"""Persistent pool with temporary files prepared to download."""

import os
import json
import hashlib
import logging
from glob import glob
from os.path import join, splitext, exists

from sugar_network.toolkit import pylru, coroutine, exception


# Maximum numer of postponed pulls master can handle at the same time
_POOL_SIZE = 256
_TAG_SUFFIX = '.tag'

_logger = logging.getLogger('node.downloads')


class Pool(object):

    def __init__(self, root):
        self._pool = pylru.lrucache(_POOL_SIZE, lambda __, dl: dl.pop())
        if not exists(root):
            os.makedirs(root)
        self._root = root

        for tag_path in glob(join(root, '*.tag')):
            path, __ = splitext(tag_path)
            if exists(path):
                try:
                    with file(tag_path) as f:
                        key, tag = json.load(f)
                    pool_key = json.dumps(key)
                    self._pool[pool_key] = _Download(key, tag, path)
                    continue
                except Exception:
                    exception('Cannot open %r download, recreate', tag_path)
                os.unlink(path)
            os.unlink(tag_path)

    def get(self, key):
        key = json.dumps(key)
        if key in self._pool:
            return self._pool[key]

    def set(self, key, tag, fetcher, *args, **kwargs):
        pool_key = json.dumps(key)
        path = join(self._root, hashlib.sha1(pool_key).hexdigest())

        def do_fetch():
            try:
                complete = fetcher(*args, path=path, **kwargs)
            except Exception:
                exception('Error while fetching %r', self)
                if exists(path):
                    os.unlink(path)
                return True
            with file(path + _TAG_SUFFIX, 'w') as f:
                json.dump([key, tag], f)
            return complete

        job = coroutine.spawn(do_fetch)
        dl = self._pool[pool_key] = _Download(key, tag, path, job)
        return dl

    def remove(self, key):
        key = json.dumps(key)
        if key in self._pool:
            self._pool.peek(key).pop()
            del self._pool[key]


class _Download(dict):

    def __init__(self, key, tag, path, job=None):
        self.tag = tag
        self._key = key
        self._path = path
        self._job = job

    def __repr__(self):
        return '<Download %r path=%r>' % (self._key, self._path)

    @property
    def ready(self):
        # pylint: disable-msg=E1101
        return self._job is None or self._job.dead

    @property
    def complete(self):
        return self._job is not None and self._job.value

    @property
    def length(self):
        if exists(self._path):
            return os.stat(self._path).st_size

    def open(self):
        if exists(self._path):
            return file(self._path, 'rb')

    def pop(self):
        if self._job is not None:
            _logger.debug('Abort fetching %r', self)
            self._job.kill()

        if exists(self._path):
            os.unlink(self._path)
        if exists(self._path + _TAG_SUFFIX):
            os.unlink(self._path + _TAG_SUFFIX)

        _logger.debug('Throw out %r from the pool', self)
