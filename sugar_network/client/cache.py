# Copyright (C) 2012-2013 Aleksey Lim
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
import time
import logging
from os.path import exists, basename

from sugar_network import client
from sugar_network.toolkit import pylru, enforce


_POOL_SIZE = 256

_logger = logging.getLogger('cache')


class Cache(object):

    def __init__(self, volume):
        self._volume = volume
        self._pool = None
        self._du = 0
        self._acquired = {}

    def __iter__(self):
        self._ensure_open()
        return iter(self._pool)

    @property
    def du(self):
        return self._du

    def ensure(self, requested_size, temp_size=0):
        self._ensure_open()
        to_free = self._to_free(requested_size, temp_size)
        if to_free <= 0:
            return
        enforce(self._du >= to_free, 'No free disk space')
        for guid, size, mtime in self._reversed_iter():
            self._checkout(guid, (size, mtime))
            to_free -= size
            if to_free <= 0:
                break

    def acquire(self, guid, size):
        self.checkout(guid)
        self._acquired.setdefault(guid, [0, size])[0] += 1
        return guid

    def release(self, *guids):
        for guid in guids:
            acquired = self._acquired.get(guid)
            if acquired is None:
                continue
            acquired[0] -= 1
            if acquired[0] <= 0:
                self.checkin(guid, acquired[1])
                del self._acquired[guid]

    def checkin(self, guid, size):
        self._ensure_open()
        if guid in self._pool:
            self._pool.__getitem__(guid)
            return
        _logger.debug('Checkin %r %d bytes long', guid, size)
        mtime = os.stat(self._volume['implementation'].path(guid)).st_mtime
        self._pool[guid] = (size, mtime)
        self._du += size

    def checkout(self, guid, *args):
        self._ensure_open()
        if guid not in self._pool:
            return False
        _logger.debug('Checkout %r', guid)
        size, __ = self._pool.peek(guid)
        self._du -= size
        del self._pool[guid]
        return True

    def recycle(self):
        self._ensure_open()
        ts = time.time()
        to_free = self._to_free(0, 0)
        for guid, size, mtime in self._reversed_iter():
            if to_free > 0:
                self._checkout(guid, (size, mtime))
                to_free -= size
            elif client.cache_lifetime.value and \
                    client.cache_lifetime.value < (ts - mtime) / 86400.0:
                self._checkout(guid, (size, None))
            else:
                break

    def _ensure_open(self):
        if self._pool is not None:
            return

        _logger.debug('Open implementations pool')

        pool = []
        impls = self._volume['implementation']
        for res in impls.find(not_layer=['local'])[0]:
            meta = res.meta('data')
            if not meta or 'blob_size' not in meta:
                continue
            clone = self._volume['context'].path(res['context'], '.clone')
            if exists(clone) and basename(os.readlink(clone)) == res.guid:
                continue
            pool.append((
                os.stat(impls.path(res.guid)).st_mtime,
                res.guid,
                meta.get('unpack_size') or meta['blob_size'],
                ))

        self._pool = pylru.lrucache(_POOL_SIZE, self._checkout)
        for mtime, guid, size in sorted(pool):
            self._pool[guid] = (size, mtime)
            self._du += size

    def _to_free(self, requested_size, temp_size):
        if not client.cache_limit.value and \
                not client.cache_limit_percent.value:
            return 0

        stat = os.statvfs(client.local_root.value)
        if stat.f_blocks == 0:
            # TODO Sonds like a tmpfs or so
            return 0

        limit = sys.maxint
        free = stat.f_bfree * stat.f_frsize
        if client.cache_limit_percent.value:
            total = stat.f_blocks * stat.f_frsize
            limit = client.cache_limit_percent.value * total / 100
        if client.cache_limit.value:
            limit = min(limit, client.cache_limit.value)
        to_free = max(limit, temp_size) - (free - requested_size)

        if to_free > 0:
            _logger.debug(
                    'Need to recycle %d bytes, '
                    'free_size=%d requested_size=%d temp_size=%d',
                    to_free, free, requested_size, temp_size)
        return to_free

    def _reversed_iter(self):
        i = self._pool.head.prev
        while True:
            while i.empty:
                if i is self._pool.head:
                    return
                i = i.prev
            size, mtime = i.value
            yield i.key, size, mtime
            if i is self._pool.head:
                break
            i = i.next

    def _checkout(self, guid, value):
        size, mtime = value
        if mtime is None:
            _logger.debug('Recycle stale %r to save %s bytes', guid, size)
        else:
            _logger.debug('Recycle %r to save %s bytes', guid, size)
        self._volume['implementation'].delete(guid)
        self._du -= size
        if guid in self._pool:
            del self._pool[guid]
