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
import json
import time
import shutil
import logging
from os.path import exists, join, isdir

from sugar_network import toolkit, client
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import pipe, enforce


_logger = logging.getLogger('cache')


def recycle():
    stat = os.statvfs(client.local_root.value)
    total = stat.f_blocks * stat.f_frsize
    free = stat.f_bfree * stat.f_frsize
    to_free = client.cache_limit.value * total / 100 - free
    ts = time.time()

    __, items = _list()
    for mtime, neg_size, path in items:
        if to_free > 0:
            shutil.rmtree(path, ignore_errors=True)
            _logger.debug('Recycled %r to save %s bytes', path, -neg_size)
            to_free += neg_size
        elif mtime == 0:
            shutil.rmtree(path, ignore_errors=True)
            _logger.debug('Recycled malformed cache item %r', path)
        elif client.cache_lifetime.value and \
                client.cache_lifetime.value < (ts - mtime) / 86400.0:
            shutil.rmtree(path, ignore_errors=True)
            _logger.debug('Recycled stale %r to get %s bytes', path, -neg_size)
        else:
            break


def ensure(requested_size=0, temp_size=0):
    stat = os.statvfs(client.local_root.value)
    if stat.f_blocks == 0:
        # TODO Sonds like a tmpfs or so
        return
    total = stat.f_blocks * stat.f_frsize
    free = stat.f_bfree * stat.f_frsize

    to_free = max(client.cache_limit.value * total / 100, temp_size) - \
            (free - requested_size)
    if to_free <= 0:
        return

    _logger.debug('Recycle %s bytes free=%d requested_size=%d temp_size=%d',
            to_free, free, requested_size, temp_size)

    cached_total, items = _list()
    enforce(cached_total >= to_free, 'No free disk space')

    for __, neg_size, path in items:
        shutil.rmtree(path, ignore_errors=True)
        _logger.debug('Recycled %r to save %s bytes', path, -neg_size)
        to_free += neg_size
        if to_free <= 0:
            break


def get(guid, hints=None):
    path = join(client.local_root.value, 'cache', 'implementation', guid)
    if exists(path):
        pipe.trace('Reuse cached %s implementation from %r', guid, path)
        ts = time.time()
        os.utime(path, (ts, ts))
        return path

    pipe.trace('Download %s implementation', guid)
    # TODO Per download progress
    pipe.feedback('download')

    ensure(hints.get('unpack_size') or 0, hints.get('bundle_size') or 0)
    blob = client.IPCConnection().download(['implementation', guid, 'data'])
    _unpack_stream(blob, path)
    with toolkit.new_file(join(path, '.unpack_size')) as f:
        json.dump(hints.get('unpack_size') or 0, f)

    topdir = os.listdir(path)[-1:]
    if topdir:
        for exec_dir in ('bin', 'activity'):
            bin_path = join(path, topdir[0], exec_dir)
            if not exists(bin_path):
                continue
            for filename in os.listdir(bin_path):
                os.chmod(join(bin_path, filename), 0755)

    return path


def _list():
    total = 0
    result = []
    root = join(client.local_root.value, 'cache', 'implementation')

    if not exists(root):
        os.makedirs(root)
        return 0, []

    for filename in os.listdir(root):
        path = join(root, filename)
        if not isdir(path):
            continue
        try:
            with file(join(path, '.unpack_size')) as f:
                unpack_size = json.load(f)
            total += unpack_size
            # Negative `unpack_size` to process large impls at first
            result.append((os.stat(path).st_mtime, -unpack_size, path))
        except Exception:
            toolkit.exception('Cannot list %r cached implementation', path)
            result.append((0, 0, path))

    return total, sorted(result)


def _unpack_stream(stream, dst):
    with toolkit.NamedTemporaryFile() as tmp_file:
        for chunk in stream:
            tmp_file.write(chunk)
        tmp_file.flush()
        if not exists(dst):
            os.makedirs(dst)
        try:
            with Bundle(tmp_file.name, 'application/zip') as bundle:
                bundle.extractall(dst)
        except Exception:
            shutil.rmtree(dst, ignore_errors=True)
            raise
