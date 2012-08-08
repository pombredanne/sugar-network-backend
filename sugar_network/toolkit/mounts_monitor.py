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
import logging
from os.path import join, exists

from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_MOVED_TO, IN_MOVED_FROM
from active_toolkit import coroutine, util


_COMPLETE_MOUNT_TIMEOUT = 3

_root = None
_jobs = coroutine.Pool()
_connects = {}
_found = {}

_logger = logging.getLogger('mounts_monitor')


def start(root):
    global _root
    if _jobs:
        return
    _root = root
    _logger.info('Start monitoring %r for mounts', _root)
    for name in os.listdir(_root):
        _found_mount(join(_root, name))
    _jobs.spawn(_monitor)


def stop():
    if _jobs:
        _logger.info('Stop monitoring %r for mounts', _root)
        _jobs.kill()
    _connects.clear()
    _found.clear()


def connect(filename, found_cb, lost_cb):
    if filename in _connects:
        return
    _connects[filename] = (found_cb, lost_cb)
    for path, filenames in _found.items():
        if exists(join(path, filename)):
            filenames.add(filename)
            _call(path, filename, 0)


def _found_mount(path):
    _found.setdefault(path, set())
    found = _found[path]
    for filename in _connects:
        if filename in found or not exists(join(path, filename)):
            continue
        found.add(filename)
        _call(path, filename, 0)


def _lost_mount(path):
    if path not in _found:
        return
    for filename in _found.pop(path):
        _call(path, filename, 1)


def _call(path, filename, cb):
    cb = _connects[filename][cb]
    if cb is None:
        return
    _logger.debug('Call %r for %r mount', cb, path)
    try:
        cb(path)
    except Exception:
        util.exception(_logger, 'Cannot call %r for %r mount', cb, path)


def _monitor():
    with Inotify() as monitor:
        monitor.add_watch(_root, IN_DELETE_SELF | IN_CREATE |
                IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
        while not monitor.closed:
            coroutine.select([monitor.fileno()], [], [])
            for name, event, __ in monitor.read():
                path = join(_root, name)
                if event & IN_DELETE_SELF:
                    _logger.warning('Lost %r, cannot monitor anymore', _root)
                    monitor.close()
                    break
                elif event & (IN_DELETE | IN_MOVED_FROM):
                    _lost_mount(path)
                elif event & (IN_CREATE | IN_MOVED_TO):
                    # Right after moutning, access to newly mounted directory
                    # might be restricted; let the system enough time
                    # to complete mounting routines
                    coroutine.sleep(_COMPLETE_MOUNT_TIMEOUT)
                    _found_mount(path)
