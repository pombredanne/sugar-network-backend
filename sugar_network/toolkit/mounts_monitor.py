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

from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_MOVED_TO, IN_MOVED_FROM
from active_toolkit import coroutine, util


_COMPLETE_MOUNT_TIMEOUT = 3

_root = None
_jobs = coroutine.Pool()
_connects = {}
_found = set()

_logger = logging.getLogger('mounts_monitor')


def start(root):
    global _root
    if _jobs:
        return
    _root = root
    _logger.info('Start monitoring %r for mounts', _root)
    for filename in os.listdir(_root):
        _found_mount(filename)
    _jobs.spawn(_monitor)


def stop():
    if not _jobs:
        return
    _logger.info('Stop monitoring %r for mounts', _root)
    _jobs.kill()


def connect(filename, found_cb, lost_cb):
    _connects[filename] = (found_cb, lost_cb)


def _found_mount(filename):
    if filename not in _connects or filename in _found:
        return
    found_cb, __ = _connects[filename]
    path = join(_root, filename)
    try:
        found_cb(path)
    except Exception:
        util.exception(_logger, 'Cannot process %r mount', path)


def _lost_mount(filename):
    if filename not in _found:
        return
    __, lost_cb = _connects[filename]
    path = join(_root, filename)
    try:
        lost_cb(path)
    except Exception:
        util.exception(_logger, 'Cannot process %r unmount', path)


def _monitor():
    _logger.info('Start monitoring %r for mounts', root)

    with Inotify() as monitor:
        monitor.add_watch(root, IN_DELETE_SELF | IN_CREATE |
                IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
        while not monitor.closed:
            coroutine.select([monitor.fileno()], [], [])
            for filename, event, __ in monitor.read():
                if event & IN_DELETE_SELF:
                    _logger.warning('Lost %r, cannot monitor anymore',
                            root)
                    monitor.close()
                    break
                elif event & (IN_DELETE | IN_MOVED_FROM):
                    _lost_mount(filename)
                elif event & (IN_CREATE | IN_MOVED_TO):
                    # Right after moutning, access to directory
                    # might be restricted; let system enough time
                    # to complete mounting
                    coroutine.sleep(_COMPLETE_MOUNT_TIMEOUT)
                    _found_mount(filename)
