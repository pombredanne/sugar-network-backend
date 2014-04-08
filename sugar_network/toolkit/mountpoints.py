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
from sugar_network.toolkit import coroutine


_COMPLETE_MOUNT_TIMEOUT = 3

_logger = logging.getLogger('mountpoints')
_connects = {}
_found = {}


def populate(root):
    for name in os.listdir(root):
        _found_mount(join(root, name))


def monitor(root):
    _logger.info('Start monitoring %r for mounts', root)

    populate(root)

    with Inotify() as inotify:
        inotify.add_watch(root, IN_DELETE_SELF | IN_CREATE |
                IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
        while not inotify.closed:
            coroutine.select([inotify.fileno()], [], [])
            for name, event, __ in inotify.read():
                path = join(root, name)
                if event & IN_DELETE_SELF:
                    _logger.warning('Lost %r, cannot monitor anymore', root)
                    inotify.close()
                    break
                elif event & (IN_DELETE | IN_MOVED_FROM):
                    _lost_mount(path)
                elif event & (IN_CREATE | IN_MOVED_TO):
                    # Right after moutning, access to newly mounted directory
                    # might be restricted; let the system enough time
                    # to complete mounting routines
                    coroutine.sleep(_COMPLETE_MOUNT_TIMEOUT)
                    _found_mount(path)


def connect(filename, found_cb, lost_cb):
    if filename in _connects:
        return
    _connects[filename] = (found_cb, lost_cb)
    for path, filenames in _found.items():
        if exists(join(path, filename)):
            filenames.add(filename)
            _call(path, filename, 0)


def _found_mount(path):
    found = _found.setdefault(path, set())
    new_found = []
    for filename in _connects:
        if filename not in found and exists(join(path, filename)):
            new_found.append(filename)
    for filename in new_found:
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
        _logger.exception('Cannot call %r for %r mount', cb, path)
