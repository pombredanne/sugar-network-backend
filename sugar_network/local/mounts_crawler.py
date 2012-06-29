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
from os.path import join, isdir
from gettext import gettext as _

import active_document as ad
from sugar_network.toolkit.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_MOVED_TO, IN_MOVED_FROM
from sugar_network import node
from active_toolkit import util, coroutine


_DB_DIRECTORY = '.network'

_logger = logging.getLogger('local.mounts_crawler')


def dispatch(root, found_cb, lost_cb):
    _logger.info(_('Start monitoring %r for mounts'), root)
    monitor = _Monitor(root, found_cb, lost_cb)
    try:
        monitor.dispatch()
    finally:
        monitor.close()
        _logger.info(_('Stop monitoring %r for mounts'), root)


class _Monitor(object):

    def __init__(self, root, found_cb, lost_cb):
        self._root = root
        self._found_cb = found_cb
        self._lost_cb = lost_cb
        self._volumes = {}
        self._populate_job = coroutine.spawn(self._populate)

    def dispatch(self):
        with Inotify() as monitor:
            monitor.add_watch(self._root, IN_DELETE_SELF | IN_CREATE | \
                    IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM)
            while not monitor.closed:
                coroutine.select([monitor.fileno()], [], [])
                for filename, event, __ in monitor.read():
                    path = join(self._root, filename)
                    try:
                        if event & IN_DELETE_SELF:
                            _logger.warning(
                                _('Lost ourselves, cannot monitor anymore'))
                            monitor.close()
                            break
                        elif event & (IN_DELETE | IN_MOVED_FROM):
                            self._lost(path)
                        elif event & (IN_CREATE | IN_MOVED_TO):
                            self._found(path)
                    except Exception:
                        util.exception(_('Cannot process %r directoryr'), path)

    def close(self):
        self._populate_job.kill()

    def _populate(self):
        for filename in os.listdir(self._root):
            self._found(join(self._root, filename))

    def _found(self, path):
        db_path = join(path, _DB_DIRECTORY)
        if isdir(db_path) and path not in self._volumes:
            _logger.debug('Found %r mount', path)
            volume = self._mount_volume(db_path)
            self._found_cb(path, volume)
            self._volumes[path] = volume

    def _lost(self, path):
        if path in self._volumes:
            _logger.debug('Lost %r mount', path)
            self._volumes.pop(path)
            self._lost_cb(path)

    def _mount_volume(self, path):
        volume = ad.SingleVolume(path, node.DOCUMENTS)

        for cls in volume.values():
            for __ in cls.populate():
                coroutine.dispatch()

        return volume
