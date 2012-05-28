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
import logging
from os.path import join, exists, isdir, dirname
from gettext import gettext as _

from active_toolkit import coroutine, util
from local_document.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_CLOSE_WRITE, \
        IN_MOVED_TO, IN_MOVED_FROM
from local_document import env


_logger = logging.getLogger('local_document.crawler')


def populate(paths, found_cb, lost_cb):

    class FakeMonitor(object):

        def add_watch(self, *args):
            pass

        def found_cb(self, path):
            found_cb(path)

        def lost_cb(self, path):
            lost_cb(path)

    for path in paths:
        env.ensure_path(path, '')
        _Root(FakeMonitor(), path)


def dispatch(paths, found_cb, lost_cb):
    with _Inotify(found_cb, lost_cb) as monitor:
        roots = []
        for path in paths:
            env.ensure_path(path, '')
            roots.append(_Root(monitor, path))

        while True:
            coroutine.select([monitor.fd], [], [])
            if monitor.closed:
                break
            for filename, event, cb in monitor.dispatch():
                try:
                    cb(filename, event)
                except Exception:
                    util.exception(_('Cannot dispatch 0x%X event for %r'),
                            event, filename)
                coroutine.dispatch()


class _Inotify(Inotify):

    def __init__(self, found_cb, lost_cb):
        Inotify.__init__(self)
        self.found_cb = found_cb
        self.lost_cb = lost_cb


class _Root(object):

    def __init__(self, monitor, path):
        self.path = env.ensure_path(path, '')
        self._monitor = monitor
        self._nodes = {}

        _logger.info('Start monitoring %r activities root', self. path)

        monitor.add_watch(self.path,
                IN_DELETE_SELF | IN_CREATE | IN_DELETE | \
                        IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        for filename in os.listdir(self.path):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(monitor, path)

    def __watch_cb(self, filename, event):
        if event & IN_DELETE_SELF:
            _logger.warning(_('Lost ourselves, cannot monitor anymore'))
            self._nodes.clear()
            return

        if event & (IN_CREATE | IN_MOVED_TO):
            path = join(self.path, filename)
            if isdir(path):
                self._nodes[filename] = _Node(self._monitor, path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            node = self._nodes.get(filename)
            if node is not None:
                node.unlink()
                del self._nodes[filename]


class _Node(object):

    def __init__(self, monitor, path):
        self._path = path
        self._monitor = monitor
        self._activity_path = join(path, 'activity')
        self._activity_dir = None

        _logger.debug('Start monitoring %r root activity directory', path)

        self._wd = monitor.add_watch(path,
                IN_CREATE | IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM,
                self.__watch_cb)

        if exists(self._activity_path):
            self._activity_dir = _ActivityDir(monitor, self._activity_path)

    def unlink(self):
        if self._activity_dir is not None:
            self._activity_dir.unlink()
            self._activity_dir = None
        _logger.debug('Stop monitoring %r root activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def __watch_cb(self, filename, event):
        if filename != 'activity':
            return
        if event & (IN_CREATE | IN_MOVED_TO):
            self._activity_dir = \
                    _ActivityDir(self._monitor, self._activity_path)
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self._activity_dir.unlink()
            self._activity_dir = None


class _ActivityDir(object):

    def __init__(self, monitor, path):
        self._path = path
        self._monitor = monitor
        self._found = False
        self._node_path = dirname(path)
        self._info_path = join(path, 'activity.info')

        _logger.debug('Start monitoring %r activity directory', path)

        self._wd = monitor.add_watch(path,
                IN_CREATE | IN_CLOSE_WRITE | IN_DELETE | IN_MOVED_TO | \
                        IN_MOVED_FROM,
                self.__watch_cb)

        if exists(self._info_path):
            self.found()

    def unlink(self):
        self.lost()
        _logger.debug('Stop monitoring %r activity directory', self._path)
        self._monitor.rm_watch(self._wd)

    def found(self):
        if self._found:
            return
        _logger.debug('Found %r', self._node_path)
        self._found = True
        self._monitor.found_cb(self._node_path)

    def lost(self):
        if not self._found:
            return
        _logger.debug('Lost %r', self._node_path)
        self._found = False
        self._monitor.lost_cb(self._node_path)

    def __watch_cb(self, filename, event):
        if filename != 'activity.info':
            return
        if event & (IN_CREATE | IN_CLOSE_WRITE | IN_MOVED_TO):
            self.found()
        elif event & (IN_DELETE | IN_MOVED_FROM):
            self.lost()
