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

import blinker
import gevent
from gevent.select import select

from local_document.inotify import Inotify, \
        IN_DELETE_SELF, IN_CREATE, IN_DELETE, IN_CLOSE_WRITE
from local_document import env, util


found = blinker.Signal()
lost = blinker.Signal()

_logger = logging.getLogger('local_document.crawler')


class Crawler(object):

    def __init__(self, path):
        self._monitor = Inotify()
        self._poll_job = gevent.spawn(self._poll, path)

    def close(self):
        if self._poll_job is None:
            return
        # XXX select() doesn't handle closing self._monitor.fd
        self._poll_job.kill()
        try:
            self._poll_job.join()
        finally:
            self._monitor.close()
            self._monitor = None
            self._poll_job = None

    def _poll(self, path):
        # pylint: disable-msg=W0612
        root = _Root(self._monitor, path)

        while True:
            select([self._monitor.fd], [], [])
            if self._monitor.closed:
                break
            for filename, event, cb in self._monitor.dispatch():
                try:
                    cb(filename, event)
                except Exception:
                    util.exception(_('Cannot dispatch %X event of %s/%s ' \
                            'directory'), event, path, filename)
                gevent.sleep()

        root = None


class _Root(object):

    def __init__(self, monitor, root):
        self._root = env.ensure_path(root, '')
        self._monitor = monitor
        self._nodes = {}

        for filename in os.listdir(self._root):
            path = join(self._root, filename)
            if isdir(path):
                self._nodes[filename] = _Node(monitor, path)

        monitor.add_watch(self._root, IN_DELETE_SELF | IN_CREATE | IN_DELETE,
                self.__watch_cb)

    def __watch_cb(self, filename, event):
        if event & IN_DELETE_SELF:
            _logger.warning(_('Lost ourselves, cannot monitor anymore'))
            self._nodes.clear()
            return

        path = join(self._root, filename)
        if not isdir(path):
            return

        if event & IN_CREATE:
            self._nodes[filename] = _Node(self._monitor, path)
        elif event & IN_DELETE:
            if filename in self._nodes:
                del self._nodes[filename]


class _Node(object):

    def __init__(self, monitor, path):
        self._monitor = monitor
        self._activity_path = join(path, 'activity')
        self._activity = None

        if exists(self._activity_path):
            self._activity = _ActivityDir(monitor, self._activity_path)

        monitor.add_watch(path, IN_CREATE | IN_DELETE, self.__watch_cb)

    def __watch_cb(self, filename, event):
        if filename != 'activity':
            return
        if event & IN_CREATE:
            self._activity = _ActivityDir(self._monitor, self._activity_path)
        elif event & IN_DELETE:
            self._activity = None


class _ActivityDir(object):

    def __init__(self, monitor, path):
        self.__found = False
        self._node_path = dirname(path)
        self._info_path = join(path, 'activity.info')

        if exists(self._info_path):
            self._found()

        monitor.add_watch(path, IN_CLOSE_WRITE | IN_DELETE, self.__watch_cb)

    def _found(self):
        if self.__found:
            return
        found.send(self._node_path)
        self.__found = True

    def _lost(self):
        if not self.__found:
            return
        lost.send(self._node_path)
        self.__found = False

    def __watch_cb(self, filename, event):
        if filename != 'activity.info':
            return
        if event & IN_CLOSE_WRITE:
            self._found()
        elif event & IN_DELETE:
            self._lost()
