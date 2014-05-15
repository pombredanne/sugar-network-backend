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

# Code is based on pyinotify sources
# http://pypi.python.org/pypi/pyinotify

"""Linux inotify integration."""

import os
import errno
import struct
import ctypes
import ctypes.util
import logging
from os.path import abspath, relpath, join, isdir

from . import coroutine


EVENT_DIR_CREATED = 1
EVENT_DIR_MOVED_FROM = 2
EVENT_DIR_DELETED = 3
EVENT_FILE_UPDATED = 4
EVENT_FILE_MOVED_FROM = 5
EVENT_FILE_DELETED = 6

"""
Supported events suitable for MASK parameter of INOTIFY_ADD_WATCH.

"""
#: File was accessed
IN_ACCESS = 0x00000001
#: File was modified
IN_MODIFY = 0x00000002
#: Metadata changed
IN_ATTRIB = 0x00000004
#: Writtable file was closed
IN_CLOSE_WRITE = 0x00000008
#: Unwrittable file closed
IN_CLOSE_NOWRITE = 0x00000010
#: Close
IN_CLOSE = (IN_CLOSE_WRITE | IN_CLOSE_NOWRITE)
#: File was opened
IN_OPEN = 0x00000020
#: File was moved from X
IN_MOVED_FROM = 0x00000040
#: File was moved to Y
IN_MOVED_TO = 0x00000080
#: Moves
IN_MOVE = (IN_MOVED_FROM | IN_MOVED_TO)
#: Subfile was created
IN_CREATE = 0x00000100
#: Subfile was deleted
IN_DELETE = 0x00000200
#: Self was deleted
IN_DELETE_SELF = 0x00000400
#: Self was moved
IN_MOVE_SELF = 0x00000800

"""
Events sent by the kernel.

"""
#: Backing fs was unmounted
IN_UNMOUNT = 0x00002000
#: Event queued overflowed
IN_Q_OVERFLOW = 0x00004000
#: File was ignored
IN_IGNORED = 0x00008000

"""
Special flags.

"""
#: Only watch the path if it is a directory
IN_ONLYDIR = 0x01000000
#: Do not follow a sym link
IN_DONT_FOLLOW = 0x02000000
#: Exclude events on unlinked objects
IN_EXCL_UNLINK = 0x04000000
#: Add to the mask of an already existing watch
IN_MASK_ADD = 0x20000000
#: Event occurred against dir
IN_ISDIR = 0x40000000
#: Only send event once
IN_ONESHOT = 0x80000000

#: All events which a program can wait on
IN_ALL_EVENTS = (
        IN_ACCESS | IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE | IN_CLOSE_NOWRITE |
        IN_OPEN | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE |
        IN_DELETE_SELF | IN_MOVE_SELF)


_EVENT_HEADER_SIZE = \
        ctypes.sizeof(ctypes.c_int) + \
        ctypes.sizeof(ctypes.c_uint32) * 3
_EVENT_BUF_MAXSIZE = 1024 * (_EVENT_HEADER_SIZE + 16)

_logger = logging.getLogger('inotify')


class Inotify(object):

    def __init__(self):
        self._libc = None
        self._fd = None
        self._wds = {}

        self._init_ctypes()
        _logger.info('Inotify initialized')

        self._fd = self._libc.inotify_init()
        _assert(self._fd >= 0, 'Cannot initialize Inotify')

    def fileno(self):
        return self._fd

    @property
    def closed(self):
        return self._fd is None

    def close(self):
        if self._fd is None:
            return

        os.close(self._fd)
        self._fd = None

        _logger.info('Inotify closed')

    def add_watch(self, path, mask, data=None):
        if self.closed:
            raise RuntimeError('Inotify is closed')

        path = abspath(path)

        cpath = ctypes.create_string_buffer(path)
        wd = self._libc.inotify_add_watch(self._fd, cpath, mask)
        _assert(wd >= 0, 'Cannot add watch for %r', path)

        if wd not in self._wds:
            _logger.trace('Added %r watch of %r with 0x%X mask',
                    wd, path, mask)
            self._wds[wd] = (path, data)

        return wd

    def rm_watch(self, wd):
        if self.closed:
            raise RuntimeError('Inotify is closed')

        if wd not in self._wds:
            return

        path, __ = self._wds[wd]
        _logger.trace('Remove %r watch of %s', wd, path)

        self._libc.inotify_rm_watch(self._fd, wd)
        del self._wds[wd]

    def read(self):
        if self.closed:
            raise RuntimeError('Inotify is closed')

        buf = os.read(self._fd, _EVENT_BUF_MAXSIZE)
        queue_size = len(buf)

        pos = 0
        while pos < queue_size:
            wd, mask, __, name_len = \
                    struct.unpack('iIII', buf[pos:pos + _EVENT_HEADER_SIZE])
            pos += _EVENT_HEADER_SIZE

            filename_end = buf.find('\x00', pos, pos + name_len)
            if filename_end == -1:
                filename = ''
            else:
                filename = buf[pos:filename_end]
            pos += name_len

            if wd not in self._wds:
                continue
            path, data = self._wds[wd]

            _logger.trace('Got event: wd=%r mask=0x%X path=%r filename=\'%s\'',
                    wd, mask, path, filename)

            yield filename, mask, data

    def _init_ctypes(self):
        libc_name = ctypes.util.find_library('c')
        self._libc = ctypes.CDLL(libc_name, use_errno=True)

        if not hasattr(self._libc, 'inotify_init') or \
                not hasattr(self._libc, 'inotify_add_watch') or \
                not hasattr(self._libc, 'inotify_rm_watch'):
            raise RuntimeError('Inotify is not found in libc')

        self._libc.inotify_init.argtypes = []
        self._libc.inotify_init.restype = ctypes.c_int
        self._libc.inotify_add_watch.argtypes = \
                [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self._libc.inotify_add_watch.restype = ctypes.c_int
        self._libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        self._libc.inotify_rm_watch.restype = ctypes.c_int

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def monitor(path):
    """Monitor specified directory recursively.

    :param path:
        root path to monitor
    :returns:
        generator object which yields tuples of event and path
        (relative to the `path`); events are `EVENT_*` contants

    """
    path = abspath(path)
    inotify = Inotify()
    try:
        root = _Directory(inotify, path, IN_DELETE_SELF)
        for __ in root.bound():
            pass
        while True:
            coroutine.select([inotify.fileno()], [], [])
            for filename, mask, cb in inotify.read():
                for event in cb(filename, mask):
                    event, event_path = event
                    event_path = relpath(event_path, path)
                    yield event, event_path
                    if event == EVENT_DIR_DELETED and event_path == '.':
                        raise RuntimeError('Root directory deleted')
    finally:
        inotify.close()


class _Directory(object):

    def __init__(self, inotify, path, mask=0, add=False):
        _logger.debug('Start monitoring %r', path)

        self._inotify = inotify
        self._path = path
        self._wd = self._inotify.add_watch(self._path,
                IN_CREATE | IN_DELETE | IN_MOVED_TO | IN_MOVED_FROM |
                        IN_CLOSE_WRITE | mask,
                self.__watch_cb)
        self._nodes = {}

    def bound(self):
        yield EVENT_DIR_CREATED, self._path
        for filename in os.listdir(self._path):
            path = join(self._path, filename)
            if isdir(path):
                node = self._nodes[filename] = _Directory(self._inotify, path)
                for event in node.bound():
                    yield event
            else:
                yield EVENT_FILE_UPDATED, path

    def unbound(self):
        _logger.debug('Stop monitoring %r', self._path)
        for node in self._nodes.values():
            node.unbound()
        self._inotify.rm_watch(self._wd)

    def __watch_cb(self, filename, event):
        path = join(self._path, filename)
        if event & IN_DELETE_SELF:
            _logger.warning('Lost ourselves, cannot monitor anymore')
            self.unbound()
            yield EVENT_DIR_DELETED, path
        elif event & (IN_CREATE | IN_MOVED_TO):
            if isdir(path):
                node = self._nodes[filename] = _Directory(self._inotify, path)
                for event in node.bound():
                    yield event
            elif event & IN_MOVED_TO or _nlink(path) > 1:
                # There is only one case when newly created file can be read,
                # if number of hardlinks is bigger than one, i.e., its content
                # already populated
                yield EVENT_FILE_UPDATED, path
        elif event & IN_CLOSE_WRITE:
            yield EVENT_FILE_UPDATED, path
        elif event & IN_DELETE:
            if filename in self._nodes:
                self._nodes.pop(filename).unbound()
                yield EVENT_DIR_DELETED, path
            else:
                yield EVENT_FILE_DELETED, path
        elif event & IN_MOVED_FROM:
            if filename in self._nodes:
                self._nodes.pop(filename).unbound()
                yield EVENT_DIR_MOVED_FROM, path
            else:
                yield EVENT_FILE_MOVED_FROM, path


def _assert(condition, message, *args):
    if condition:
        return
    if args:
        message = message % args
    code = ctypes.get_errno()
    message = '%s: %s (%s)' % \
            (message, os.strerror(code), errno.errorcode[code])
    raise RuntimeError(message)


def _nlink(path):
    try:
        return os.stat(path).st_nlink
    except Exception:
        return 0
