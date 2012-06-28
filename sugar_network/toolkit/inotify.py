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

# Code is based on pyinotify sources
# http://pypi.python.org/pypi/pyinotify

"""Linux inotify integration.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/inotify.py$
$Date: 2012-06-28$

"""
import os
import errno
import struct
import ctypes
import ctypes.util
import logging
from os.path import abspath
from gettext import gettext as _


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
        _logger.info(_('Monitor initialized'))

        self._fd = self._libc.inotify_init()
        _assert(self._fd >= 0, _('Cannot initialize Inotify'))

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

        _logger.info(_('Monitor closed'))

    def add_watch(self, path, mask, data=None):
        if self.closed:
            raise RuntimeError(_('Inotify is closed'))

        path = abspath(path)

        cpath = ctypes.create_string_buffer(path)
        wd = self._libc.inotify_add_watch(self._fd, cpath, mask)
        _assert(wd >= 0, _('Cannot add watch for %r'), path)

        if wd not in self._wds:
            _logger.debug('Added %r watch of %r with 0x%X mask',
                    wd, path, mask)
            self._wds[wd] = (path, data)

        return wd

    def rm_watch(self, wd):
        if self.closed:
            raise RuntimeError(_('Inotify is closed'))

        if wd not in self._wds:
            return

        path, __ = self._wds[wd]
        _logger.debug('Remove %r watch of %s', wd, path)

        self._libc.inotify_rm_watch(self._fd, wd)
        del self._wds[wd]

    def read(self):
        if self.closed:
            raise RuntimeError(_('Inotify is closed'))

        buf = os.read(self._fd, _EVENT_BUF_MAXSIZE)
        queue_size = len(buf)

        pos = 0
        while pos < queue_size:
            wd, mask, __, name_len = \
                    struct.unpack('iIII', buf[pos:pos + _EVENT_HEADER_SIZE])
            pos += _EVENT_HEADER_SIZE

            filename_end = buf.find('\0', pos, pos + name_len)
            if filename_end == -1:
                filename = ''
            else:
                filename = buf[pos:filename_end]
            pos += name_len

            if wd not in self._wds:
                continue
            path, data = self._wds[wd]

            _logger.debug('Got event: wd=%r mask=0x%X path=%r filename=\'%s\'',
                    wd, mask, path, filename)

            yield filename, mask, data

    def _init_ctypes(self):
        libc_name = ctypes.util.find_library('c')
        self._libc = ctypes.CDLL(libc_name, use_errno=True)

        if not hasattr(self._libc, 'inotify_init') or \
                not hasattr(self._libc, 'inotify_add_watch') or \
                not hasattr(self._libc, 'inotify_rm_watch'):
            raise RuntimeError(_('Inotify is not found in libc'))

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


def _assert(condition, message, *args):
    if condition:
        return
    if args:
        message = message % args
    code = ctypes.get_errno()
    message = '%s: %s (%s)' % \
            (message, os.strerror(code), errno.errorcode[code])
    raise RuntimeError(message)


if __name__ == '__main__':
    import select

    logging.basicConfig(level=logging.DEBUG)

    with Inotify() as monitor:
        monitor.add_watch('/tmp', IN_MASK_ADD | IN_ALL_EVENTS)
        poll = select.poll()
        poll.register(monitor.fileno(), select.POLLIN)
        while poll.poll():
            for event in monitor.read():
                pass
