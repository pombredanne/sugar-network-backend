# Copyright (C) 2013 Aleksey Lim
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
import json
import struct
import logging

from sugar_network.toolkit import coroutine, exception


_logger = logging.getLogger('gbus')
_dbus_thread = None
_dbus_loop = None


def call(op, *args, **kwargs):
    result = coroutine.ThreadResult()

    class _Exception(tuple):
        pass

    def do_call():
        try:
            op(result, *args, **kwargs)
        except Exception:
            result.set(_Exception(sys.exc_info()))

    _logger.trace('Call %s(%r, %r)', op, args, kwargs)

    _call(do_call)
    value = result.get()
    if type(value) is _Exception:
        etype, error, traceback = value
        raise etype, error, traceback

    return value


def pipe(op, *args, **kwargs):
    fd_r, fd_w = os.pipe()

    def feedback(event=None):
        if event is None:
            os.close(fd_w)
            return
        event = json.dumps(event)
        os.write(fd_w, struct.pack('i', len(event)))
        os.write(fd_w, event)

    def do_call():
        try:
            op(feedback, *args, **kwargs)
        except Exception:
            exception('Failed to call %r(%r, %r)', op, args, kwargs)
            os.close(fd_w)

    _logger.trace('Pipe %s(%r, %r)', op, args, kwargs)

    try:
        _call(do_call)
        while True:
            coroutine.select([fd_r], [], [])
            length = os.read(fd_r, struct.calcsize('i'))
            if not length:
                break
            length = struct.unpack('i', length)[0]
            yield json.loads(os.read(fd_r, length))
    finally:
        os.close(fd_r)


def join():
    global _dbus_thread

    if _dbus_thread is None:
        return

    import gobject

    gobject.idle_add(_dbus_loop.quit)
    _dbus_thread.join()
    _dbus_thread = None


def _call(op):
    import threading
    import gobject
    from dbus.mainloop import glib

    global _dbus_loop
    global _dbus_thread

    if _dbus_thread is None:
        gobject.threads_init()
        glib.threads_init()
        glib.DBusGMainLoop(set_as_default=True)
        _dbus_loop = gobject.MainLoop()
        _dbus_thread = threading.Thread(target=_dbus_loop.run)
        _dbus_thread.daemon = True
        _dbus_thread.start()

    gobject.idle_add(op)
