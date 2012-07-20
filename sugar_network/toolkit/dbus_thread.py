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

import logging
import threading

import dbus
import gobject
from dbus.mainloop.glib import threads_init, DBusGMainLoop

from active_toolkit import util


_logger = logging.getLogger('dbus_thread')
_thread = None
_mainloop = None
_services = []


def spawn(callback, *args):
    global _thread, _mainloop

    if _thread is None:
        gobject.threads_init()
        threads_init()
        _mainloop = gobject.MainLoop()
        _thread = threading.Thread(target=_mainloop_thread)
        _thread.daemon = True
        _thread.start()

    gobject.idle_add(callback, *args)


def spawn_service(service_class):
    spawn(lambda: _services.append(service_class()))


def shutdown():
    global _thread, _mainloop

    if _thread is None:
        return

    gobject.idle_add(_mainloop.quit)
    _thread.join()
    _thread = None
    _mainloop = None


def _mainloop_thread():
    DBusGMainLoop(set_as_default=True)

    def disconnect_cb():
        _logger.info('Service disconnected from the bus, will exit')
        _mainloop.quit()

    bus = dbus.SessionBus()
    bus.add_signal_receiver(disconnect_cb, signal_name='Disconnected',
            dbus_interface='org.freedesktop.DBus.Local')
    bus.set_exit_on_disconnect(False)

    _logger.info('Started thread')

    try:
        _mainloop.run()
    except Exception:
        util.exception(_logger, 'Thread shutdown with error')
    finally:
        while _services:
            _services.pop().close()
        _logger.info('Stopped thread')
