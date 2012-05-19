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
from Queue import Queue, Empty
from gettext import gettext as _

from active_document import coroutine


_LOOKUP_RESULT_LOCAL = 8
_PROTO_UNSPEC = -1
_IF_UNSPEC = -1

_DBUS_NAME = 'org.freedesktop.Avahi'
_DBUS_INTERFACE_SERVICE_BROWSER = 'org.freedesktop.Avahi.ServiceBrowser'

_thread = None
_job = None
_cond = None
_queue = None
_pool = []

_logger = logging.getLogger('local_document.zeroconf')


def browse_workstation():
    _init()

    for address in _pool:
        yield address


def _init():
    global _thread, _job, _cond, _queue

    if _job is not None:
        return

    _logger.info(_('Start browsing hosts using Avahi'))

    _queue = Queue()
    _cond = coroutine.AsyncCondition()
    _thread = _Thread()
    _thread.daemon = True
    _thread.start()
    _job = coroutine.spawn(_waiter)


def _waiter():
    while True:
        _cond.wait()
        try:
            while True:
                address = _queue.get_nowait()
                if address not in _pool:
                    _logger.debug('Add new %r address', address)
                    _pool.append(address)
        except Empty:
            pass


class _Thread(threading.Thread):

    _server = None

    def run(self):
        import gobject
        import dbus
        import dbus.glib
        from dbus.mainloop.glib import DBusGMainLoop

        gobject.threads_init()
        dbus.glib.threads_init()

        mainloop = DBusGMainLoop()
        bus = dbus.SystemBus(mainloop=mainloop)

        self._server = dbus.Interface(bus.get_object(_DBUS_NAME, '/'),
                'org.freedesktop.Avahi.Server')
        sbrowser = dbus.Interface(
                bus.get_object(_DBUS_NAME,
                    self._server.ServiceBrowserNew(_IF_UNSPEC, _PROTO_UNSPEC,
                        '_workstation._tcp', 'local', 0)),
                _DBUS_INTERFACE_SERVICE_BROWSER)

        sbrowser.connect_to_signal('ItemNew', self.__ItemNew_cb)
        sbrowser.connect_to_signal('ItemRemove', self.__ItemRemove_cb)

        gobject.MainLoop().run()

    def __ItemNew_cb(self, interface, protocol, name, stype, domain, flags):
        if flags & _LOOKUP_RESULT_LOCAL:
            return

        self._server.ResolveService(interface, protocol, name, stype, domain,
                _PROTO_UNSPEC, 0, reply_handler=self.__ResolveService_cb,
                error_handler=self.__error_handler_cb)

    def __ResolveService_cb(self, interface, protocol, name, type_, domain,
            host, aprotocol, address, port, txt, flags):
        _queue.put(str(address))
        _cond.notify()

    def __ItemRemove_cb(self, interface, protocol, name, type_, domain, *args):
        pass

    def __error_handler_cb(self, error, *args):
        _logger.warning('Avahi browse failed: %s', error)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    while True:
        for __ in browse_workstation():
            print __
        coroutine.sleep(3)
