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

from active_toolkit import coroutine


_LOOKUP_RESULT_LOCAL = 8
_PROTO_UNSPEC = -1
_IF_UNSPEC = -1

_DBUS_NAME = 'org.freedesktop.Avahi'
_DBUS_INTERFACE_SERVICE_BROWSER = 'org.freedesktop.Avahi.ServiceBrowser'


_logger = logging.getLogger('zeroconf')


class ServiceBrowser(object):

    def __init__(self):
        _logger.info('Start browsing hosts using Avahi')
        self._queue = Queue()
        self._cond = coroutine.AsyncCondition()
        self._thread = _Thread(self._queue, self._cond)
        self._thread.daemon = True
        self._thread.start()

    def browse(self):
        while True:
            if self._thread is None:
                break
            self._cond.wait()
            try:
                while True:
                    yield self._queue.get_nowait()
            except Empty:
                pass

    def close(self):
        if self._thread is not None:
            _logger.info('Stop browsing hosts using Avahi')
            self._thread.kill()
            self._thread.join()
            self._thread = None
        self._cond.notify()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Thread(threading.Thread):

    def __init__(self, queue, cond):
        import gobject

        threading.Thread.__init__(self)

        self._queue = queue
        self._cond = cond
        self._loop = gobject.MainLoop()
        self._server = None

    def run(self):
        import dbus
        from dbus.mainloop.glib import DBusGMainLoop, threads_init
        import gobject

        gobject.threads_init()
        threads_init()

        DBusGMainLoop(set_as_default=True)
        bus = dbus.SystemBus()

        self._server = dbus.Interface(bus.get_object(_DBUS_NAME, '/'),
                'org.freedesktop.Avahi.Server')
        sbrowser = dbus.Interface(
                bus.get_object(_DBUS_NAME,
                    self._server.ServiceBrowserNew(_IF_UNSPEC, _PROTO_UNSPEC,
                        '_workstation._tcp', 'local', 0)),
                _DBUS_INTERFACE_SERVICE_BROWSER)

        sbrowser.connect_to_signal('ItemNew', self.__ItemNew_cb)
        sbrowser.connect_to_signal('ItemRemove', self.__ItemRemove_cb)

        self._loop.run()

    def kill(self):
        import gobject
        gobject.idle_add(self._loop.quit)

    def __ItemNew_cb(self, interface, protocol, name, stype, domain, flags):
        if flags & _LOOKUP_RESULT_LOCAL:
            return
        _logger.debug('Got new workstation: %s', name)
        self._server.ResolveService(interface, protocol, name, stype, domain,
                _PROTO_UNSPEC, 0, reply_handler=self.__ResolveService_cb,
                error_handler=self.__error_handler_cb)

    def __ResolveService_cb(self, interface, protocol, name, type_, domain,
            host, aprotocol, address, port, txt, flags):
        _logger.debug('Got new address: %s', address)
        self._queue.put(str(address))
        self._cond.notify()

    def __ItemRemove_cb(self, interface, protocol, name, type_, domain, *args):
        _logger.debug('Got removed workstation: %s', name)

    def __error_handler_cb(self, error, *args):
        _logger.warning('ResolveService failed: %s', error)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    with ServiceBrowser() as monitor:
        for i in monitor.browse():
            pass
