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
from dbus.service import Object
from dbus.mainloop.glib import threads_init, DBusGMainLoop

import active_document as ad
from active_toolkit import util, coroutine
from sugar_network.toolkit import sugar
from sugar_network.resources.volume import Request


_logger = logging.getLogger('dbus_thread')
_services = []
_call_queue = coroutine.AsyncQueue()


def start(commands_processor):
    gobject.threads_init()
    threads_init()

    mainloop = gobject.MainLoop()
    thread = threading.Thread(target=_mainloop_thread, args=[mainloop])
    thread.daemon = True
    thread.start()

    def handle_event(event):
        for service in _services:
            spawn(service.handle_event, event)

    commands_processor.connect(handle_event)
    try:
        for request, reply_cb, error_cb, args in _call_queue:
            try:
                reply = commands_processor.call(request)
            except Exception, error:
                util.exception(_logger, 'DBus %r request failed', request)
                if error_cb is not None:
                    spawn(error_cb, error)
            else:
                if reply_cb is not None:
                    if reply is not None:
                        args += [reply]
                    spawn(reply_cb, *args)
    finally:
        commands_processor.disconnect(handle_event)
        spawn(mainloop.quit)
        thread.join()


def spawn(callback, *args):

    def process_callback():
        try:
            callback(*args)
        except Exception:
            util.exception(_logger, 'Fail to spawn %r(%r) in DBus thread',
                    callback, args)

    gobject.idle_add(process_callback)


def spawn_service(service_class):

    def start_service():
        _services.append(service_class())
        _logger.debug('Service %r started', service_class)

    spawn(start_service)


class Service(Object):

    def call(self, reply_cb, error_cb, content=None, content_stream=None,
            content_length=None, args=None, **kwargs):
        """Call a command in parent thread."""
        request = Request(kwargs)
        request.principal = sugar.uid()
        request.access_level = ad.ACCESS_LOCAL
        request.content = content
        request.content_stream = content_stream
        request.content_length = content_length
        _call_queue.put((request, reply_cb, error_cb, args or []))

    def handle_event(self, event):
        """Handle, in child thread, events gotten from parent thread."""
        pass

    def stop(self):
        pass


def _mainloop_thread(mainloop):
    DBusGMainLoop(set_as_default=True)

    def Disconnected_cb():
        _logger.info('Service disconnected from the bus, will exit')
        mainloop.quit()

    disconnected_hid = None
    try:
        bus = dbus.SessionBus()
        disconnected_hid = bus.add_signal_receiver(Disconnected_cb,
                signal_name='Disconnected',
                dbus_interface='org.freedesktop.DBus.Local')
        bus.set_exit_on_disconnect(False)

        _logger.info('Started thread')
        mainloop.run()

    except Exception:
        util.exception(_logger, 'Thread failed')
    finally:
        if disconnected_hid is not None:
            disconnected_hid.remove()
        while _services:
            _services.pop().stop()
        _call_queue.abort()
        _logger.debug('Thread stopped')
