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

import logging

from sugar_network.toolkit import gbus


_LOOKUP_RESULT_LOCAL = 8
_PROTO_UNSPEC = -1
_IF_UNSPEC = -1

_DBUS_NAME = 'org.freedesktop.Avahi'
_DBUS_INTERFACE_SERVICE_BROWSER = 'org.freedesktop.Avahi.ServiceBrowser'


_logger = logging.getLogger('zeroconf')


def browse_workstations():
    _logger.info('Start browsing hosts using Avahi')

    for address in gbus.pipe(_browser):
        yield address


def _browser(pipe):
    import dbus

    bus = dbus.SystemBus()
    server = dbus.Interface(bus.get_object(_DBUS_NAME, '/'),
            'org.freedesktop.Avahi.Server')

    def ItemNew_cb(interface, protocol, name, stype, domain, flags):
        if flags & _LOOKUP_RESULT_LOCAL:
            return
        _logger.debug('Got new workstation: %s', name)
        server.ResolveService(interface, protocol, name, stype, domain,
                _PROTO_UNSPEC, 0, reply_handler=ResolveService_cb,
                error_handler=error_handler_cb)

    def ResolveService_cb(interface, protocol, name, type_, domain,
            host, aprotocol, address, port, txt, flags):
        _logger.debug('Got new address: %s', address)
        pipe(str(address))

    def ItemRemove_cb(interface, protocol, name, type_, domain, *args):
        _logger.debug('Got removed workstation: %s', name)

    def error_handler_cb(error, *args):
        _logger.warning('ResolveService failed: %s', error)

    browser = dbus.Interface(
            bus.get_object(_DBUS_NAME,
                server.ServiceBrowserNew(_IF_UNSPEC, _PROTO_UNSPEC,
                    '_workstation._tcp', 'local', 0)),
            _DBUS_INTERFACE_SERVICE_BROWSER)
    browser.connect_to_signal('ItemNew', ItemNew_cb)
    browser.connect_to_signal('ItemRemove', ItemRemove_cb)


if __name__ == '__main__':
    from pprint import pprint
    logging.basicConfig(level=logging.DEBUG)
    for __ in browse_workstations():
        pprint(__)
