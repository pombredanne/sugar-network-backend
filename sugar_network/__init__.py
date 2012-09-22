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

from sugar_network.toolkit import sugar
from sugar_network.local.activities import checkins
from sugar_network.local import api_url, server_mode
from sugar_network_webui import webui_port


def launch(*args, **kwargs):
    from sugar_network.zerosugar import injector
    return injector.launch(*args, **kwargs)


def checkin(*args, **kwargs):
    from sugar_network.zerosugar import injector
    return injector.checkin(*args, **kwargs)


def Client(url=None, **kwargs):
    from sugar_network.toolkit import http
    if url is None:
        url = api_url.value
    return http.Client(url, **kwargs)


def IPCClient(**kwargs):
    from sugar_network.toolkit import http
    from sugar_network.local import ipc_port
    return http.Client('http://localhost:%s' % ipc_port.value, **kwargs)


def DBusClient(*args, **kwargs):
    from sugar_network.local import dbus_client
    return dbus_client.DBusClient(*args, **kwargs)
