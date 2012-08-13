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

import json

import dbus
from dbus.service import BusName, method, signal

from sugar_network.toolkit import dbus_thread


_SERVICE = 'org.sugarlabs.Network'
_INTERFACE = 'org.sugarlabs.Network'
_OBJECT_PATH = '/org/sugarlabs/Network'


class Network(dbus_thread.Service):

    def __init__(self):
        bus_name = BusName(_SERVICE, bus=dbus.SessionBus())
        dbus_thread.Service.__init__(self, bus_name, _OBJECT_PATH)

    def handle_event(self, event):
        self.Event(json.dumps(event))

    @signal(_INTERFACE, signature='s')
    def Event(self, event):
        pass

    @method(_INTERFACE, in_signature='s', out_signature='s',
            async_callbacks=('reply_cb', 'error_cb'))
    def Call(self, cmd, reply_cb, error_cb):
        self.call(lambda response=None: reply_cb(json.dumps(response)),
                error_cb, **json.loads(cmd))

    @method(_INTERFACE, in_signature='sssas', out_signature='a{sv}',
            async_callbacks=('reply_cb', 'error_cb'))
    def Get(self, mountpoint, document, guid, reply, reply_cb, error_cb):
        self.call(reply_cb, error_cb, method='GET', mountpoint=mountpoint,
                document=document, guid=guid, reply=reply)

    @method(_INTERFACE, in_signature='ssss', out_signature='a{sv}',
            async_callbacks=('reply_cb', 'error_cb'))
    def GetBlob(self, mountpoint, document, guid, prop, reply_cb, error_cb):
        self.call(lambda result: reply_cb(result or {}), error_cb,
                method='GET', cmd='get_blob', mountpoint=mountpoint,
                document=document, guid=guid, prop=prop)

    @method(_INTERFACE, in_signature='ssasa{sv}', out_signature='aa{sv}u',
            async_callbacks=('reply_cb', 'error_cb'))
    def Find(self, mountpoint, document, reply, options, reply_cb, error_cb):
        self.call(lambda result: reply_cb(result['result'], result['total']),
                error_cb, method='GET', mountpoint=mountpoint,
                document=document, reply=reply, **options)

    @method(_INTERFACE, in_signature='sssa{sv}', out_signature='',
            async_callbacks=('reply_cb', 'error_cb'))
    def Update(self, mountpoint, document, guid, props, reply_cb, error_cb):
        self.call(reply_cb, error_cb, method='PUT', mountpoint=mountpoint,
                document=document, guid=guid, content=props)

    @method(_INTERFACE, in_signature='a{sv}', out_signature='')
    def Publish(self, event):
        self.call(None, None, method='POST', cmd='publish', content=event)
