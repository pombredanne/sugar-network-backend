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


class DBusClient(object):

    def __init__(self, **common_args):
        self._object = dbus.Interface(
                dbus.SessionBus().get_object(
                    'org.sugarlabs.Network',
                    '/org/sugarlabs/Network'),
                'org.sugarlabs.Network')
        self._common_args = common_args

    def __call__(self, method='GET', cmd=None,
            reply_handler=None, error_handler=None, **kwargs):
        kwargs.update(self._common_args)
        kwargs['method'] = method
        if cmd:
            kwargs['cmd'] = cmd
        cmd = json.dumps(kwargs)

        if reply_handler is None:
            return json.loads(self.Call(cmd))
        else:
            self.Call(cmd, reply_handler=lambda response:
                    reply_handler(json.loads(response)),
                    error_handler=error_handler)

    def __getattr__(self, name):
        return getattr(self._object, name)
