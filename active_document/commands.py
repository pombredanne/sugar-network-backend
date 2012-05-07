# Copyright (C) 2012, Aleksey Lim
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

from gettext import gettext as _

from active_document.util import enforce


class _Commands(dict):

    def register(self, cb, **kwargs):
        cmd = Command(cb, **kwargs)
        enforce(cmd.key not in self, _('Command %r already exists'), cmd)
        self[cmd.key] = cmd

    def find(self, cmds):
        for cmd in cmds:
            if cmd in self:
                return self[cmd]


volumes = _Commands()
directories = _Commands()
documents = _Commands()


def active_command(**kwargs):

    def decorate(func):
        func._is_active_command = True
        func.kwargs = kwargs
        return func

    return decorate


def directory_command(**kwargs):

    def decorate(func):
        directories.register(func, **kwargs)
        return func

    return decorate


def volume_command(**kwargs):

    def decorate(func):
        volumes.register(func, **kwargs)
        return func

    return decorate


class Command(object):

    def __init__(self, callback=None, method='GET', document=None, cmd=None,
            mime_type='application/json', permissions=0):
        self.callback = callback
        self.mime_type = mime_type
        self.permissions = permissions

        key = [method]
        if cmd:
            key.append(cmd)
        if document:
            key.append(document)
        if len(key) > 1:
            self.key = tuple(key)
        else:
            self.key = key[0]

    def __repr__(self):
        return str(self.key)
