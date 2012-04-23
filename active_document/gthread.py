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

import gevent
import gevent.event


#: Process events loop during long running operations, e.g., synchronization
dispatch = gevent.sleep


class Condition(object):

    def __init__(self):
        self._event = gevent.event.Event()
        self._value = None

    def wait(self, timeout=None):
        self._event.wait(timeout)
        return self._value

    def notify(self, value=None):
        self._value = value
        self._event.set()
        self._event.clear()
