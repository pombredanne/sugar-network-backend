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

import logging

import gobject

from sugar_network.connection import Connection


_logger = logging.getLogger('sugar_network')


class Client(gobject.GObject):

    __gsignals__ = {
        'connect': (
            # (mountpoint, connected)
            gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, [str, bool]),
        'keep': (
            # (bundle_id, value)
            gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, [str, bool]),
        'keep_impl': (
            # (bundle_id, value)
            gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, [str, bool]),
        }

    def __init__(self):
        gobject.GObject.__init__(self)

        self._subscription = None

        def init():
            self._subscription = Connection.subscribe()
            gobject.io_add_watch(self._subscription.fileno(),
                    gobject.IO_IN | gobject.IO_HUP, self.__subscription_cb)

        gobject.idle_add(init)

    def __del__(self):
        if self._subscription is not None:
            self._subscription.close()

    def get(self, mountpoint, document, guid, reply):
        conn = Connection(mountpoint, document)
        return conn.send('GET', guid=guid, reply=reply)

    def find(self, mountpoint, document, offset, limit, reply):
        params = {'limit': limit,
                  'reply': reply,
                  }
        conn = Connection(mountpoint, document)

        def fetch_chunk(offset):
            params['offset'] = offset
            response = conn.send('GET', **params)
            return response['result'], response['total']

        items, total = fetch_chunk(offset)
        while True:
            for i in items:
                offset += 1
                yield i
            if offset >= total:
                break
            items, total = fetch_chunk(offset)

    def update(self, mountpoint, document, guid, **props):
        conn = Connection(mountpoint, document)
        conn.send('PUT', guid=guid, content=props,
                    content_type='application/json')

    def __subscription_cb(self, source, cb_condition):
        event = self._subscription.read_message()
        if event is None:
            return False

        event_type = event['event']

        if event_type == 'connect':
            self.emit('connect', event['mountpoint'], True)

        elif event_type == 'disconnect':
            self.emit('connect', event['mountpoint'], False)

        elif event['mountpoint'] == '~' and event['document'] == 'context':
            if event_type in ('create', 'update'):
                bundle_id = event['guid']
                props = event['props']
                if 'keep' in props:
                    self.emit('keep', bundle_id, props['keep'])
                if props.get('keep_impl') in (0, 2):
                    self.emit('keep_impl', bundle_id, bool(props['keep_impl']))
            elif event_type == 'delete':
                bundle_id = event['guid']
                self.emit('keep_impl', bundle_id, False)

        return True
