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
import collections
from gettext import gettext as _

import gobject
from gevent import socket

from local_document import ipc
from local_document.socket import SocketFile
from active_document import util


_logger = logging.getLogger('local_document.ipc_glib_client')


class ServerError(RuntimeError):
    pass


class GlibClient(object):
    """IPC class to get access from a client side.

    See http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/Client
    for detailed information.

    """

    def __init__(self, online):
        self._socket_file = None
        self._online = online
        self._io_hid = None
        self._reply_queue = collections.deque()

    def close(self):
        if self._socket_file is not None:
            gobject.source_remove(self._io_hid)
            self._io_hid = None
            self._socket_file.close()
            self._socket_file = None
            self._reply_queue.clear()

    def get(self, resource, guid, reply_handler=None, error_handler=None,
            **kwargs):
        return self._call(reply_handler, error_handler,
                resource=resource, cmd='get', guid=guid, **kwargs)

    def get_blob(self, resource, guid, prop,
            reply_handler=None, error_handler=None, **kwargs):
        return self._call(reply_handler, error_handler,
                resource=resource, cmd='get_blob', guid=guid, prop=prop,
                **kwargs)

    def _call(self, reply_handler, error_handler, data=None, **request):
        if self._socket_file is None:
            ipc.rendezvous()
            # pylint: disable-msg=E1101
            conn = socket.socket(socket.AF_UNIX)
            conn.connect(ipc.path('accept'))
            self._socket_file = SocketFile(conn)
            self._io_hid = gobject.io_add_watch(conn.fileno(),
                    gobject.IO_IN | gobject.IO_HUP, self.__io_cb)

        _logger.debug('Make a call: %r', request)
        self._socket_file.write_message(request)

        if data is not None:
            self._socket_file.write(data)
            _logger.debug('Sent %s bytes of payload', len(data))

        if reply_handler is None:
            reply, error = self._read_message()
            if error is not None:
                raise error
            return reply
        else:
            self._reply_queue.append((reply_handler, error_handler))

    def _read_message(self):
        try:
            reply = self._socket_file.read_message()
            _logger.debug('Got a reply: %r', reply)
            if type(reply) is dict and 'error' in reply:
                return None, ServerError(reply['error'])
            return reply, None
        except Exception, error:
            return None, error

    def __io_cb(self, sender, condition):
        reply, error = self._read_message()
        if reply is None and error is None:
            self.close()
            return False

        def callback(cb, arg):
            try:
                cb(arg)
            except Exception:
                util.exception(_logger, _('Callback failed'))

        if self._reply_queue:
            reply_handler, error_handler = self._reply_queue.popleft()
            if error is None:
                callback(reply_handler, reply)
            else:
                if error_handler is None:
                    _logger.error(_('Failed to get reply: %s'), error)
                else:
                    callback(error_handler, error)
        else:
            _logger.error(_('Got %r reply for empty queue'), reply)

        return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
