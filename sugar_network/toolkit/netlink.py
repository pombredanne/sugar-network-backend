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

"""Linux Netlink integration.

$Repo: git://git.sugarlabs.org/alsroot/codelets.git$
$File: src/netlink.py$
$Date: 2012-06-28$

"""
import os
import socket
import struct
import logging
from gettext import gettext as _


# RTnetlink multicast groups - backwards compatibility for userspace
RTMGRP_LINK = 1
RTMGRP_NOTIFY = 2
RTMGRP_NEIGH = 4
RTMGRP_TC = 8
RTMGRP_IPV4_IFADDR = 0x10
RTMGRP_IPV4_MROUTE = 0x20
RTMGRP_IPV4_ROUTE = 0x40
RTMGRP_IPV4_RULE = 0x80
RTMGRP_IPV6_IFADDR = 0x100
RTMGRP_IPV6_MROUTE = 0x200
RTMGRP_IPV6_ROUTE = 0x400
RTMGRP_IPV6_IFINFO = 0x800
RTMGRP_DECnet_IFADDR = 0x1000
RTMGRP_DECnet_ROUTE = 0x4000
RTMGRP_IPV6_PREFIX = 0x20000

#: Message type, Nothing
NLMSG_NOOP = 0x1
#: Message type, Error
NLMSG_ERROR = 0x2
#: Message type, End of a dump
NLMSG_DONE = 0x3
#: Message type, Data lost
NLMSG_OVERRUN = 0x4


_MESSAGE_MAX_SIZE = 16384

_logger = logging.getLogger('netlink')


class Netlink(object):

    def __init__(self, proto, groups):
        _logger.info(_('Start reading Netlink messages'))

        self._socket = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, proto)
        self._socket.bind((0, groups))

    @property
    def fileno(self):
        if self._socket is not None:
            return self._socket.fileno()

    @property
    def closed(self):
        return self._socket is None

    def close(self):
        if self._socket is None:
            return

        self._socket.close()
        self._socket = None

        _logger.info(_('Stop reading Netlink messages'))

    def read(self):
        if self.closed:
            raise RuntimeError(_('Netlink is closed'))

        data = self._socket.recv(_MESSAGE_MAX_SIZE)
        if not data:
            self.close()
            return

        msg = Message()
        __, msg.type, msg.flags, msg.seqno, msg.pid = \
                struct.unpack('IHHII', data[:16])
        msg.payload = data[16:]

        _logger.debug('Got message: %r', msg)

        if msg.type == NLMSG_ERROR:
            errno = - struct.unpack('i', msg.payload[:4])[0]
            if errno:
                error = OSError(_('Netlink error, %s(%d)') % \
                        (os.strerror(errno), errno))
                error.errno = errno
                raise error

        return msg

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class Message(object):

    type = None
    flags = None
    seqno = 0
    pid = -1
    payload = None

    def __repr__(self):
        return '<netlink.Message type=%r flags=0x%x seqno=%r pid=%r>' % \
                (self.type, self.flags, self.seqno, self.pid)


if __name__ == '__main__':
    import select

    logging.basicConfig(level=logging.DEBUG)

    with Netlink(socket.NETLINK_ROUTE,
            RTMGRP_IPV4_ROUTE | RTMGRP_IPV6_ROUTE | RTMGRP_NOTIFY) as netlink:
        poll = select.poll()
        poll.register(netlink.fileno(), select.POLLIN)
        while poll.poll():
            netlink.read()
