# Copyright (C) 2013-2014 Aleksey Lim
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
from urlparse import urlsplit

from sugar_network import toolkit
from sugar_network.model.post import Post
from sugar_network.model.report import Report
from sugar_network.node import obs, model
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.router import route, ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, packets, pylru, ranges, enforce


RESOURCES = (model.User, model.Context, Post, Report)

_logger = logging.getLogger('node.master')


class MasterRoutes(NodeRoutes):

    def __init__(self, master_api, **kwargs):
        NodeRoutes.__init__(self, urlsplit(master_api).netloc, **kwargs)
        self._pulls = pylru.lrucache(1024)

    @route('POST', cmd='sync', arguments={'accept_length': int})
    def sync(self, accept_length):
        return packets.encode(self._push() + (self._pull() or []),
                limit=accept_length, header={'from': self.guid},
                on_complete=this.cookie.clear)

    @route('POST', cmd='push')
    def push(self):
        return packets.encode(self._push(), header={'from': self.guid})

    @route('GET', cmd='pull', arguments={'accept_length': int})
    def pull(self, accept_length):
        reply = self._pull()
        if reply is None:
            return None
        return packets.encode(reply, limit=accept_length,
                header={'from': self.guid}, on_complete=this.cookie.clear)

    @route('PUT', ['context', None], cmd='presolve',
            acl=ACL.AUTH, mime_type='application/json')
    def presolve(self):
        aliases = this.volume['context'].get(this.request.guid)['aliases']
        enforce(aliases, http.BadRequest, 'Nothing to presolve')
        return obs.presolve(None, aliases, this.volume.blobs.path('packages'))

    def status(self):
        result = NodeRoutes.status(self)
        result['mode'] = 'master'
        return result

    def _push(self):
        cookie = this.cookie
        reply = []

        for packet in packets.decode(
                this.request.content, this.request.content_length):
            sender = packet['from']
            enforce(packet['to'] == self.guid, http.BadRequest,
                    'Misaddressed packet')
            if packet.name == 'push':
                seqno, push_r = model.patch_volume(packet)
                ack_r = [] if seqno is None else [[seqno, seqno]]
                ack = {'ack': ack_r, 'ranges': push_r, 'to': sender}
                reply.append(('ack', ack, None))
                cookie.setdefault('ack', {}) \
                      .setdefault(sender, []) \
                      .append((push_r, ack_r))
            elif packet.name == 'pull':
                cookie.setdefault('ack', {}).setdefault(sender, [])
                ranges.include(cookie.setdefault('pull', []), packet['ranges'])
            elif packet.name == 'request':
                cookie.setdefault('request', []).append(packet.header)

        return reply

    def _pull(self):
        processed = this.cookie.get('id')
        if processed in self._pulls:
            cookie = this.cookie = self._pulls[processed]
            if not cookie:
                return None
        else:
            cookie = this.cookie
            cookie['id'] = toolkit.uuid()
            self._pulls[cookie['id']] = cookie

        pull_r = cookie.get('pull')
        if not pull_r:
            return []

        reply = []
        exclude = []
        acks = cookie.get('ack')
        if acks:
            acked = {}
            for req in cookie.get('request') or []:
                ack_r = None
                for push_r, ack_r in acks.get(req['origin']) or []:
                    if req['ranges'] == push_r:
                        break
                else:
                    continue
                ranges.include(acked.setdefault(req['from'], []), ack_r)
                reply.append(('ack', {'to': req['from'], 'ack': ack_r}, []))
            for node, ack_ranges in acks.items():
                acked_r = acked.setdefault(node, [])
                for __, i in ack_ranges:
                    ranges.include(acked_r, i)
            r = reduce(lambda x, y: ranges.intersect(x, y), acked.values())
            ranges.include(exclude, r)

        push = model.diff_volume(pull_r, exclude, one_way=True, files=[''])
        reply.append(('push', None, push))

        return reply
