# Copyright (C) 2013 Aleksey Lim
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
import base64
import logging
from urlparse import urlsplit
from Cookie import SimpleCookie
from os.path import join

from sugar_network import db, client, node
from sugar_network.node import sync, stats_user, files, volume, downloads
from sugar_network.node.commands import NodeCommands
from sugar_network.toolkit import cachedir, util, enforce


_logger = logging.getLogger('node.master')


class MasterCommands(NodeCommands):

    def __init__(self, volume_, guid=None):
        if not guid:
            guid = urlsplit(client.api_url.value).netloc
        NodeCommands.__init__(self, True, guid, volume_)

        self._pulls = {
            'pull': lambda layer, seq, out_seq=None:
                ('diff', None, volume.diff(self.volume, seq, out_seq, layer)),
            'files_pull': lambda layer, seq, out_seq=None:
                ('files_diff', None, self._files.diff(seq, out_seq)),
            }

        self._pull_queue = downloads.Pool(join(cachedir.value, 'pulls'))
        self._files = None

        if node.files_root.value:
            self._files = files.Index(node.files_root.value,
                    join(volume_.root, 'files.index'), volume_.seqno)

    @db.volume_command(method='POST', cmd='sync',
            permissions=db.ACCESS_AUTH)
    def sync(self, request):
        reply, cookie = self._push(sync.decode(request.content_stream))
        for op, layer, seq in cookie:
            reply.append(self._pulls[op](layer, seq))
        return sync.encode(src=self.guid, *reply)

    @db.volume_command(method='POST', cmd='push')
    def push(self, request, response):
        reply, cookie = self._push(sync.package_decode(request.content_stream))
        # Read passed cookie only after excluding `merged_seq`.
        # If there is `pull` out of currently pushed packet, excluding
        # `merged_seq` should not affect it.
        cookie.update(_Cookie(request))
        cookie.store(response)
        return sync.package_encode(src=self.guid, *reply)

    @db.volume_command(method='GET', cmd='pull',
            mime_type='application/octet-stream',
            arguments={'accept_length': db.to_int})
    def pull(self, request, response, accept_length=None):
        cookie = _Cookie(request)
        if not cookie:
            _logger.warning('Requested full dump in pull command')
            cookie.append(('pull', None, util.Sequence([[1, None]])))
            cookie.append(('files_pull', None, util.Sequence([[1, None]])))

        reply = None
        for pull_key in cookie:
            op, layer, seq = pull_key

            pull = self._pull_queue.get(pull_key)
            if pull is not None:
                if not pull.ready:
                    continue
                if not pull.tag:
                    self._pull_queue.remove(pull_key)
                    cookie.remove(pull_key)
                    continue
                if accept_length is None or pull.length <= accept_length:
                    _logger.debug('Found ready to use %r', pull)
                    if pull.complete:
                        cookie.remove(pull_key)
                    else:
                        seq.exclude(pull.tag)
                    reply = pull.open()
                    break
                _logger.debug('Existing %r is too big, will recreate', pull)
                self._pull_queue.remove(pull_key)

            out_seq = util.Sequence()
            pull = self._pull_queue.set(pull_key, out_seq,
                    sync.sneakernet_encode,
                    [self._pulls[op](layer, seq, out_seq)],
                    limit=accept_length, src=self.guid)
            _logger.debug('Start new %r', pull)

        if reply is None:
            if cookie:
                _logger.debug('No ready pulls')
                # TODO Might be useful to set meaningful value here
                cookie.delay = node.pull_timeout.value
            else:
                _logger.debug('Nothing to pull')

        cookie.store(response)
        return reply

    def _push(self, stream):
        reply = []
        cookie = _Cookie()
        pull_seq = None
        merged_seq = util.Sequence([])

        for packet in stream:
            src = packet['src']
            enforce(packet['dst'] == self.guid, 'Misaddressed packet')

            if packet.name == 'pull':
                pull_seq = cookie['pull', packet['layer']]
                pull_seq.include(packet['sequence'])
            elif packet.name == 'files_pull':
                if self._files is not None:
                    cookie['files_pull'].include(packet['sequence'])
            elif packet.name == 'diff':
                seq, ack_seq = volume.merge(self.volume, packet)
                reply.append(('ack', {
                    'ack': ack_seq,
                    'sequence': seq,
                    'dst': src,
                    }, None))
                merged_seq.include(ack_seq)
            elif packet.name == 'stats_diff':
                reply.append(('stats_ack', {
                    'sequence': stats_user.merge(packet),
                    'dst': src,
                    }, None))

        if pull_seq is not None:
            pull_seq.exclude(merged_seq)

        return reply, cookie


class _Cookie(list):

    def __init__(self, request=None):
        list.__init__(self)
        if request is not None:
            self.update(self._get_cookie(request, 'sugar_network_sync') or [])
        self.delay = 0

    def update(self, that):
        for op, layer, seq in that:
            self[op, layer].include(seq)

    def store(self, response):
        if self:
            _logger.debug('Postpone %r in cookie', self)
            to_store = base64.b64encode(json.dumps(self))
            self._set_cookie(response, 'sugar_network_sync', to_store)
            self._set_cookie(response, 'sugar_network_delay', self.delay)
        else:
            self._unset_cookie(response, 'sugar_network_sync')
            self._unset_cookie(response, 'sugar_network_delay')

    def __getitem__(self, key):
        if not isinstance(key, tuple):
            key = (key, None)
        for op, layer, seq in self:
            if (op, layer) == key:
                return seq
        seq = util.Sequence()
        self.append(key + (seq,))
        return seq

    def _get_cookie(self, request, name):
        cookie_str = request.environ.get('HTTP_COOKIE')
        if not cookie_str:
            return
        cookie = SimpleCookie()
        cookie.load(cookie_str)
        if name not in cookie:
            return
        value = cookie.get(name).value
        if value != 'unset_%s' % name:
            return json.loads(base64.b64decode(value))

    def _set_cookie(self, response, name, value, age=3600):
        response.setdefault('Set-Cookie', [])
        cookie = '%s=%s; Max-Age=%s; HttpOnly' % (name, value, age)
        response['Set-Cookie'].append(cookie)

    def _unset_cookie(self, response, name):
        self._set_cookie(response, name, 'unset_%s' % name, 0)
