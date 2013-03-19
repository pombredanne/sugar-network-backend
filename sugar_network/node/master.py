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

import re
import time
import json
import base64
import logging
from Cookie import SimpleCookie
from os.path import join

from sugar_network import db, node
from sugar_network.node import sync, stats_user, files, volume, downloads, obs
from sugar_network.node.commands import NodeCommands
from sugar_network.toolkit import cachedir, coroutine, util, enforce


_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')

_logger = logging.getLogger('node.master')


class MasterCommands(NodeCommands):

    def __init__(self, guid, volume_):
        NodeCommands.__init__(self, guid, volume_)

        self._pulls = {
            'pull': lambda **kwargs:
                ('diff', None, volume.diff(self.volume, **kwargs)),
            'files_pull': lambda **kwargs:
                ('files_diff', None, self._files.diff(**kwargs)),
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
        exclude_seq = None
        if len(cookie.sent) == 1:
            exclude_seq = cookie.sent.values()[0]
        for op, layer, seq in cookie:
            reply.append(self._pulls[op](in_seq=seq,
                exclude_seq=exclude_seq, layer=layer))
        return sync.encode(reply, src=self.guid)

    @db.volume_command(method='POST', cmd='push')
    def push(self, request, response):
        reply, cookie = self._push(sync.package_decode(request.content_stream))
        # Read passed cookie only after excluding `merged_seq`.
        # If there is `pull` out of currently pushed packet, excluding
        # `merged_seq` should not affect it.
        cookie.update(_Cookie(request))
        cookie.store(response)
        return sync.package_encode(reply, src=self.guid)

    @db.volume_command(method='GET', cmd='pull',
            mime_type='application/octet-stream',
            arguments={'accept_length': db.to_int})
    def pull(self, request, response, accept_length=None):
        cookie = _Cookie(request)
        if not cookie:
            _logger.warning('Requested full dump in pull command')
            cookie.append(('pull', None, util.Sequence([[1, None]])))
            cookie.append(('files_pull', None, util.Sequence([[1, None]])))

        exclude_seq = None
        if len(cookie.sent) == 1:
            exclude_seq = util.Sequence(cookie.sent.values()[0])

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
                    [self._pulls[op](in_seq=seq, out_seq=out_seq,
                        exclude_seq=exclude_seq, layer=layer)],
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

    def before_create(self, request, props):
        implement = props.get('implement')
        if implement:
            implement = implement[0]
            enforce(not self.volume[request['document']].exists(implement),
                    'Document already exists')
            enforce(_GUID_RE.match(implement) is not None, 'Malformed GUID')
            props['guid'] = implement
        NodeCommands.before_create(self, request, props)

    def after_post(self, doc):
        if doc.metadata.name == 'context':
            shift_implementations = ('dependencies' in doc.props)
            if 'aliases' in doc.props:
                # TODO Already launched job should be killed
                coroutine.spawn(self._resolve_aliases, doc)
                shift_implementations = True
            if shift_implementations and not doc.is_new:
                # Shift mtime to invalidate solutions
                self.volume['implementation'].mtime = int(time.time())
        NodeCommands.after_post(self, doc)

    def _push(self, stream):
        reply = []
        cookie = _Cookie()

        for packet in stream:
            src = packet['src']
            enforce(packet['dst'] == self.guid, 'Misaddressed packet')

            if packet.name == 'pull':
                pull_seq = cookie['pull', packet['layer'] or None]
                pull_seq.include(packet['sequence'])
                cookie.sent.setdefault(src, util.Sequence())
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
                sent_seq = cookie.sent.setdefault(src, util.Sequence())
                sent_seq.include(ack_seq)
            elif packet.name == 'stats_diff':
                reply.append(('stats_ack', {
                    'sequence': stats_user.merge(packet),
                    'dst': src,
                    }, None))

        return reply, cookie

    def _resolve_aliases(self, doc):
        packages = {}
        for repo in obs.get_repos():
            alias = doc['aliases'].get(repo['distributor_id'])
            if not alias:
                continue
            package = packages[repo['name']] = {}
            for kind in ('binary', 'devel'):
                obs_fails = []
                for to_resolve in alias.get(kind) or []:
                    if not to_resolve:
                        continue
                    try:
                        for arch in repo['arches']:
                            obs.resolve(repo['name'], arch, to_resolve)
                    except Exception, error:
                        _logger.warning('Failed to resolve %r on %s',
                                to_resolve, repo['name'])
                        obs_fails.append(str(error))
                        continue
                    package[kind] = to_resolve
                    break
                else:
                    package['status'] = '; '.join(obs_fails)
                    break
            else:
                if 'binary' in package:
                    package['status'] = 'success'
                else:
                    package['status'] = 'no packages to resolve'

        if packages != doc['packages']:
            doc.request.call('PUT', document='context', guid=doc.guid,
                    content={'packages': packages})
        obs.presolve(doc['aliases'])


class _Cookie(list):

    def __init__(self, request=None):
        list.__init__(self)

        self.sent = {}
        self.delay = 0

        if request is not None:
            self.update(self._get_cookie(request, 'sugar_network_pull') or [])
            self.sent = self._get_cookie(request, 'sugar_network_sent') or {}

    def __repr__(self):
        return '<Cookie pull=%s sent=%r>' % (list.__repr__(self), self.sent)

    def update(self, that):
        for op, layer, seq in that:
            self[op, layer].include(seq)

    def store(self, response):
        response['Set-Cookie'] = []
        if self:
            _logger.debug('Postpone %r in cookie', self)
            self._set_cookie(response, 'sugar_network_pull',
                    base64.b64encode(json.dumps(self)))
            self._set_cookie(response, 'sugar_network_sent',
                    base64.b64encode(json.dumps(self.sent)))
            self._set_cookie(response, 'sugar_network_delay', self.delay)
        else:
            self._unset_cookie(response, 'sugar_network_pull')
            self._unset_cookie(response, 'sugar_network_sent')
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
        cookie = '%s=%s; Max-Age=%s; HttpOnly' % (name, value, age)
        response['Set-Cookie'].append(cookie)

    def _unset_cookie(self, response, name):
        self._set_cookie(response, name, 'unset_%s' % name, 0)