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

import os
import json
import base64
import logging
import hashlib
import tempfile
from Cookie import SimpleCookie
from os.path import exists, join

from pylru import lrucache

import active_document as ad
from sugar_network import node
from sugar_network.toolkit.sneakernet import InPacket, OutBufferPacket, \
        OutPacket, DiskFull
from sugar_network.toolkit.collection import Sequence
from active_toolkit import coroutine, util, enforce


_PULL_QUEUE_SIZE = 256
_DEFAULT_MASTER_GUID = 'api-testing.network.sugarlabs.org'

_logger = logging.getLogger('node.commands')


class NodeCommands(ad.VolumeCommands):

    def __init__(self, volume, subscriber=None):
        ad.VolumeCommands.__init__(self, volume)
        self._subscriber = subscriber
        self._is_master = False

        node_path = join(volume.root, 'node')
        master_path = join(volume.root, 'master')

        if exists(node_path):
            with file(node_path) as f:
                self._guid = f.read().strip()
        elif exists(master_path):
            with file(master_path) as f:
                self._guid = f.read().strip()
            self._is_master = True
        else:
            self._guid = ad.uuid()
            with file(node_path, 'w') as f:
                f.write(self._guid)

        if not self._is_master and not exists(master_path):
            with file(master_path, 'w') as f:
                f.write(_DEFAULT_MASTER_GUID)

    @ad.volume_command(method='GET')
    def hello(self, response):
        response.content_type = 'text/html'
        return _HELLO_HTML

    @ad.volume_command(method='GET', cmd='stat')
    def stat(self):
        return {'guid': self._guid,
                'master': self._is_master,
                'seqno': self.volume.seqno.value,
                }

    @ad.volume_command(method='POST', cmd='subscribe',
            permissions=ad.ACCESS_AUTH)
    def subscribe(self):
        enforce(self._subscriber is not None, 'Subscription is disabled')
        return self._subscriber.new_ticket()

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid):
        # Servers data should not be deleted immediately
        # to let master-node synchronization possible
        directory = self.volume[document]
        directory.update(guid, {'layer': ['deleted']})

    @ad.directory_command(method='GET')
    def find(self, document, request, offset=None, limit=None, query=None,
            reply=None, order_by=None, **kwargs):
        if limit is None:
            limit = node.find_limit.value
        elif limit > node.find_limit.value:
            _logger.warning('The find limit is restricted to %s',
                    node.find_limit.value)
            limit = node.find_limit.value
        return ad.VolumeCommands.find(self, document, request, offset, limit,
                query, reply, order_by, **kwargs)

    def resolve(self, request):
        cmd = ad.VolumeCommands.resolve(self, request)
        if cmd is None:
            return

        if cmd.permissions & ad.ACCESS_AUTH:
            enforce(request.principal is not None, node.Unauthorized,
                    'User is not authenticated')

        if cmd.permissions & ad.ACCESS_AUTHOR and 'guid' in request:
            doc = self.volume[request['document']].get(request['guid'])
            enforce(request.principal in doc['user'], ad.Forbidden,
                    'Operation is permitted only for authors')

        return cmd

    def before_create(self, request, props):
        if request['document'] == 'user':
            props['guid'], props['pubkey'] = _load_pubkey(props['pubkey'])
            props['user'] = [props['guid']]
        else:
            props['user'] = [request.principal]
            self._set_author(props)
        ad.VolumeCommands.before_create(self, request, props)

    def before_update(self, request, props):
        if 'user' in props:
            self._set_author(props)
        ad.VolumeCommands.before_update(self, request, props)

    def _set_author(self, props):
        users = self.volume['user']
        authors = []
        for user_guid in props['user']:
            if not users.exists(user_guid):
                _logger.warning('No %r user to set author property',
                        user_guid)
                continue
            user = users.get(user_guid)
            if user['name']:
                authors.append(user['name'])
        props['author'] = authors


class MasterCommands(NodeCommands):

    def __init__(self, volume, subscriber=None):
        NodeCommands.__init__(self, volume, subscriber)
        self._pull_queue = lrucache(_PULL_QUEUE_SIZE)

    @ad.volume_command(method='POST', cmd='push')
    def push(self, request, response):
        with InPacket(stream=request) as in_packet:
            enforce('src' in in_packet.header and \
                    in_packet.header['src'] != self._guid,
                    'Misaddressed packet')
            enforce('dst' in in_packet.header and \
                    in_packet.header['dst'] == self._guid,
                    'Misaddressed packet')

            out_packet = OutBufferPacket(src=self._guid,
                    dst=in_packet.header['src'],
                    filename='ack.' + in_packet.header.get('filename'))
            pull_to_forward = Sequence()
            pushed = Sequence()
            merged = Sequence()

            for record in in_packet.records(dst=self._guid):
                cmd = record.get('cmd')
                if cmd == 'sn_push':
                    seqno = self.volume.merge(record)
                    merged.include(seqno, seqno)

                elif cmd == 'sn_commit':
                    _logger.debug('Merged %r commit', record)
                    pushed.include(record['sequence'])

                elif cmd == 'sn_pull':
                    # Nodes create singular packet, forward PULLs
                    # to process them in `pull()` later
                    pull_to_forward.include(record['sequence'])

            enforce(not merged or pushed,
                    '"sn_push" record without "sn_commit"')
            if pushed:
                out_packet.push(cmd='sn_ack', sequence=pushed, merged=merged)

            pull_to_forward.exclude(merged)
            pull_to_forward.include(_cookie_get(request, 'sn_pull'))
            if pull_to_forward:
                _logger.debug('Forward %r pull in cookies', pull_to_forward)
                _cookie_set(response, 'sn_pull', pull_to_forward)
            else:
                _cookie_unset(response, 'sn_pull')

            response.content_type = out_packet.content_type
            if not out_packet.empty:
                return out_packet.pop()

    @ad.volume_command(method='POST', cmd='pull',
            mime_type='application/octet-stream')
    def pull(self, request, response, accept_length=None):
        pull_seq = Sequence(_cookie_get(request, 'sn_pull'))
        if pull_seq:
            _logger.debug('Reuse %r pull from cookies', pull_seq)

        if request.content_length:
            with InPacket(stream=request) as in_packet:
                enforce(in_packet.header.get('src') != self._guid,
                        'Misaddressed packet')
                enforce('dst' in in_packet.header and \
                        in_packet.header['dst'] == self._guid,
                        'Misaddressed packet')
                for record in in_packet.records():
                    if record.get('cmd') == 'sn_pull':
                        pull_seq.include(record['sequence'])

        accept_length = _to_int('accept_length', accept_length)
        return self._pull(response, pull_seq, accept_length, False)

    @ad.volume_command(method='GET', cmd='clone',
            mime_type='application/octet-stream')
    def clone(self, request, response, accept_length=None):
        pull_seq = Sequence()
        pull_seq.include(1, None)
        accept_length = _to_int('accept_length', accept_length)
        return self._pull(response, pull_seq, accept_length, True)

    def _pull(self, response, pull_seq, accept_length, clone):
        pull_hash = hashlib.sha1(json.dumps(pull_seq)).hexdigest()

        if pull_hash in self._pull_queue:
            pull = self._pull_queue[pull_hash]
            _logger.debug('Reuse existing %r pull', pull_seq)
        else:
            pull = self._pull_queue[pull_hash] = _Pull(pull_hash, self.volume,
                    pull_seq, clone, src=self._guid,
                    seqno=self.volume.seqno.value, limit=accept_length)
            _logger.debug('Preparing %r pull', pull_seq)

        if pull.exception is not None:
            del self._pull_queue[pull_hash]
            raise pull.exception

        response.content_type = pull.content_type
        content = None

        if pull.ready:
            _logger.debug('Response with ready %r pull', pull_seq)
            _cookie_unset(response, 'sn_delay')
            pull_seq = pull.sequence
            content = pull.content
        else:
            _logger.debug('Pull %r is not yet ready', pull_seq)
            _cookie_set_raw(response, 'sn_delay',
                    'sn_delay:%s' % pull.seconds_remained)

        if pull_seq:
            _logger.debug('Postpone %r pull in cookies', pull_seq)
            _cookie_set(response, 'sn_pull', pull_seq)
        else:
            _cookie_unset(response, 'sn_pull')

        return content


class _Pull(object):

    def __init__(self, pull_hash, volume, sequence, clone, **packet_args):
        self.sequence = sequence
        self.exception = None
        self.seconds_remained = 0
        self.content_type = None
        self.path = join(node.tmpdir.value, pull_hash + '.pull')
        self._job = None

        if exists(self.path):
            try:
                with InPacket(self.path) as packet:
                    self.content_type = packet.content_type
                _logger.debug('Pickup cached %r pull', self.path)
            except Exception:
                util.exception('Cannot open cached %r pull, will recreate')
                os.unlink(self.path)

        if not exists(self.path):
            packet = OutPacket(stream=file(self.path, 'wb+'), **packet_args)
            self.content_type = packet.content_type
            # TODO Might be useful to set meaningful value here
            self.seconds_remained = 30
            self._job = coroutine.spawn(self._diff, volume, clone, packet)

    @property
    def ready(self):
        # pylint: disable-msg=E1101
        return self._job is None or self._job.dead

    @property
    def content(self):
        if exists(self.path):
            return file(self.path, 'rb')

    def __del__(self):
        if exists(self.path):
            os.unlink(self.path)

    def _diff(self, volume, clone, packet):
        try:
            volume.diff(self.sequence, packet, clone=clone)
        except DiskFull:
            pass
        except Exception, exception:
            util.exception('Error while preparing pull')
            self.exception = exception
            packet.clear()
        else:
            self.sequence.clear()
        self.seconds_remained = 0
        packet.close()


def _cookie_get(request, name):
    cookie_str = request.environ.get('HTTP_COOKIE')
    if not cookie_str:
        return
    cookie = SimpleCookie()
    cookie.load(cookie_str)
    if name not in cookie:
        return
    value = cookie.get(name).value
    if value != '%s_unset' % name:
        return json.loads(base64.b64decode(value))


def _cookie_set(response, name, value):
    value = base64.b64encode(json.dumps(value))
    _cookie_set_raw(response, name, value)


def _cookie_unset(response, name):
    _cookie_set_raw(response, name, '%s_unset' % name, 1)


def _cookie_set_raw(response, name, value, age=3600):
    response.setdefault('Set-Cookie', [])
    cookie = '%s=%s; Max-Age=%s; HttpOnly' % (name, value, age)
    response['Set-Cookie'].append(cookie)


def _load_pubkey(pubkey):
    pubkey = pubkey.strip()
    try:
        with tempfile.NamedTemporaryFile() as key_file:
            key_file.file.write(pubkey)
            key_file.file.flush()
            # SSH key needs to be converted to PKCS8 to ket M2Crypto read it
            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', key_file.name, '-e', '-m', 'PKCS8'])
    except Exception:
        message = 'Cannot read DSS public key gotten for registeration'
        util.exception(message)
        if node.trust_users.value:
            logging.warning('Failed to read registration pubkey, ' \
                    'but we trust users')
            # Keep SSH key for further converting to PKCS8
            pubkey_pkcs8 = pubkey
        else:
            raise ad.Forbidden(message)

    return str(hashlib.sha1(pubkey.split()[1]).hexdigest()), pubkey_pkcs8


def _to_int(name, value):
    if isinstance(value, basestring):
        enforce(value.isdigit(),
                'Argument %r should be an integer value', name)
        value = int(value)
    return value


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Consult <a href="http://wiki.sugarlabs.org/go/Platform_Team/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
