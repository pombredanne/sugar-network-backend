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
import hashlib
import logging
from Cookie import SimpleCookie
from os.path import exists, join

from pylru import lrucache

import active_document as ad
from sugar_network import node, toolkit
from sugar_network.toolkit.sneakernet import InPacket, OutBufferPacket, \
        OutPacket, DiskFull
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit.files_sync import Seeders
from sugar_network.node import stats
from active_toolkit import coroutine, util, enforce


_PULL_QUEUE_SIZE = 256

_logger = logging.getLogger('node.sync_master')


class SyncCommands(object):

    _guid = None
    volume = None

    def __init__(self):
        self._file_syncs = Seeders(node.sync_dirs.value,
                join(node.data_root.value, 'sync'), self.volume.seqno)
        self._pull_queue = lrucache(_PULL_QUEUE_SIZE,
                lambda key, pull: pull.unlink())

    @ad.volume_command(method='POST', cmd='push')
    def push(self, request, response):
        with InPacket(stream=request) as in_packet:
            enforce('src' in in_packet.header and
                    in_packet.header['src'] != self._guid,
                    'Misaddressed packet')
            enforce('dst' in in_packet.header and
                    in_packet.header['dst'] == self._guid,
                    'Misaddressed packet')

            out_packet = OutBufferPacket(src=self._guid,
                    dst=in_packet.header['src'],
                    filename='ack.' + in_packet.header.get('filename'))
            pushed = Sequence()
            merged = Sequence()
            cookie = _Cookie()
            stats_pushed = {}

            for record in in_packet.records(dst=self._guid):
                cmd = record.get('cmd')
                if cmd == 'sn_push':
                    seqno = self.volume.merge(record)
                    merged.include(seqno, seqno)
                elif cmd == 'sn_commit':
                    _logger.debug('Merged %r commit', record)
                    pushed.include(record['sequence'])
                elif cmd == 'sn_pull':
                    cookie['sn_pull'].include(record['sequence'])
                elif cmd == 'files_pull':
                    cookie[record['directory']].include(record['sequence'])
                elif cmd == 'stats_push':
                    db = record['db']
                    user = record['user']

                    rrd = stats.get_rrd(user)
                    rrd.put(db, record['values'], record['timestamp'])

                    user_seq = stats_pushed.setdefault(user, {})
                    db_seq = user_seq.setdefault(db, Sequence())
                    db_seq.include(record['sequence'])

            enforce(not merged or pushed,
                    '"sn_push" record without "sn_commit"')
            if pushed:
                out_packet.push(cmd='sn_ack', sequence=pushed, merged=merged)
            if stats_pushed:
                out_packet.push(cmd='stats_ack', sequence=stats_pushed)

            cookie['sn_pull'].exclude(merged)
            # Read passed cookie only after excluding `merged`.
            # If there is sn_pull out of currently pushed packet, excluding
            # `merged` should not affect it.
            cookie.include(_Cookie(request))
            cookie.store(response)

            response.content_type = out_packet.content_type
            if not out_packet.empty:
                return out_packet.pop()

    @ad.volume_command(method='GET', cmd='pull',
            mime_type='application/octet-stream',
            arguments={'accept_length': ad.to_int})
    def pull(self, request, response, accept_length=None, **pulls):
        cookie = _Cookie(request)
        for key, seq in pulls.items():
            cookie[key][:] = json.loads(seq)
        if not cookie:
            _logger.debug('Clone full dump')
            cookie['sn_pull'].include(1, None)

        pull_key = hashlib.sha1(json.dumps(cookie)).hexdigest()
        pull = None
        content = None

        if pull_key in self._pull_queue:
            pull = self._pull_queue[pull_key]
            if accept_length is not None and pull.length > accept_length:
                _logger.debug('Cached %r pull is bigger than requested '
                        'length, will recreate it', cookie)
                pull.unlink()
                del self._pull_queue[pull_key]
                pull = None

        if pull is None:
            pull = self._pull_queue[pull_key] = _Pull(pull_key, cookie,
                    self._pull, src=self._guid, seqno=self.volume.seqno.value,
                    limit=accept_length)

        if pull.exception is not None:
            del self._pull_queue[pull_key]
            raise pull.exception

        if pull.ready:
            _logger.debug('Response with ready %r pull', cookie)
            content = pull.content
            response.content_type = pull.content_type
            cookie = pull.cookie
        else:
            _logger.debug('Pull %r is not yet ready', cookie)
            cookie.delay = pull.seconds_remained

        cookie.store(response)
        return content

    def _pull(self, cookie, packet):
        sn_pull = cookie['sn_pull']
        if sn_pull:
            self.volume.diff(sn_pull, packet)

        for directory, seq in cookie.items():
            sync = self._file_syncs.get(directory)
            if sync is None or not sync.pending(seq):
                continue
            sync.pull(seq, packet)


class _Pull(object):

    def __init__(self, pull_key, cookie, pull_cb, **packet_args):
        self.cookie = cookie
        self.exception = None
        self.seconds_remained = 0
        self.content_type = None
        self._path = join(toolkit.tmpdir.value, pull_key + '.pull')
        self._job = None

        if exists(self._path):
            try:
                with InPacket(self._path) as packet:
                    self.content_type = packet.content_type
                    self.cookie = _Cookie()
                    self.cookie.update(packet.header['cookie'])
            except Exception:
                util.exception('Cannot open cached packet for %r, recreate',
                        self._path)
                os.unlink(self._path)

        if not exists(self._path):
            packet = OutPacket(stream=file(self._path, 'wb+'), **packet_args)
            self.content_type = packet.content_type
            # TODO Might be useful to set meaningful value here
            self.seconds_remained = node.pull_timeout.value
            self._job = coroutine.spawn(self._pull, packet, pull_cb)

    @property
    def ready(self):
        # pylint: disable-msg=E1101
        return self._job is None or self._job.dead

    @property
    def content(self):
        if exists(self._path):
            return file(self._path, 'rb')

    @property
    def length(self):
        if exists(self._path):
            return os.stat(self._path).st_size

    def unlink(self):
        if self._job is not None:
            self._job.kill()
        if exists(self._path):
            _logger.debug('Eject %r pull from queue', self._path)
            os.unlink(self._path)

    def _pull(self, packet, cb):
        try:
            cb(self.cookie, packet)
        except DiskFull:
            pass
        except Exception, exception:
            util.exception('Error while making %r pull', self.cookie)
            self.exception = exception
            self.unlink()
        else:
            self.cookie.clear()
        packet.header['cookie'] = self.cookie
        packet.close()


class _Cookie(dict):

    def __init__(self, request=None):
        dict.__init__(self)

        if request is not None:
            value = self._get_cookie(request, 'sugar_network_sync')
            for key, seq in (value or {}).items():
                self[key] = Sequence(seq)

        self.delay = 0

    def include(self, cookie):
        for key, seq in cookie.items():
            self[key].include(seq)

    def store(self, response):
        to_store = {}
        for key, value in self.items():
            if value:
                to_store[key] = value

        if to_store:
            _logger.debug('Postpone %r pull in cookie', to_store)
            to_store = base64.b64encode(json.dumps(to_store))
            self._set_cookie(response, 'sugar_network_sync', to_store)
            self._set_cookie(response, 'sugar_network_delay', self.delay)
        else:
            self._unset_cookie(response, 'sugar_network_sync')
            self._unset_cookie(response, 'sugar_network_delay')

    def __getitem__(self, key):
        seq = self.get(key)
        if seq is None:
            seq = self[key] = Sequence()
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
