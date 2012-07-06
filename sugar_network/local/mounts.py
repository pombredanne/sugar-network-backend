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
import shutil
import logging
from urlparse import urlparse
from os.path import isabs, exists, join
from gettext import gettext as _

import sweets_recipe
import active_document as ad
from sugar_network.toolkit.collection import Sequences, PersistentSequences
from sugar_network.toolkit import crypto, sugar, sneakernet, http
from sugar_network.local import activities
from sugar_network import local, checkin, sugar
from active_toolkit import sockets, util, coroutine, enforce


_DEFAULT_MASTER = 'http://api-testing.network.sugarlabs.org'

_LOCAL_PROPS = {
        'keep': False,
        'keep_impl': 0,
        'position': (-1, -1),
        }

_logger = logging.getLogger('local.mounts')


class _Mount(object):

    def __init__(self):
        self.mountpoint = None
        self.publisher = None
        self._mounted = False

    @property
    def mounted(self):
        return self._mounted

    def set_mounted(self, value):
        if self._mounted == value:
            return
        self._mounted = value
        self.publish({
            'event': 'mount' if value else 'unmount',
            'mountpoint': self.mountpoint,
            'document': '*',
            })

    def publish(self, event):
        if self.publisher is not None:
            # pylint: disable-msg=E1102
            self.publisher(event)


class LocalMount(ad.VolumeCommands, _Mount):

    def __init__(self, volume):
        ad.VolumeCommands.__init__(self, volume)
        _Mount.__init__(self)

        volume.connect(self._events_cb)

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_READ)
        return directory.get(guid).meta(prop)

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        directory = self.volume[document]
        directory.metadata[prop].assert_access(ad.ACCESS_WRITE)
        enforce(isabs(path), _('Path is not absolute'))
        try:
            directory.set_blob(guid, prop, path)
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def before_create(self, request, props):
        props['user'] = [sugar.uid()]
        props['author'] = [sugar.nickname()]
        ad.VolumeCommands.before_create(self, request, props)

    def _events_cb(self, event):
        if 'mountpoint' not in event:
            event['mountpoint'] = self.mountpoint
        self.publish(event)


class HomeMount(LocalMount):

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop, request=None):
        if document == 'context' and prop == 'feed':
            return json.dumps(self._get_feed(request))
        else:
            return LocalMount.get_blob(self, document, guid, prop, request)

    @ad.property_command(method='GET')
    def get_prop(self, document, guid, prop, request, response, seqno=None):
        if document == 'context' and prop == 'feed':
            directory = self.volume[document]
            directory.metadata[prop].assert_access(ad.ACCESS_READ)
            return self._get_feed(request)
        elif document == 'implementation' and prop == 'data':
            path = activities.guid_to_path(guid)
            if not exists(path):
                return None
            dir_info, dir_reader = sockets.encode_directory(path)
            response.content_length = dir_info.content_length
            response.content_type = dir_info.content_type
            return dir_reader
        else:
            return LocalMount.get_prop(self, document, guid, prop, request,
                    response, seqno)

    def _get_feed(self, request):
        feed = {}

        for path in activities.checkins(request['guid']):
            try:
                spec = sweets_recipe.Spec(root=path)
            except Exception:
                util.exception(_logger, _('Failed to read %r spec file'), path)
                continue

            if request.access_level == ad.ACCESS_LOCAL:
                impl_id = spec.root
            else:
                impl_id = activities.path_to_guid(spec.root)

            feed[spec['version']] = {
                    '*-*': {
                        'guid': impl_id,
                        'stability': 'stable',
                        'commands': {
                            'activity': {
                                'exec': spec['Activity', 'exec'],
                                },
                            },
                        },
                    }

        return feed

    def _events_cb(self, event):
        if 'props' in event:
            found_commons = False
            props = event['props']

            for prop in _LOCAL_PROPS.keys():
                if prop not in props:
                    continue
                if prop == 'keep_impl':
                    if props[prop] == 0:
                        self._checkout(event['guid'])
                    elif props[prop] == 1:
                        self._checkin(event['guid'])
                found_commons = True

            if found_commons:
                # These local properties exposed from `_proxy_call` as well
                event['mountpoint'] = '*'

        LocalMount._events_cb(self, event)

    def _checkout(self, guid):
        for path in activities.checkins(guid):
            _logger.info(_('Checkout %r implementation from %r'), guid, path)
            shutil.rmtree(path)

    def _checkin(self, guid):
        for phase, __ in checkin('/', guid, 'activity'):
            # TODO Publish checkin progress
            if phase == 'failure':
                self.publish({
                    'event': 'alert',
                    'mountpoint': self.mountpoint,
                    'severity': 'error',
                    'message': _("Cannot check-in '%s' implementation") % guid,
                    })
                for __ in activities.checkins(guid):
                    keep_impl = 2
                    break
                else:
                    keep_impl = 0
                self.volume['context'].update(guid, {'keep_impl': keep_impl})
                break


class RemoteMount(ad.CommandsProcessor, _Mount):

    def __init__(self, home_volume):
        ad.CommandsProcessor.__init__(self)
        _Mount.__init__(self)

        self._home_volume = home_volume
        self._seqno = {}
        self._remote_volume_guid = None
        self._api_urls = []
        if local.api_url.value:
            self._api_urls.append(local.api_url.value)
        self._connections = coroutine.Pool()

    def call(self, request, response):

        def super_call(request, response):
            try:
                return ad.CommandsProcessor.call(self, request, response)
            except ad.CommandNotFound:
                pass

            method = request.pop('method')
            document = request.pop('document')
            guid = request.pop('guid') if 'guid' in request else None
            prop = request.pop('prop') if 'prop' in request else None

            path = [document]
            if guid:
                path.append(guid)
            if prop:
                path.append(prop)

            return http.request(method, path, data=request.content,
                    params=request, headers={
                        'Content-Type': 'application/json',
                        'Accept-Language': ','.join(request.accept_language),
                        })

        return _proxy_call(self._home_volume, request, response, super_call,
                self.get_blob)

    def set_mounted(self, value):
        if value != self.mounted:
            if value:
                self.mount()
            else:
                self._connections.kill()

    @ad.property_command(method='GET', cmd='get_blob')
    def get_blob(self, document, guid, prop):
        blob_path = join(local.local_root.value, 'cache', document, guid[:2],
                guid, prop)
        meta_path = blob_path + '.meta'
        meta = {}

        def download(seqno):
            mime_type = http.download([document, guid, prop], blob_path, seqno,
                    document == 'implementation' and prop == 'data')
            meta['mime_type'] = mime_type
            meta['seqno'] = self._seqno[document]
            meta['volume'] = self._remote_volume_guid
            with file(meta_path, 'w') as f:
                json.dump(meta, f)

        if exists(meta_path):
            with file(meta_path) as f:
                meta = json.load(f)
            if meta.get('volume') != self._remote_volume_guid:
                download(None)
            elif meta.get('seqno') < self._seqno[document]:
                download(meta['seqno'])
        else:
            download(None)

        if not exists(blob_path):
            return None
        meta['path'] = blob_path
        return meta

    @ad.property_command(method='PUT', cmd='upload_blob')
    def upload_blob(self, document, guid, prop, path, pass_ownership=False):
        enforce(isabs(path), _('Path is not absolute'))

        try:
            with file(path, 'rb') as f:
                http.request('PUT', [document, guid, prop], files={'file': f})
        finally:
            if pass_ownership and exists(path):
                os.unlink(path)

    def mount(self, url=None):
        if url and url not in self._api_urls:
            self._api_urls.append(url)
        if self._api_urls and not self.mounted and not self._connections:
            self._connections.spawn(self._connect)

    def _connect(self):

        def connect(url):
            _logger.debug('Connecting to %r master', url)
            subscription = http.request('POST', [''],
                    params={'cmd': 'subscribe'},
                    headers={'Content-Type': 'application/json'})
            conn = sockets.SocketFile(coroutine.socket())
            conn.connect((urlparse(url).hostname, subscription['port']))
            conn.write_message({'ticket': subscription['ticket']})
            return conn

        def listen_events(url, conn):
            stat = http.request('GET', [], params={'cmd': 'stat'},
                    headers={'Content-Type': 'application/json'})
            for document, props in stat['documents'].items():
                self._seqno[document] = props.get('seqno') or 0
            self._remote_volume_guid = stat.get('guid')

            _logger.info(_('Connected to %r master'), url)
            _Mount.set_mounted(self, True)

            while True:
                coroutine.select([conn.fileno()], [], [])
                event = conn.read_message()
                if event is None:
                    break

                seqno = event.get('seqno')
                if seqno:
                    self._seqno[event['document']] = seqno

                event['mountpoint'] = self.mountpoint
                self.publish(event)

        for url in self._api_urls:
            try:
                conn = connect(url)
            except Exception, error:
                _logger.warning('Cannot connect to %r master: %s', url, error)
                continue
            try:
                listen_events(url, conn)
            except Exception:
                util.exception(_logger, _('Failed to dispatch remote event'))
            finally:
                _logger.info(_('Got disconnected from %r master'), url)
                _Mount.set_mounted(self, False)


class NodeMount(LocalMount):

    def __init__(self, volume, home_volume):
        LocalMount.__init__(self, volume)

        self._home_volume = home_volume
        self._push_seq = PersistentSequences(
                join(volume.root, 'push.sequence'), [1, None], volume.keys())
        self._pull_seq = PersistentSequences(
                join(volume.root, 'pull.sequence'), [1, None], volume.keys())
        self._sync_session = None

        self._node_guid = crypto.ensure_dsa_pubkey(
                sugar.profile_path('owner.key'))

        master_path = join(volume.root, 'master')
        if exists(master_path):
            with file(master_path) as f:
                self._master = f.read().strip()
        else:
            self._master = _DEFAULT_MASTER
            with file(master_path, 'w') as f:
                f.write(self._master)

    def call(self, request, response):
        return _proxy_call(self._home_volume, request, response,
                super(NodeMount, self).call, self.get_blob)

    def sync(self, path, accept_length=None, push_sequence=None, session=None):
        to_push_seq = Sequences(empty_value=[1, None])
        if push_sequence is None:
            to_push_seq.update(self._push_seq)
        else:
            to_push_seq.update(push_sequence)

        if session is None:
            session_is_new = True
            session = ad.uuid()
        else:
            session_is_new = False

        while True:
            self._import(path, session, to_push_seq)
            self._push_seq.commit()
            self._pull_seq.commit()

            if session_is_new:
                with sneakernet.OutPacket('pull', root=path,
                        src=self._node_guid, dst=self._master,
                        session=session) as packet:
                    packet.header['sequence'] = self._pull_seq

            with sneakernet.OutPacket('push', root=path, limit=accept_length,
                    src=self._node_guid, dst=self._master,
                    session=session) as packet:
                _logger.debug('Generating %r PUSH packet to %r',
                        packet, packet.path)
                self.publish({
                    'event': 'sync_progress',
                    'progress': _('Generating %r PUSH packet') % \
                            packet.basename,
                    })

                out_seq = packet.header['sequence'] = Sequences()
                try:
                    self.volume.diff(to_push_seq, out_seq, packet)
                except sneakernet.DiskFull:
                    return {'push_sequence': to_push_seq, 'session': session}
                else:
                    break

    def sync_session(self, mounts):
        _logger.debug('Start synchronization session with %r session ' \
                'for %r mounts', self._sync_session, mounts)

        try:
            for path in mounts:
                self.publish({'event': 'sync_start', 'path': path})
                self._sync_session = \
                        self.sync(path, **(self._sync_session or {}))
                if self._sync_session is None:
                    break
        except Exception, error:
            util.exception(_logger, _('Failed to complete synchronization'))
            self.publish({'event': 'sync_error', 'error': str(error)})
            self._sync_session = None

        if self._sync_session is None:
            _logger.debug('Synchronization completed')
            self.publish({'event': 'sync_complete'})
        else:
            _logger.debug('Postpone synchronization with %r session',
                    self._sync_session)
            self.publish({'event': 'sync_continue'})

    def _import(self, path, session, to_push_seq):
        for packet in sneakernet.walk(path):
            if packet.header.get('type') == 'push' and \
                    packet.header.get('src') != self._node_guid:
                self.publish({
                    'event': 'sync_progress',
                    'progress': _('Reading %r PUSH packet') % packet.basename,
                    })
                _logger.debug('Processing %r PUSH packet from %r',
                        packet, packet.path)
                self.volume.merge(packet, increment_seqno=False)
                if packet.header.get('src') == self._master:
                    self._pull_seq.exclude(packet.header['sequence'])

            elif packet.header.get('type') == 'ack' and \
                    packet.header.get('src') == self._master and \
                    packet.header.get('dst') == self._node_guid:
                self.publish({
                    'event': 'sync_progress',
                    'progress': _('Reading %r ACK packet') % packet.basename,
                    })
                _logger.debug('Processing %r ACK packet from %r',
                        packet, packet.path)
                self._push_seq.exclude(packet.header['push_sequence'])
                self._pull_seq.exclude(packet.header['pull_sequence'])
                to_push_seq.exclude(packet.header['push_sequence'])
                _logger.debug('Remove processed %r ACK packet', packet)
                os.unlink(packet.path)

            elif packet.header.get('type') in ('push', 'pull') and \
                    packet.header.get('src') == self._node_guid:
                if packet.header.get('session') == session:
                    _logger.debug('Keep current session %r packet', packet)
                else:
                    _logger.debug('Remove our previous %r packet', packet)
                    os.unlink(packet.path)

            else:
                _logger.debug('No need to process %r packet', packet)


def _proxy_call(home_volume, request, response, super_call, get_blob):
    patch = {}
    result = None
    command = request['method'], request.get('cmd')
    document = request['document']
    guid = request.get('guid')

    if document == 'context':
        if command == ('GET', None):
            if 'reply' in request:
                reply = request.get('reply', [])[:]
                for prop, default in _LOCAL_PROPS.items():
                    if prop in reply:
                        patch[prop] = default
                        reply.remove(prop)
                request['reply'] = reply
        elif command in (('POST', None), ('PUT', None)):
            for prop in _LOCAL_PROPS.keys():
                if prop in request.content:
                    patch[prop] = request.content.pop(prop)
            if not request.content:
                result = guid

    if result is None:
        result = super_call(request, response)

    if document == 'context' and patch:
        directory = home_volume['context']
        if command == ('GET', None):
            if guid:
                if directory.exists(guid):
                    to_patch = directory.get(guid).properties(patch.keys())
                else:
                    to_patch = patch
                result.update(to_patch)
            else:
                for props in result['result']:
                    if directory.exists(props['guid']):
                        to_patch = directory.get(props['guid']).properties(
                                patch.keys())
                    else:
                        to_patch = patch
                    props.update(to_patch)
        else:
            if command == ('POST', None):
                guid = result
            if directory.exists(guid):
                directory.update(guid, patch)
            elif [True for prop, value in patch.items() if value]:
                clone = ad.Request(method='GET', document='context', guid=guid)
                clone.accept_language = request.accept_language
                props = super_call(clone, ad.Response())
                props.update(patch)
                props['guid'] = guid
                props['user'] = [sugar.uid()]
                directory.create(props)
                for prop in ('icon', 'artifact_icon', 'preview'):
                    blob = get_blob('context', guid, prop)
                    if blob:
                        directory.set_blob(guid, prop, blob['path'])

    return result
