# Copyright (C) 2012-2014 Aleksey Lim
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
import logging
from httplib import IncompleteRead
from os.path import join

from sugar_network import db, client, node, toolkit
from sugar_network.model import FrontRoutes
from sugar_network.client import model
from sugar_network.client.journal import Routes as JournalRoutes
from sugar_network.toolkit.router import Request, Router, Response
from sugar_network.toolkit.router import route, fallbackroute
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import netlink, zeroconf, coroutine, http, packets
from sugar_network.toolkit import ranges, lsb_release, enforce


_SYNC_TIMEOUT = 30
_RECONNECT_TIMEOUT = 3
_RECONNECT_TIMEOUT_MAX = 60 * 15

_logger = logging.getLogger('client.routes')


class ClientRoutes(FrontRoutes, JournalRoutes):

    def __init__(self, home_volume, creds, no_subscription=False):
        FrontRoutes.__init__(self)
        JournalRoutes.__init__(self)

        this.localcast = this.broadcast

        self._local = _LocalRoutes(home_volume)
        self._remote = None
        self._remote_urls = []
        self._creds = creds
        self._inline = coroutine.Event()
        self._inline_job = coroutine.Pool()
        self._connect_jobs = coroutine.Pool()
        self._sync_jobs = coroutine.Pool()
        self._no_subscription = no_subscription
        self._pull_r = toolkit.Bin(
                join(home_volume.root, 'var', 'pull'), [[1, None]])

    def connect(self, api=None):
        if self._connect_jobs:
            return
        self._got_offline()
        if not api:
            self._connect_jobs.spawn(self._discover_node)
        else:
            self._remote_urls.append(api)
        self._connect_jobs.spawn(self._wait_for_connectivity)
        self._local.volume.populate()

    def close(self):
        self._connect_jobs.kill()
        self._got_offline()
        self._local.volume.close()
        self._pull_r.commit()

    @fallbackroute('GET', ['hub'])
    def hub(self):
        """Serve Hub via HTTP instead of file:// for IPC users.

        Since SSE doesn't support CORS for now.

        """
        if this.request.environ['PATH_INFO'] == '/hub':
            raise http.Redirect('/hub/')

        path = this.request.path[1:]
        if not path:
            path = ['index.html']
        path = join(client.hub_root.value, *path)

        mtime = int(os.stat(path).st_mtime)
        if this.request.if_modified_since >= mtime:
            raise http.NotModified()

        if path.endswith('.js'):
            this.response.content_type = 'text/javascript'
        if path.endswith('.css'):
            this.response.content_type = 'text/css'
        this.response.last_modified = mtime

        return file(path, 'rb')

    @fallbackroute('GET', ['packages'])
    def route_packages(self):
        if self.inline():
            return self.fallback()
        else:
            # Let caller know that we are in offline and
            # no way to process specified request on the node
            raise http.ServiceUnavailable()

    @route('GET', cmd='inline',
            mime_type='application/json')
    def inline(self):
        return self._inline.is_set()

    @route('GET', cmd='whoami', mime_type='application/json')
    def whoami(self):
        if self.inline():
            result = self.fallback()
            result['route'] = 'proxy'
        else:
            result = {'route': 'offline'}
        result['guid'] = self._creds.login
        return result

    @route('POST', ['report'], cmd='submit', mime_type='text/event-stream')
    def submit_report(self):
        props = this.request.content
        logs = props.pop('logs')
        props['uname'] = os.uname()
        props['lsb_release'] = {
                'distributor_id': lsb_release.distributor_id(),
                'release': lsb_release.release(),
                }
        guid = self.fallback(method='POST', path=['report'],
                content=props, content_type='application/json')
        for logfile in logs:
            with file(logfile) as f:
                self.fallback(method='POST', path=['report', guid, 'logs'],
                        content=f, content_type='text/plain')
        yield {'event': 'done', 'guid': guid}

    @route('GET', ['context', None], cmd='launch', arguments={'args': list},
            mime_type='text/event-stream')
    def launch(self):
        return this.injector.launch(this.request.guid, **this.request)

    @route('PUT', ['context', None], cmd='checkin',
            mime_type='text/event-stream')
    def put_checkin(self):
        self._checkin_context()
        for event in this.injector.checkin(this.request.guid):
            yield event

    @route('DELETE', ['context', None], cmd='checkin')
    def delete_checkin(self):
        this.injector.checkout(this.request.guid)
        self._checkout_context()

    @route('PUT', ['context', None], cmd='favorite')
    def put_favorite(self):
        self._checkin_context('favorite')

    @route('DELETE', ['context', None], cmd='favorite')
    def delete_favorite(self):
        self._checkout_context('favorite')

    @route('GET', cmd='recycle')
    def recycle(self):
        return this.injector.recycle()

    @route('GET', [None],
            arguments={'offset': int, 'limit': int, 'reply': ['guid']},
            mime_type='application/json')
    def find(self, reply):
        request = this.request
        if not self.inline() or 'pins' in request:
            return self._local.call(request, this.response)
        if 'guid' not in reply:
            # Otherwise no way to mixin `pins` or sync checkins
            reply.append('guid')
        if 'mtime' not in reply:
            # To track updates for checked-in resources
            reply.append('mtime')
        result = self.fallback()
        directory = self._local.volume[request.resource]
        for item in result['result']:
            checkin = directory[item['guid']]
            if not checkin.exists:
                continue
            pins = item['pins'] = checkin.repr('pins')
            if pins and item['mtime'] > checkin['mtime']:
                pull = Request(method='GET',
                        path=[checkin.metadata.name, checkin.guid], cmd='diff')
                self._sync_jobs.spawn(self._pull_checkin, pull, None, 'ranges')
        return result

    @route('GET', [None, None], mime_type='application/json')
    def get(self):
        request = this.request
        if self._local.volume[request.resource][request.guid].exists:
            return self._local.call(request, this.response)
        else:
            return self.fallback()

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self):
        return self.get()

    @route('PUT', [None, None])
    def update(self):
        if not self.inline():
            return self.fallback()
        request = this.request
        local = self._local.volume[request.resource][request.guid]
        if not local.exists or not local.repr('pins'):
            return self.fallback()
        self._pull_checkin(request, None, 'pull')

    @route('PUT', [None, None, None])
    def update_prop(self):
        self.update()

    @route('DELETE', [None, None])
    def delete(self):
        self.update()

    @fallbackroute()
    def fallback(self, request=None, response=None, **kwargs):
        if request is None:
            request = Request(**kwargs) if kwargs else this.request
        if response is None:
            response = this.response

        if not self.inline():
            return self._local.call(request, response)

        try:
            result = self._remote.call(request, response)
            if hasattr(result, 'read'):
                if response.relocations:
                    return result
                else:
                    return _ResponseStream(result, self._restart_online)
            else:
                return result
        except (http.ConnectionError, IncompleteRead):
            if response.relocations:
                raise
            self._restart_online()
            return self._local.call(request, response)

    def _got_online(self, url):
        enforce(not self.inline())
        _logger.debug('Got online on %r', self._remote)
        self._inline.set()
        self._local.volume.mute = True
        this.injector.api = url
        this.localcast({'event': 'inline', 'state': 'online'})
        if not self._local.volume.empty:
            self._sync_jobs.spawn_later(_SYNC_TIMEOUT, self._sync)

    def _got_offline(self):
        if self._remote is not None:
            self._remote.close()
            self._remote = None
        if self.inline():
            _logger.debug('Got offline on %r', self._remote)
            self._inline.clear()
            self._local.volume.mute = False
            this.injector.api = None
            this.localcast({'event': 'inline', 'state': 'offline'})
        self._sync_jobs.kill()

    def _restart_online(self):
        _logger.debug('Lost %r connection, try to reconnect in %s seconds',
                self._remote, _RECONNECT_TIMEOUT)
        self._remote_connect(_RECONNECT_TIMEOUT)

    def _discover_node(self):
        for host in zeroconf.browse_workstations():
            url = 'http://%s:%s' % (host, node.port.default)
            if url not in self._remote_urls:
                self._remote_urls.append(url)
            self._remote_connect()

    def _wait_for_connectivity(self):
        for gw in netlink.wait_for_route():
            if gw:
                self._remote_connect()
            else:
                self._got_offline()

    def _remote_connect(self, timeout=0):

        def pull_events():
            for event in self._remote.subscribe():
                if event.get('event') == 'release':
                    this.injector.seqno = event['seqno']
                this.broadcast(event)

        def handshake(url):
            _logger.debug('Connecting to %r node', url)
            self._remote = client.Connection(url, creds=self._creds)
            status = self._remote.get(cmd='status')
            seqno = status.get('seqno')
            if seqno and 'releases' in seqno:
                this.injector.seqno = seqno['releases']
            if self.inline():
                _logger.info('Reconnected to %r node', url)
            else:
                self._got_online(url)

        def connect():
            timeout = _RECONNECT_TIMEOUT
            while True:
                this.localcast({'event': 'inline', 'state': 'connecting'})
                for url in self._remote_urls:
                    while True:
                        try:
                            handshake(url)
                            timeout = _RECONNECT_TIMEOUT
                            if self._no_subscription:
                                return
                            pull_events()
                        except (http.BadGateway, http.GatewayTimeout):
                            _logger.debug('Retry %r on gateway error', url)
                            continue
                        except Exception:
                            _logger.exception('Connection to %r failed', url)
                        break
                self._got_offline()
                if not timeout:
                    break
                _logger.debug('Try to reconect in %s seconds', timeout)
                coroutine.sleep(timeout)
                timeout *= _RECONNECT_TIMEOUT
                timeout = min(timeout, _RECONNECT_TIMEOUT_MAX)

        self._inline_job.kill()
        self._inline_job.spawn_later(timeout, connect)

    def _checkin_context(self, pin=None):
        contexts = self._local.volume['context']
        local_context = contexts[this.request.guid]
        if not local_context.exists:
            enforce(self.inline(), http.ServiceUnavailable,
                    'Not available in offline')
            _logger.debug('Checkin %r context', local_context.guid)
            pull = Request(method='GET',
                    path=['context', local_context.guid], cmd='diff')
            self._pull_checkin(pull, None, 'ranges')
        pins = local_context['pins']
        if pin and pin not in pins:
            contexts.update(local_context.guid, {'pins': pins + [pin]})

    def _checkout_context(self, pin=None):
        contexts = self._local.volume['context']
        local_context = contexts[this.request.guid]
        if not local_context.exists:
            return
        pins = set(local_context.repr('pins'))
        if pin:
            pins -= set([pin])
        if not self.inline() or pins:
            if pin:
                contexts.update(local_context.guid, {'pins': list(pins)})
        else:
            contexts.delete(local_context.guid)

    def _pull_checkin(self, request, response, header_key):
        request.headers[header_key] = self._pull_r.value
        packet = packets.decode(self.fallback(request, response))

        volume = self._local.volume
        volume[request.resource].patch(request.guid, packet['patch'])
        for blob in packet:
            volume.blobs.patch(blob)
        ranges.exclude(self._pull_r.value, packet['ranges'])

    def _pull(self):
        _logger.debug('Start pulling checkin updates')

        response = Response()
        for directory in self._local.volume.values():
            if directory.empty:
                continue
            request = Request(method='GET',
                    path=[directory.metadata.name], cmd='diff')
            while True:
                request.headers['ranges'] = self._pull_r.value
                diff = self.fallback(request, response)
                if not diff:
                    break
                for guid, r in diff.items():
                    checkin = Request(method='GET',
                            path=[request.resource, guid], cmd='diff')
                    self._pull_checkin(checkin, response, 'ranges')
                    ranges.exclude(self._pull_r.value, r)

    def _push(self):
        volume = self._local.volume

        _logger.debug('Start pushing offline updates')

        dump = packets.encode(model.dump_volume(volume))
        request = Request(method='POST', cmd='apply', content=dump)
        self.fallback(request, Response())

        _logger.debug('Wipeout offline updates')

        for directory in volume.values():
            if directory.empty:
                continue
            if directory.has_noseqno:
                directory.dilute()
            else:
                directory.wipe()

        _logger.debug('Wipeout offline blobs')

        for blob in volume.blobs.walk():
            if int(blob.meta['x-seqno']):
                volume.blobs.wipe(blob)

    def _sync(self):
        try:
            self._pull()
            if self._local.volume.has_seqno:
                self._push()
        except:
            this.localcast({'event': 'sync', 'state': 'failed'})
            raise
        else:
            this.localcast({'event': 'sync', 'state': 'done'})


class _LocalRoutes(db.Routes, Router):

    def __init__(self, volume):
        db.Routes.__init__(self, volume)
        Router.__init__(self, self)


class _ResponseStream(object):

    def __init__(self, stream, on_fail_cb):
        self._stream = stream
        self._on_fail_cb = on_fail_cb

    def __hasattr__(self, name):
        return hasattr(self._stream, name)

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def read(self, size=None):
        try:
            return self._stream.read(size)
        except (http.ConnectionError, IncompleteRead):
            self._on_fail_cb()
            raise
