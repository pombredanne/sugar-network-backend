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
from base64 import b64encode
from httplib import IncompleteRead
from os.path import join

from sugar_network import db, client, node, toolkit, model
from sugar_network.client import journal
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import ACL, Request, Response, Router
from sugar_network.toolkit.router import route, fallbackroute
from sugar_network.toolkit import netlink, zeroconf, coroutine, http, parcel
from sugar_network.toolkit import lsb_release, exception, enforce


# Flag file to recognize a directory as a synchronization directory
_RECONNECT_TIMEOUT = 3
_RECONNECT_TIMEOUT_MAX = 60 * 15

_logger = logging.getLogger('client.routes')


class ClientRoutes(model.FrontRoutes, journal.Routes):

    def __init__(self, home_volume, no_subscription=False):
        model.FrontRoutes.__init__(self)
        journal.Routes.__init__(self)

        this.localcast = this.broadcast

        self._local = _LocalRoutes(home_volume)
        self._inline = coroutine.Event()
        self._inline_job = coroutine.Pool()
        self._remote_urls = []
        self._node = None
        self._connect_jobs = coroutine.Pool()
        self._no_subscription = no_subscription
        self._auth = _Auth()

    def connect(self, api=None):
        if self._connect_jobs:
            return
        self._got_offline()
        if not api:
            self._connect_jobs.spawn(self._discover_node)
        else:
            self._remote_urls.append(api)
        self._connect_jobs.spawn(self._wait_for_connectivity)

    def close(self):
        self._connect_jobs.kill()
        self._got_offline()
        self._local.volume.close()

    @fallbackroute('GET', ['hub'])
    def hub(self, request, response):
        """Serve Hub via HTTP instead of file:// for IPC users.

        Since SSE doesn't support CORS for now.

        """
        if request.environ['PATH_INFO'] == '/hub':
            raise http.Redirect('/hub/')

        path = request.path[1:]
        if not path:
            path = ['index.html']
        path = join(client.hub_root.value, *path)

        mtime = int(os.stat(path).st_mtime)
        if request.if_modified_since >= mtime:
            raise http.NotModified()

        if path.endswith('.js'):
            response.content_type = 'text/javascript'
        if path.endswith('.css'):
            response.content_type = 'text/css'
        response.last_modified = mtime

        return file(path, 'rb')

    @fallbackroute('GET', ['packages'])
    def route_packages(self, request, response):
        if self._inline.is_set():
            return self.fallback(request, response)
        else:
            # Let caller know that we are in offline and
            # no way to process specified request on the node
            raise http.ServiceUnavailable()

    @route('GET', cmd='inline',
            mime_type='application/json')
    def inline(self):
        return self._inline.is_set()

    @route('GET', cmd='whoami', mime_type='application/json')
    def whoami(self, request, response):
        if self._inline.is_set():
            result = self.fallback(request, response)
            result['route'] = 'proxy'
        else:
            result = {'roles': [], 'route': 'offline'}
        result['guid'] = self._auth.login
        return result

    @route('GET', [None],
            arguments={'offset': int, 'limit': int, 'reply': ('guid',)},
            mime_type='application/json')
    def find(self, request, response):
        if not self._inline.is_set() or 'pins' in request:
            return self._local.call(request, response)

        reply = request.setdefault('reply', ['guid'])
        if 'pins' not in reply:
            return self.fallback(request, response)

        if 'guid' not in reply:
            # Otherwise there is no way to mixin `pins`
            reply.append('guid')
        result = self.fallback(request, response)

        directory = self._local.volume[request.resource]
        for item in result['result']:
            doc = directory[item['guid']]
            if doc.exists:
                item['pins'] += doc.repr('pins')

        return result

    @route('GET', [None, None], mime_type='application/json')
    def get(self, request, response):
        if self._local.volume[request.resource][request.guid].exists:
            return self._local.call(request, response)
        else:
            return self.fallback(request, response)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self, request, response):
        if self._local.volume[request.resource][request.guid].exists:
            return self._local.call(request, response)
        else:
            return self.fallback(request, response)

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
                        content_stream=f, content_type='text/plain')
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
    def delete_checkin(self, request):
        this.injector.checkout(this.request.guid)
        self._checkout_context()

    @route('PUT', ['context', None], cmd='favorite')
    def put_favorite(self, request):
        self._checkin_context('favorite')

    @route('DELETE', ['context', None], cmd='favorite')
    def delete_favorite(self, request):
        self._checkout_context('favorite')

    @route('GET', cmd='recycle')
    def recycle(self):
        return this.injector.recycle()

    @fallbackroute()
    def fallback(self, request=None, response=None, **kwargs):
        if request is None:
            request = Request(**kwargs)
        if response is None:
            response = Response()

        if not self._inline.is_set():
            return self._local.call(request, response)

        request.principal = self._auth.login
        try:
            reply = self._node.call(request, response)
            if hasattr(reply, 'read'):
                if response.relocations:
                    return reply
                else:
                    return _ResponseStream(reply, self._restart_online)
            else:
                return reply
        except (http.ConnectionError, IncompleteRead):
            if response.relocations:
                raise
            self._restart_online()
            return self._local.call(request, response)

    def _got_online(self, url):
        enforce(not self._inline.is_set())
        _logger.debug('Got online on %r', self._node)
        self._inline.set()
        self._local.volume.mute = True
        this.injector.api = url
        this.localcast({'event': 'inline', 'state': 'online'})

    def _got_offline(self):
        if self._node is not None:
            self._node.close()
        if self._inline.is_set():
            _logger.debug('Got offline on %r', self._node)
            self._inline.clear()
            self._local.volume.mute = False
            this.injector.api = None
            this.localcast({'event': 'inline', 'state': 'offline'})

    def _restart_online(self):
        _logger.debug('Lost %r connection, try to reconnect in %s seconds',
                self._node, _RECONNECT_TIMEOUT)
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
            for event in self._node.subscribe():
                if event.get('event') == 'release':
                    this.injector.seqno = event['seqno']
                this.broadcast(event)

        def handshake(url):
            _logger.debug('Connecting to %r node', url)
            self._node = client.Connection(url, auth=self._auth)
            status = self._node.get(cmd='status')
            self._auth.allow_basic_auth = (status.get('level') == 'master')
            seqno = status.get('seqno')
            if seqno and 'releases' in seqno:
                this.injector.seqno = seqno['releases']
            if self._inline.is_set():
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
                            exception(_logger, 'Connection to %r failed', url)
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
        context = this.volume['context'][this.request.guid]
        if not context.exists:
            enforce(self.inline(), http.ServiceUnavailable,
                    'Not available in offline')
            _logger.debug('Checkin %r context', context.guid)
            clone = self.fallback(
                    method='GET', path=['context', context.guid], cmd='clone')
            this.volume.patch(next(parcel.decode(clone)))
        pins = context['pins']
        if pin and pin not in pins:
            this.volume['context'].update(context.guid, {'pins': pins + [pin]})

    def _checkout_context(self, pin=None):
        directory = this.volume['context']
        context = directory[this.request.guid]
        if not context.exists:
            return
        pins = set(context.repr('pins'))
        if pin:
            pins -= set([pin])
        if not self._inline.is_set() or pins:
            if pin:
                directory.update(context.guid, {'pins': list(pins)})
        else:
            directory.delete(context.guid)


class CachedClientRoutes(ClientRoutes):

    def __init__(self, home_volume, api_url=None, no_subscription=False):
        self._push_seq = toolkit.PersistentSequence(
                join(home_volume.root, 'push.sequence'), [1, None])
        self._push_job = coroutine.Pool()
        ClientRoutes.__init__(self, home_volume, api_url, no_subscription)

    def _got_online(self, url):
        ClientRoutes._got_online(self, url)
        self._push_job.spawn(self._push)

    def _got_offline(self):
        self._push_job.kill()
        ClientRoutes._got_offline(self)

    def _push(self):
        # TODO should work using regular diff
        return



        pushed_seq = toolkit.Sequence()
        skiped_seq = toolkit.Sequence()
        volume = self._local.volume

        def push(request, seq):
            try:
                self.fallback(request)
            except Exception:
                _logger.exception('Cannot push %r, will postpone', request)
                skiped_seq.include(seq)
            else:
                pushed_seq.include(seq)

        for res in volume.resources:
            if volume.mtime(res) <= self._push_seq.mtime:
                continue

            _logger.debug('Check %r local cache to push', res)

            for guid, patch in volume[res].diff(self._push_seq):
                diff = {}
                diff_seq = toolkit.Sequence()
                post_requests = []
                for prop, meta, seqno in patch:
                    value = meta['value']
                    diff[prop] = value
                    diff_seq.include(seqno, seqno)
                if not diff:
                    continue
                if 'guid' in diff:
                    request = Request(method='POST', path=[res])
                    access = ACL.CREATE | ACL.WRITE
                else:
                    request = Request(method='PUT', path=[res, guid])
                    access = ACL.WRITE
                for name in diff.keys():
                    if not (volume[res].metadata[name].acl & access):
                        del diff[name]
                request.content_type = 'application/json'
                request.content = diff
                push(request, diff_seq)
                for request, seqno in post_requests:
                    push(request, [[seqno, seqno]])

        if not pushed_seq:
            if not self._push_seq.mtime:
                self._push_seq.commit()
            return

        _logger.info('Pushed %r local cache', pushed_seq)

        self._push_seq.exclude(pushed_seq)
        if not skiped_seq:
            self._push_seq.stretch()
            if 'report' in volume:
                # No any decent reasons to keep fail reports after uploding.
                # TODO The entire offlile synchronization should be improved,
                # for now, it is possible to have a race here
                volume['report'].wipe()

        self._push_seq.commit()


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


class _Auth(http.SugarAuth):

    def __init__(self):
        http.SugarAuth.__init__(self, client.keyfile.value)
        if client.login.value:
            self._login = client.login.value
        self.allow_basic_auth = False

    def profile(self):
        if self.allow_basic_auth and \
                client.login.value and client.password.value:
            return None
        import gconf
        conf = gconf.client_get_default()
        self._profile['name'] = conf.get_string('/desktop/sugar/user/nick')
        return http.SugarAuth.profile(self)

    def __call__(self, nonce):
        if not self.allow_basic_auth or \
                not client.login.value or not client.password.value:
            return http.SugarAuth.__call__(self, nonce)
        auth = b64encode('%s:%s' % (client.login.value, client.password.value))
        return 'Basic %s' % auth
