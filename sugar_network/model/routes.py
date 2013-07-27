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
import logging
import mimetypes
from os.path import join, split

from sugar_network import static
from sugar_network.toolkit.router import route, fallbackroute, Blob, ACL
from sugar_network.toolkit import coroutine


_logger = logging.getLogger('model.routes')


class Routes(object):

    def __init__(self):
        self._pooler = _Pooler()

    @route('GET', mime_type='text/html')
    def hello(self):
        return _HELLO_HTML

    @route('OPTIONS')
    def options(self, request, response):
        if request.environ['HTTP_ORIGIN']:
            response['Access-Control-Allow-Methods'] = \
                    request.environ['HTTP_ACCESS_CONTROL_REQUEST_METHOD']
            response['Access-Control-Allow-Headers'] = \
                    request.environ['HTTP_ACCESS_CONTROL_REQUEST_HEADERS']
        else:
            response['Allow'] = 'GET, HEAD, POST, PUT, DELETE'
        response.content_length = 0

    @route('GET', cmd='whoami', mime_type='application/json')
    def whoami(self, request, response):
        roles = []
        # pylint: disable-msg=E1101
        if self.authorize(request.principal, 'root'):
            roles.append('root')
        return {'roles': roles, 'guid': request.principal}

    @route('GET', cmd='subscribe', mime_type='text/event-stream')
    def subscribe(self, request=None, response=None, ping=False, **condition):
        """Subscribe to Server-Sent Events."""
        if request is not None and not condition:
            condition = request
        if response is not None:
            response.content_type = 'text/event-stream'
            response['Cache-Control'] = 'no-cache'
        peer = 'anonymous'
        if hasattr(request, 'environ'):
            peer = request.environ.get('HTTP_X_SN_LOGIN') or peer
        return self._pull_events(peer, ping, condition)

    @route('POST', cmd='broadcast',
            mime_type='application/json', acl=ACL.LOCAL)
    def broadcast(self, event=None, request=None):
        if request is not None:
            event = request.content
        _logger.debug('Broadcast event: %r', event)
        self._pooler.notify_all(event)
        coroutine.dispatch()

    @fallbackroute('GET', ['static'])
    def get_static(self, request):
        path = join(static.PATH, *request.path[1:])
        if not mimetypes.inited:
            mimetypes.init()
        mime_type = mimetypes.types_map.get('.' + path.rsplit('.', 1)[-1])
        return Blob({
            'blob': path,
            'filename': split(path)[-1],
            'mime_type': mime_type,
            })

    @route('GET', ['robots.txt'], mime_type='text/plain')
    def robots(self, request, response):
        return 'User-agent: *\nDisallow: /\n'

    @route('GET', ['favicon.ico'])
    def favicon(self, request, response):
        return Blob({
            'blob': join(static.PATH, 'favicon.ico'),
            'mime_type': 'image/x-icon',
            })

    def _pull_events(self, peer, ping, condition):
        _logger.debug('Start pulling events to %s user', peer)

        if ping:
            # XXX The whole commands' kwargs handling should be redesigned
            if 'ping' in condition:
                condition.pop('ping')
            # If non-greenlet application needs only to initiate
            # a subscription and do not stuck in waiting for the first event,
            # it should pass `ping` argument to return fake event to unblock
            # `GET /?cmd=subscribe` call.
            yield 'data: %s\n\n' % json.dumps({'event': 'pong'})

        try:
            while True:
                event = self._pooler.wait()
                for key, value in condition.items():
                    if value.startswith('!'):
                        if event.get(key) == value[1:]:
                            break
                    elif event.get(key) != value:
                        break
                else:
                    yield 'data: %s\n\n' % json.dumps(event)
        finally:
            _logger.debug('Stop pulling events to %s user', peer)


class _Pooler(object):
    """One-producer-to-many-consumers events delivery."""

    def __init__(self):
        self._value = None
        self._waiters = 0
        self._ready = coroutine.Event()
        self._open = coroutine.Event()
        self._open.set()

    def wait(self):
        self._open.wait()
        self._waiters += 1
        try:
            self._ready.wait()
        finally:
            self._waiters -= 1
            if self._waiters == 0:
                self._ready.clear()
                self._open.set()
        return self._value

    def notify_all(self, value=None):
        self._open.wait()
        if not self._waiters:
            return
        self._open.clear()
        self._value = value
        self._ready.set()


_HELLO_HTML = """\
<h2>Welcome to Sugar Network API!</h2>
Visit the <a href="http://wiki.sugarlabs.org/go/Sugar_Network/API">
Sugar Labs Wiki</a> to learn how it can be used.
"""
