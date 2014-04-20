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

from sugar_network.toolkit.router import route
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import coroutine, http


_logger = logging.getLogger('model.routes')


class FrontRoutes(object):

    def __init__(self):
        self._spooler = coroutine.Spooler()
        this.broadcast = self._broadcast
        this.localcast = self._broadcast

    @route('GET')
    def hello(self):
        raise http.Redirect('http://wiki.sugarlabs.org/go/Sugar_Network/API')

    @route('OPTIONS')
    def options(self):
        response = this.response
        environ = this.request.environ
        if environ['HTTP_ORIGIN']:
            response['Access-Control-Allow-Methods'] = \
                    environ['HTTP_ACCESS_CONTROL_REQUEST_METHOD']
            response['Access-Control-Allow-Headers'] = \
                    environ['HTTP_ACCESS_CONTROL_REQUEST_HEADERS']
        else:
            response['Allow'] = 'GET, HEAD, POST, PUT, DELETE'
        response.content_length = 0

    @route('GET', cmd='subscribe', mime_type='text/event-stream')
    def subscribe(self, **condition):
        """Subscribe to Server-Sent Events."""
        this.response['Cache-Control'] = 'no-cache'

        _logger.debug('Start %s-nth subscription', self._spooler.waiters + 1)

        # Unblock `GET /?cmd=subscribe` call to let non-greenlet application
        # initiate a subscription and do not stuck in waiting for the 1st event
        yield {'event': 'pong'}

        subscription = this.request.content_stream
        if subscription is not None:
            coroutine.spawn(self._wait_for_closing, subscription)

        while True:
            event = self._spooler.wait()
            if not isinstance(event, dict):
                if event is subscription:
                    break
                else:
                    continue
            for key, value in condition.items():
                if value.startswith('!'):
                    if event.get(key) == value[1:]:
                        break
                elif event.get(key) != value:
                    break
            else:
                yield event

        _logger.debug('Stop %s-nth subscription', self._spooler.waiters)

    @route('GET', ['robots.txt'], mime_type='text/plain')
    def robots(self):
        return 'User-agent: *\nDisallow: /\n'

    @route('GET', ['favicon.ico'])
    def favicon(self):
        return this.volume.blobs.get('assets/favicon.ico')

    def _broadcast(self, event):
        _logger.debug('Broadcast event: %r', event)
        self._spooler.notify_all(event)

    def _wait_for_closing(self, rfile):
        try:
            coroutine.select([rfile.fileno()], [], [])
        finally:
            self._spooler.notify_all(rfile)
