#!/usr/bin/env python
# sugar-lint: disable

import select

from __init__ import tests

import active_document as ad
from sugar_network import client as local
from sugar_network.toolkit import router, http
from active_toolkit import coroutine


class HTTPTest(tests.Test):

    def test_Subscribe(self):

        class Router(router.Router):

            events = []

            @router.route('GET', '/')
            def subscribe(self, request, response):
                assert request.get('cmd') == 'subscribe'
                while Router.events:
                    coroutine.sleep(.3)
                    yield Router.events.pop(0) + '\n'

        self.server = coroutine.WSGIServer(('localhost', local.ipc_port.value), Router(ad.CommandsProcessor()))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        client = http.Client('http://localhost:%s' % local.ipc_port.value, sugar_auth=False)

        events = []
        Router.events = ['', 'fake', 'data: fail', 'data: null', 'data: -1', 'data: {"foo": "bar"}']
        try:
            for i in client.subscribe():
                events.append(i)
        except Exception:
            pass
        self.assertEqual(
                [-1, {'foo': 'bar'}],
                events)

        events = []
        Router.events = ['', 'fake', 'data: fail', 'data: null', 'data: -1', 'data: {"foo": "bar"}']
        subscription = client.subscribe()
        try:
            while select.select([subscription.fileno()], [], []):
                events.append(subscription.pull())
            assert False
        except Exception:
            pass
        self.assertEqual(
                [None, None, None, None, -1, {'foo': 'bar'}],
                events)


if __name__ == '__main__':
    tests.main()
