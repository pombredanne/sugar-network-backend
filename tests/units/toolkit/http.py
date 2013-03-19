#!/usr/bin/env python
# sugar-lint: disable

import json
import select

from __init__ import tests

from sugar_network import db, client as local
from sugar_network.db import router
from sugar_network.toolkit import coroutine, http


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

        self.server = coroutine.WSGIServer(('localhost', local.ipc_port.value), Router(db.CommandsProcessor()))
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

    def test_call_ReturnStream(self):

        class Commands(db.CommandsProcessor):

            @db.volume_command(method='GET', cmd='f1', mime_type='application/json')
            def f1(self):
                yield json.dumps('result')

            @db.volume_command(method='GET', cmd='f2', mime_type='foo/bar')
            def f2(self):
                yield json.dumps('result')

        self.server = coroutine.WSGIServer(('localhost', local.ipc_port.value), router.Router(Commands()))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        client = http.Client('http://localhost:%s' % local.ipc_port.value, sugar_auth=False)

        request = db.Request()
        request['method'] = 'GET'
        request['cmd'] = 'f1'
        self.assertEqual('result', client.call(request))

        request = db.Request()
        request['method'] = 'GET'
        request['cmd'] = 'f2'
        self.assertEqual('result', json.load(client.call(request)))


if __name__ == '__main__':
    tests.main()
