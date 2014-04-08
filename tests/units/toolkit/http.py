#!/usr/bin/env python
# sugar-lint: disable

import json
import select

from __init__ import tests

from sugar_network import client as local
from sugar_network.toolkit.router import route, Router, Request, Response
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import coroutine, http


class HTTPTest(tests.Test):

    def test_Subscribe(self):

        class CommandsProcessor(object):

            events = []

            @route('GET', cmd='subscribe')
            def subscribe(self, request, response):
                while CommandsProcessor.events:
                    coroutine.sleep(.1)
                    yield CommandsProcessor.events.pop(0) + '\n'

        self.server = coroutine.WSGIServer(('127.0.0.1', local.ipc_port.value), Router(CommandsProcessor()))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        client = http.Connection('http://127.0.0.1:%s' % local.ipc_port.value)

        events = []
        CommandsProcessor.events = ['', 'fake', 'data: fail', 'data: null', 'data: -1', 'data: {"foo": "bar"}']
        try:
            for i in client.subscribe():
                events.append(i)
        except Exception:
            pass
        self.assertEqual(
                [-1, {'foo': 'bar'}],
                events)

        events = []
        CommandsProcessor.events = ['', 'fake', 'data: fail', 'data: null', 'data: -1', 'data: {"foo": "bar"}']
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

    def test_call(self):

        class Commands(object):

            @route('FOO', [None, None], cmd='f1', mime_type='application/json')
            def f1(self):
                return this.request.path

        self.server = coroutine.WSGIServer(('127.0.0.1', local.ipc_port.value), Router(Commands()))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        conn = http.Connection('http://127.0.0.1:%s' % local.ipc_port.value)

        request = Request({
            'REQUEST_METHOD': 'FOO',
            'PATH_INFO': '/foo/bar',
            'QUERY_STRING': 'cmd=f1',
            })
        self.assertEqual(['foo', 'bar'], conn.call(request))

        self.assertEqual(['foo', 'bar'], conn.call(Request(method='FOO', path=['foo', 'bar'], cmd='f1')))

    def test_call_ReturnStream(self):

        class Commands(object):

            @route('GET', cmd='f1', mime_type='application/json')
            def f1(self):
                yield json.dumps('result')

            @route('GET', cmd='f2', mime_type='foo/bar')
            def f2(self):
                yield json.dumps('result')

        self.server = coroutine.WSGIServer(('127.0.0.1', local.ipc_port.value), Router(Commands()))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        client = http.Connection('http://127.0.0.1:%s' % local.ipc_port.value)

        request = Request({
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
            'QUERY_STRING': 'cmd=f1',
            })
        self.assertEqual('result', client.call(request))

        request = Request({
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/',
            'QUERY_STRING': 'cmd=f2',
            })
        self.assertEqual('result', json.load(client.call(request)))


if __name__ == '__main__':
    tests.main()
