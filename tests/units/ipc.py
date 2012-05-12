#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent

from __init__ import tests

from sugar_network import client as client_
from sugar_network.client import Client, ServerError
from local_document.server import Server
from sugar_network_server.resources.context import Context


class IPCTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.calls = []

    def start_server(self):

        def server():
            self.server.serve_forever()

        def call(request, response):
            if request.command == 'DELETE' and request.get('guid') == 'fake':
                raise RuntimeError()
            self.calls.append(dict(request))

        self.server = Server('local', [])
        self.server._mounts.call = call
        gevent.spawn(server)
        gevent.sleep()

    def test_Rendezvous(self):

        def server():
            time.sleep(1)
            server = Server('local', [])
            server._mounts.call = lambda *args: None
            server.serve_forever()

        ts = time.time()
        self.fork(server)

        client = Client('/')
        client.Context.delete('guid')
        assert time.time() - ts >= 1

    def test_Exception(self):
        self.start_server()
        client = Client('/')
        self.assertRaises(ServerError, client.Resource.delete, 'fake')

    def test_ConsecutiveRequests(self):
        self.start_server()

        client_._CONNECTION_POOL = 3

        def call(client, i, n):
            getattr(client, 'Resource%s' % i).delete('wait%s%s' % (i, n))

        ts = time.time()
        clients = [Client('/'), Client('/'), Client('/')]
        calls = []
        for i, client in enumerate(clients):
            for n in range(9):
                calls.append(gevent.spawn(call, client, i, n))

        gevent.joinall(calls)
        assert time.time() - ts < 4

        standard = []
        for i, client in enumerate(clients):
            for n in range(9):
                standard.append({'mountpoint': '/', 'guid': 'wait%s%s' % (i, n), 'document': 'resource%s' % i})
        self.assertEqual(
                sorted(standard),
                sorted(self.calls))


if __name__ == '__main__':
    tests.main()
