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


class IPCTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        Mounts.calls = []

    def start_server(self):

        def server():
            Server(Mounts()).serve_forever()

        gevent.spawn(server)
        gevent.sleep()

    def test_Rendezvous(self):

        def server():
            time.sleep(1)
            Server(Mounts()).serve_forever()

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
                sorted(Mounts.calls))


class Mounts(dict):

    calls = []

    def call(self, request, response):
        if request.command == 'DELETE' and request.get('guid') == 'fake':
            raise RuntimeError()
        Mounts.calls.append(dict(request))


if __name__ == '__main__':
    tests.main()
