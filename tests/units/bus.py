#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine
from sugar_network.client import bus
from sugar_network import Client, ServerError
from sugar_network.local.bus import IPCServer
from sugar_network.local.mounts import Mounts


class IPCTest(tests.Test):

    def test_Rendezvous(self):

        def server():
            time.sleep(1)
            volume = ad.SingleVolume('local', [])
            mounts = Mounts(volume)
            server = IPCServer(mounts)
            mounts.call = lambda *args: None
            server.serve_forever()

        ts = time.time()
        self.fork(server)

        client = Client('/')
        client.Context.delete('guid')
        assert time.time() - ts >= 1

    def test_Exception(self):
        self.start_server()

        def call_handle(request, response):
            raise RuntimeError()

        self.mounts.call = call_handle

        client = Client('/')
        self.assertRaises(ServerError, client.Resource.delete, 'fake')

    def test_ConsecutiveRequests(self):
        self.start_server()
        calls = []
        self.mounts.call = lambda request, response: calls.append(dict(request))

        bus._CONNECTION_POOL = 3

        def caller(client, i, n):
            getattr(client, 'Resource%s' % i).delete('wait%s%s' % (i, n))

        ts = time.time()
        clients = [Client('/'), Client('/'), Client('/')]
        call_jobs = []
        for i, client in enumerate(clients):
            for n in range(9):
                call_jobs.append(coroutine.spawn(caller, client, i, n))

        coroutine.joinall(call_jobs)
        assert time.time() - ts < 4

        standard = []
        for i, client in enumerate(clients):
            for n in range(9):
                standard.append({'method': 'DELETE', 'mountpoint': '/', 'guid': 'wait%s%s' % (i, n), 'document': 'resource%s' % i})
        self.assertEqual(
                sorted(standard),
                sorted(calls))


if __name__ == '__main__':
    tests.main()
