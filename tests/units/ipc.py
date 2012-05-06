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
        CommandsProcessor.calls = []

    def start_server(self):

        def server():
            Server(CommandsProcessor()).serve_forever()

        gevent.spawn(server)
        gevent.sleep()

    def test_Rendezvous(self):

        def server():
            time.sleep(1)
            Server(CommandsProcessor()).serve_forever()

        ts = time.time()
        self.fork(server)

        client = Client('/')
        client.Context.delete('guid')
        assert time.time() - ts >= 1

    def test_delete(self):
        self.start_server()

        client = Client('/')
        client.Context.delete('guid-1')

        self.assertEqual([
            ('delete', 'context', 'guid-1'),
            ],
            CommandsProcessor.calls)

    def test_find(self):
        self.start_server()
        client = Client('/')

        cursor = client.Context.cursor()
        self.assertEqual(10, cursor.total)
        self.assertEqual(
                [('find', 'context', 0, 18, None, None, None, {})],
                CommandsProcessor.calls)

        cursor = client.Context.cursor('query', order_by='foo', reply=None, bar=-1)
        self.assertEqual(10, cursor.total)
        self.assertEqual(
                [('find', 'context', 0, 18, 'query', 'foo', None, {'bar': -1})],
                CommandsProcessor.calls[1:])

    def test_create(self):
        self.start_server()
        client = Client('/')

        res = client.Resource()
        assert not res.guid
        res['prop_1'] = 'value_1'
        res['prop_2'] = 2
        res.post()

        self.assertEqual(
                [('create', 'resource', {'prop_1': 'value_1', 'prop_2': 2})],
                CommandsProcessor.calls)
        self.assertEqual(-1, res['guid'])

        res['prop_3'] = 3
        res.post()

        self.assertEqual(
                [('update', 'resource', -1, {'prop_3': 3})],
                CommandsProcessor.calls[1:])
        self.assertEqual(-1, res['guid'])

    def test_update(self):
        self.start_server()
        client = Client('/')

        res = client.Resource('guid')
        res['prop'] = 'value'
        res.post()

        self.assertEqual(
                [('update', 'resource', 'guid', {'prop': 'value'})],
                CommandsProcessor.calls)

    def test_get(self):
        self.start_server()
        client = Client('/')

        res = client.Resource('guid', ['prop'])
        self.assertEqual('value', res['prop'])
        self.assertEqual(
                [('get', 'resource', 'guid')],
                CommandsProcessor.calls)

    def test_get_blob(self):
        self.start_server()
        client = Client('/')

        res = client.Resource('guid')

        self.assertEqual('blob-path', res.get_blob_path('blob'))
        self.assertEqual('blob-value', res.get_blob('blob').read())

        self.assertEqual([
            ('get_blob', 'resource', 'guid', 'blob'),
            ('get_blob', 'resource', 'guid', 'blob'),
            ],
            CommandsProcessor.calls)

    def test_get_blob_EmptyBlob(self):
        self.start_server()
        client = Client('/')

        res = client.Resource('guid')

        self.assertEqual(None, res.get_blob_path('empty'))
        self.assertEqual('', res.get_blob('empty').read())

        self.assertEqual([
            ('get_blob', 'resource', 'guid', 'empty'),
            ('get_blob', 'resource', 'guid', 'empty'),
            ],
            CommandsProcessor.calls)

    def test_set_blob(self):
        self.start_server()
        client = Client('/')

        client.Resource('guid_1').set_blob('blob_1', 'string')
        client.Resource('guid_2').set_blob('blob_2', {'file': 'path'})
        client.Resource('guid_3').set_blob_by_url('blob_3', 'url')

        self.assertEqual([
            ('set_blob', 'resource', 'guid_1', 'blob_1', None, None, 'string'),
            ('set_blob', 'resource', 'guid_2', 'blob_2', {'file': 'path'}, None, None),
            ('set_blob', 'resource', 'guid_3', 'blob_3', None, 'url', None),
            ],
            CommandsProcessor.calls)

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
                standard.append(('delete', 'resource%s' % i, 'wait%s%s' % (i, n)))
        self.assertEqual(
                sorted(standard),
                sorted(CommandsProcessor.calls))


class CommandsProcessor(object):

    calls = []

    def call(self, socket, cmd, mountpoint, params):
        return getattr(self, cmd)(socket, **params)

    def create(self, socket, resource, props):
        reply = ('create', resource, props)
        CommandsProcessor.calls.append(reply)
        return -1

    def update(self, socket, resource, guid, props):
        reply = ('update', resource, guid, props)
        CommandsProcessor.calls.append(reply)

    def get(self, socket, resource, guid, reply):
        reply = ('get', resource, guid)
        CommandsProcessor.calls.append(reply)
        return {'guid': -1, 'prop': 'value'}

    def find(self, socket, resource, offset=None, limit=None,
            query=None, order_by=None, reply=None, **kwargs):
        reply = ('find', resource, offset, limit, query, order_by,
                reply, kwargs)
        CommandsProcessor.calls.append(reply)
        result = [{'guid': i} for i in range(offset, offset + 3)]
        return {'total': 10, 'result': result}

    def delete(self, socket, resource, guid):
        if guid == 'fake':
            raise RuntimeError()
        if guid.startswith('wait'):
            gevent.sleep(1)
        reply = ('delete', resource, guid)
        CommandsProcessor.calls.append(reply)

    def get_blob(self, socket, resource, guid, prop):
        reply = ('get_blob', resource, guid, prop)
        CommandsProcessor.calls.append(reply)
        if prop == 'empty':
            return None
        with file('blob-path', 'w') as f:
            f.write('blob-value')
        return {'path': 'blob-path', 'mime_type': 'application/json'}

    def set_blob(self, socket, resource, guid, prop, files=None,
            url=None):
        if files is None and url is None:
            data = socket.read()
        else:
            data = None
        reply = ('set_blob', resource, guid, prop, files, url, data)
        CommandsProcessor.calls.append(reply)


if __name__ == '__main__':
    tests.main()
