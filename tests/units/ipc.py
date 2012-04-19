#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from cStringIO import StringIO
from os.path import join

import gevent

from __init__ import tests

from local_document.ipc_client import Client
from local_document.ipc_server import Server


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
        fork = self.fork(server)

        client = Client()
        reply = client.Context('guid').ping()
        self.assertEqual(['ping', 'context', 'guid'], reply)
        assert time.time() - ts >= 1

    def test_delete(self):
        self.start_server()

        client = Client()
        client.Context.delete('guid-1')
        client.Context('guid-2').delete()

        self.assertEqual([
            ('delete', 'context', 'guid-1'),
            ('delete', 'context', 'guid-2'),
            ],
            CommandsProcessor.calls)

    def test_find(self):
        self.start_server()
        client = Client()

        query = client.Context.find()
        self.assertEqual(10, query.total)
        self.assertEqual(
                [('find', 'context', 0, 16, None, None, ['guid'], {})],
                CommandsProcessor.calls)

        query = client.Context.find('query', order_by='foo', reply=['f1', 'f2'], bar=-1)
        self.assertEqual(10, query.total)
        self.assertEqual(
                [('find', 'context', 0, 16, 'query', 'foo', ['f1', 'f2'], {'bar': -1})],
                CommandsProcessor.calls[1:])

    def test_create(self):
        self.start_server()
        client = Client()

        res = client.Resource()
        assert 'guid' not in res
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
        client = Client()

        res = client.Resource('guid')
        res['prop'] = 'value'
        res.post()

        self.assertEqual(
                [('update', 'resource', 'guid', {'prop': 'value'})],
                CommandsProcessor.calls)

    def test_get(self):
        self.start_server()
        client = Client()

        res = client.Resource('guid')
        self.assertEqual('value', res['prop'])
        self.assertEqual(
                [('get', 'resource', 'guid', None)],
                CommandsProcessor.calls)

        res = client.Resource('guid', reply=['prop'])
        self.assertEqual('value', res['prop'])
        self.assertEqual(
                [('get', 'resource', 'guid', ['prop'])],
                CommandsProcessor.calls[1:])

    def test_get_blob(self):
        self.start_server()
        client = Client()

        res = client.Resource('guid')
        blob = res.blobs['blob']

        self.assertEqual('blob-path', blob.path)
        self.assertEqual({'blob': -1}, blob.content)
        self.assertEqual('{"blob": -1}', ''.join([i for i in blob.iter_content()]))

        self.assertEqual([
            ('get_blob', 'resource', 'guid', 'blob'),
            ('get_blob', 'resource', 'guid', 'blob'),
            ('get_blob', 'resource', 'guid', 'blob'),
            ],
            CommandsProcessor.calls)

    def test_set_blob(self):
        self.start_server()
        client = Client()

        client.Resource('guid_1').blobs['blob_1'] = 'string'
        client.Resource('guid_2').blobs['blob_2'] = StringIO('stream')
        client.Resource('guid_3').blobs['blob_3'] = {'file': 'path'}
        client.Resource('guid_4').blobs['blob_4'].url = 'url'

        self.assertEqual([
            ('set_blob', 'resource', 'guid_1', 'blob_1', None, None, 'string'),
            ('set_blob', 'resource', 'guid_2', 'blob_2', None, None, 'stream'),
            ('set_blob', 'resource', 'guid_3', 'blob_3', {'file': 'path'}, None, None),
            ('set_blob', 'resource', 'guid_4', 'blob_4', None, 'url', None),
            ],
            CommandsProcessor.calls)


class CommandsProcessor(object):

    calls = []

    def create(self_, socket, resource, props):
        reply = ('create', resource, props)
        CommandsProcessor.calls.append(reply)
        return {'guid': -1}

    def update(self_, socket, resource, guid, props):
        reply = ('update', resource, guid, props)
        CommandsProcessor.calls.append(reply)

    def get(self_, socket, resource, guid, reply=None):
        reply = ('get', resource, guid, reply)
        CommandsProcessor.calls.append(reply)
        return {'guid': -1, 'prop': 'value'}

    def find(self_, socket, resource, offset=None, limit=None,
            query=None, order_by=None, reply=None, **kwargs):
        reply = ('find', resource, offset, limit, query, order_by,
                reply, kwargs)
        CommandsProcessor.calls.append(reply)
        result = [{'guid': i} for i in range(offset, offset + 3)]
        return {'total': 10, 'result': result}

    def delete(self_, socket, resource, guid):
        reply = ('delete', resource, guid)
        CommandsProcessor.calls.append(reply)

    def get_blob(self_, socket, resource, guid, prop):
        reply = ('get_blob', resource, guid, prop)
        CommandsProcessor.calls.append(reply)
        with file('blob-path', 'w') as f:
            f.write(json.dumps({'blob': -1}))
        return {'path': 'blob-path', 'mime_type': 'application/json'}

    def set_blob(self_, socket, resource, guid, prop, files=None,
            url=None):
        if files is None and url is None:
            socket.setblocking(0)
            try:
                data = socket.recv(1024)
            finally:
                socket.setblocking(1)
        else:
            data = None
        reply = ('set_blob', resource, guid, prop, files, url, data)
        CommandsProcessor.calls.append(reply)

    def ping(self_, socket, resource, guid):
        reply = ('ping', resource, guid)
        CommandsProcessor.calls.append(reply)
        return reply


if __name__ == '__main__':
    tests.main()