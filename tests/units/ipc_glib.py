#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent
import gobject

from __init__ import tests

from sugar_network.ipc_glib_client import GlibClient, ServerError
from local_document.ipc_server import Server


class IPCTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        CommandsProcessor.calls = []

    def start_server(self):

        def server():
            Server(None, CommandsProcessor()).serve_forever()

        gevent.spawn(server)
        gevent.sleep()

    def test_Rendezvous(self):

        def server():
            time.sleep(1)
            Server(None, CommandsProcessor()).serve_forever()

        ts = time.time()
        fork = self.fork(server)

        client = GlibClient(False)
        reply = client.get('context', 'guid')
        self.assertEqual({'guid': -1, 'prop': 'value'}, reply)
        assert time.time() - ts >= 1

    def test_get(self):
        self.start_server()
        client = GlibClient(False)

        reply = client.get('resource', 'guid')
        self.assertEqual({'guid': -1, 'prop': 'value'}, reply)
        self.assertEqual(
                [('get', 'resource', 'guid', None)],
                CommandsProcessor.calls)

        reply = client.get('resource', 'guid', reply=['prop'])
        self.assertEqual({'guid': -1, 'prop': 'value'}, reply)
        self.assertEqual(
                [('get', 'resource', 'guid', ['prop'])],
                CommandsProcessor.calls[1:])

    def test_get_async(self):
        self.start_server()
        client = GlibClient(False)

        replies = []
        errors = []

        def reply_handler(reply):
            replies.append(reply)

        def error_handler(error):
            errors.append(error)

        reply = client.get('resource', 'guid', reply_handler, error_handler)
        self.assertEqual(None, reply)

        gevent.sleep(.5)
        mainloop = gobject.MainLoop()
        gobject.timeout_add(500, mainloop.quit)
        mainloop.run()

        self.assertEqual(
                [{'guid': -1, 'prop': 'value'}],
                replies)
        self.assertEqual(
                [],
                errors)

    def test_get_blob(self):
        self.start_server()
        client = GlibClient(False)

        reply = client.get_blob('resource', 'guid', 'blob')
        self.assertEqual(
                {'path': 'blob-path', 'mime_type': 'application/json'},
                reply)
        self.assertEqual([
            ('get_blob', 'resource', 'guid', 'blob'),
            ],
            CommandsProcessor.calls)

    def test_get_blob_async(self):
        self.start_server()
        client = GlibClient(False)

        replies = []
        errors = []

        def reply_handler(reply):
            replies.append(reply)

        def error_handler(error):
            errors.append(error)

        reply = client.get_blob('resource', 'guid', 'blob',
                reply_handler, error_handler)
        self.assertEqual(None, reply)

        gevent.sleep(.5)
        mainloop = gobject.MainLoop()
        gobject.timeout_add(500, mainloop.quit)
        mainloop.run()

        self.assertEqual(
                [{'path': 'blob-path', 'mime_type': 'application/json'}],
                replies)
        self.assertEqual(
                [],
                errors)

    def test_get_blob_EmptyBlob(self):
        self.start_server()
        client = GlibClient(False)

        reply = client.get_blob('resource', 'guid', 'empty')
        self.assertEqual(None, reply)
        self.assertEqual([
            ('get_blob', 'resource', 'guid', 'empty'),
            ],
            CommandsProcessor.calls)

    def test_Exception(self):
        self.start_server()
        client = GlibClient(False)
        self.assertRaises(ServerError, client.get, '', '')

    def test_Exception_async(self):
        self.start_server()
        client = GlibClient(False)

        replies = []
        errors = []

        def reply_handler(reply):
            replies.append(reply)

        def error_handler(error):
            errors.append(error)

        reply = client.get('', '', reply_handler, error_handler)
        self.assertEqual(None, reply)

        gevent.sleep(.5)
        mainloop = gobject.MainLoop()
        gobject.timeout_add(500, mainloop.quit)
        mainloop.run()

        self.assertEqual(
                [],
                replies)
        self.assertEqual(1, len(errors))
        self.assertEqual('fail', str(errors[0]))


class CommandsProcessor(object):

    calls = []

    def get(self, socket, resource, guid, reply=None):
        reply = ('get', resource, guid, reply)
        CommandsProcessor.calls.append(reply)
        if not resource:
            raise RuntimeError('fail')
        return {'guid': -1, 'prop': 'value'}

    def get_blob(self, socket, resource, guid, prop):
        reply = ('get_blob', resource, guid, prop)
        CommandsProcessor.calls.append(reply)
        if prop == 'empty':
            return None
        with file('blob-path', 'w') as f:
            f.write(json.dumps({'blob': -1}))
        return {'path': 'blob-path', 'mime_type': 'application/json'}


if __name__ == '__main__':
    tests.main()
