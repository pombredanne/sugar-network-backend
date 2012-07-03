#!/usr/bin/env python
# sugar-lint: disable

import os
from os.path import isdir

from __init__ import tests

import active_document as ad

from active_toolkit import coroutine
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.local.mounts import HomeMount
from sugar_network.local.mountset import Mountset
from sugar_network import Client
from sugar_network.resources.volume import Volume


class IPCClientTest(tests.Test):

    def test_RealtimeUpdates(self):
        self.start_server()

        client_1 = Client('~')
        client_2 = Client('~')

        events_1 = []
        events_2 = []

        def waiter(cursor, events):
            for i in cursor.read_events():
                events.append(i)

        cursor_1 = client_1.Context.cursor(reply=['guid', 'title'])
        self.assertEqual(0, cursor_1.total)
        coroutine.spawn(waiter, cursor_1, events_1)

        cursor_2 = client_2.Context.cursor(reply=['guid', 'title'])
        self.assertEqual(0, cursor_2.total)
        coroutine.spawn(waiter, cursor_2, events_2)

        guid_1 = client_1.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        coroutine.sleep(.1)
        self.assertEqual([None], events_1)
        self.assertEqual([None], events_2)
        self.assertEqual(
                sorted([
                    (guid_1, 'title'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    (guid_1, 'title'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

        client_1.Context(guid_1, title='title-2').post()
        coroutine.sleep(.1)
        self.assertEqual([None], events_1[1:])
        self.assertEqual([None], events_2[1:])
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

        guid_2 = client_2.Context(
                type='activity',
                title='title-3',
                summary='summary',
                description='description').post()
        coroutine.sleep(.1)
        self.assertEqual([None], events_1[2:])
        self.assertEqual([None], events_2[2:])
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    (guid_2, 'title-3'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    (guid_2, 'title-3'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

        client_2.Context(guid_2, title='title-4').post()
        coroutine.sleep(.1)
        self.assertEqual([None], events_1[3:])
        self.assertEqual([None], events_2[3:])
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    (guid_2, 'title-4'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    (guid_1, 'title-2'),
                    (guid_2, 'title-4'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

        client_1.Context.delete(guid_1)
        coroutine.sleep(.1)
        self.assertEqual([None], events_1[4:])
        self.assertEqual([None], events_2[4:])
        self.assertEqual(
                sorted([
                    (guid_2, 'title-4'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    (guid_2, 'title-4'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

        client_2.Context.delete(guid_2)
        coroutine.sleep(.1)
        self.assertEqual([None], events_1[5:])
        self.assertEqual([None], events_2[5:])
        self.assertEqual(
                sorted([
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))

    def test_ReplaceReadEventsCalls(self):
        self.start_server()

        client = Client('~')
        cursor = client.Context.cursor(reply=['guid', 'title'])

        def waiter(cursor, events):
            for i in cursor.read_events():
                events.append(i)

        events_1 = []
        coroutine.spawn(waiter, cursor, events_1)
        coroutine.sleep()

        events_2 = []
        coroutine.spawn(waiter, cursor, events_2)
        coroutine.sleep()

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        coroutine.sleep()

        self.assertEqual([], events_1)
        self.assertEqual([None], events_2)

        events_3 = []
        coroutine.spawn(waiter, cursor, events_3)
        coroutine.sleep()

        client.Context(guid, title='title-1').post()
        coroutine.sleep()

        self.assertEqual([], events_1)
        self.assertEqual([None], events_2)
        self.assertEqual([None], events_3)

    def test_Cursor_Gets(self):
        self.start_server()

        client = Client('~')

        guid_1 = client.Context(
                type='activity',
                title='title-1',
                summary='summary',
                description='description').post()
        guid_2 = client.Context(
                type='activity',
                title='title-2',
                summary='summary',
                description='description').post()
        guid_3 = client.Context(
                type='activity',
                title='title-3',
                summary='summary',
                description='description').post()

        cursor = client.Context.cursor(reply=['guid', 'title'])
        self.assertEqual('title-1', cursor[0]['title'])
        self.assertEqual('title-2', cursor[1]['title'])
        self.assertEqual('title-3', cursor[2]['title'])
        self.assertEqual('title-1', cursor[guid_1]['title'])
        self.assertEqual('title-2', cursor[guid_2]['title'])
        self.assertEqual('title-3', cursor[guid_3]['title'])

        cursor = client.Context.cursor('FOO', reply=['guid', 'title'])
        self.assertEqual(0, cursor.total)
        self.assertEqual('title-1', cursor[guid_1]['title'])
        self.assertEqual('title-2', cursor[guid_2]['title'])
        self.assertEqual('title-3', cursor[guid_3]['title'])

    def test_ConnectEventsInCursor(self):

        def remote_server():
            coroutine.sleep(1)
            self.restful_server()

        pid = self.fork(self.restful_server)

        self.start_server()
        client = Client('/')

        events = []
        cursor = client.Context.cursor(reply=['guid', 'title'])

        def waiter():
            for i in cursor.read_events():
                events.append(i)

        coroutine.spawn(waiter)
        coroutine.sleep(.1)

        self.assertEqual([], events)

        coroutine.sleep(1)

        self.assertEqual([None], events)

        self.waitpid(pid)
        coroutine.sleep(1)

        self.assertEqual([None, None], events)

    def test_PublishEvents(self):
        self.start_server()

        events = []
        Client.connect(lambda event: events.append(event))

        Client.publish('probe', payload=1)
        Client.publish('probe', payload=2)
        Client.publish('probe', payload=3)
        coroutine.sleep()

        self.assertEqual([
            {'payload': 1, 'event': 'probe'},
            {'payload': 2, 'event': 'probe'},
            {'payload': 3, 'event': 'probe'},
            ],
            events)

    def test_GetBLOBs(self):

        class Mounts(object):

            def call(self_, request, response):
                if not (request['cmd'] == 'get_blob' and \
                        request['document'] == 'document' and \
                        'guid' in request):
                    return
                if request['prop'] == 'directory':
                    os.makedirs('directory')
                    return {'path': tests.tmpdir + '/directory', 'mime_type': 'fake'}
                elif request['prop'] == 'file':
                    self.touch(('file', 'file'))
                    return {'path': tests.tmpdir + '/file', 'mime_type': 'fake'}
                elif request['prop'] == 'value':
                    return 'value'

            def close(self):
                pass

        self.start_server([])
        self.server._mounts = Mounts()
        client = Client('~')

        blob = client.Document('guid').get_blob_path('directory')
        self.assertEqual(tests.tmpdir + '/directory', blob[0])
        self.assertEqual('fake', blob[1])
        assert isdir('directory')

        blob = client.Document('guid').get_blob('file')
        self.assertEqual(tests.tmpdir + '/file', blob.name)
        self.assertEqual('fake', blob.mime_type)
        self.assertEqual('file', blob.read())

        blob = client.Document('guid').get_blob('value')
        self.assertEqual('value', blob.read())

    def test_Direct(self):
        volume = Volume('local', [User, Context])
        Client._connection = Mountset(volume)
        Client._connection['~'] = HomeMount(volume)
        Client._connection.open()
        client = Client('~')

        guid_1 = client.Context(
                type='activity',
                title='title-1',
                summary='summary',
                description='description').post()
        guid_2 = client.Context(
                type='activity',
                title='title-2',
                summary='summary',
                description='description').post()

        self.assertEqual(
                'title-1',
                client.Context(guid_1, reply=['title'])['title'])

        self.assertEqual(
                sorted([
                    (guid_1, 'title-1'),
                    (guid_2, 'title-2'),
                    ]),
                sorted([(i.guid, i['title']) \
                        for i in client.Context.cursor(reply=['title'])]))

        self.touch(('file', 'blob'))
        client.Context(guid_2).upload_blob('preview', 'file')
        self.assertEqual(
                'blob',
                client.Context(guid_2).get_blob('preview').read())

        client.Context.delete(guid_1)
        client.Context.delete(guid_2)
        self.assertEqual(0, client.Context.cursor().total)


if __name__ == '__main__':
    tests.main()
