#!/usr/bin/env python
# sugar-lint: disable

import gevent

from __init__ import tests

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

import active_document as ad

from sugar_network.client import Client
from local_document.bus import Server
from local_document.mounts import Mounts
from local_document import mounts


class ClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.server = None
        self.mounts = None

    def tearDown(self):
        if self.server is not None:
            self.server.stop()
        tests.Test.tearDown(self)

    def start_server(self):

        def server():
            self.server.serve_forever()

        self.server = Server('local', [User, Context])
        gevent.spawn(server)
        gevent.sleep()
        self.mounts = self.server._mounts

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
        gevent.spawn(waiter, cursor_1, events_1)

        cursor_2 = client_2.Context.cursor(reply=['guid', 'title'])
        self.assertEqual(0, cursor_2.total)
        gevent.spawn(waiter, cursor_2, events_2)

        guid_1 = client_1.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        gevent.sleep(.1)
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
        gevent.sleep(.1)
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
        gevent.sleep(.1)
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
        gevent.sleep(.1)
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
        gevent.sleep(.1)
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
        gevent.sleep(.1)
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
        gevent.spawn(waiter, cursor, events_1)
        gevent.sleep()

        events_2 = []
        gevent.spawn(waiter, cursor, events_2)
        gevent.sleep()

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        gevent.sleep()

        self.assertEqual([], events_1)
        self.assertEqual([None], events_2)

        events_3 = []
        gevent.spawn(waiter, cursor, events_3)
        gevent.sleep()

        client.Context(guid, title='title-1').post()
        gevent.sleep()

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
        mounts._RECONNECTION_TIMEOUT = .1

        def remote_server():
            gevent.sleep(1)
            self.restful_server()

        pid = self.fork(self.restful_server)

        self.start_server()
        client = Client('/')

        events = []
        cursor = client.Context.cursor(reply=['guid', 'title'])

        def waiter():
            for i in cursor.read_events():
                events.append(i)

        gevent.spawn(waiter)
        gevent.sleep(.5)

        self.assertEqual([], events)

        gevent.sleep(1)

        self.assertEqual([None], events)

        self.waitpid(pid)
        gevent.sleep(1)

        self.assertEqual([None, None], events)







if __name__ == '__main__':
    tests.main()
