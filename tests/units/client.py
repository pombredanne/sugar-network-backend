#!/usr/bin/env python
# sugar-lint: disable

import gevent

from __init__ import tests

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from sugar_network.client import Client
from local_document.server import Server
from local_document.mounts import Mounts


class ClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.mounts = Mounts('local', [User, Context])

    def tearDown(self):
        self.mounts.close()
        tests.Test.tearDown(self)

    def test_RealtimeUpdates(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()

        client_1 = Client('~')
        client_2 = Client('~')

        events_1 = []
        events_2 = []

        cursor_1 = client_1.Context.cursor(reply=['guid', 'title'])
        cursor_1.connect(lambda: events_1.append(True))
        self.assertEqual(0, cursor_1.total)
        cursor_2 = client_2.Context.cursor(reply=['guid', 'title'])
        cursor_2.connect(lambda: events_2.append(True))
        self.assertEqual(0, cursor_2.total)

        guid_1 = client_1.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        gevent.sleep()
        self.assertEqual([True], events_1)
        self.assertEqual([True], events_2)
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
        gevent.sleep()
        self.assertEqual([True], events_1[1:])
        self.assertEqual([True], events_2[1:])
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
        gevent.sleep()
        self.assertEqual([True], events_1[2:])
        self.assertEqual([True], events_2[2:])
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
        gevent.sleep()
        self.assertEqual([True], events_1[3:])
        self.assertEqual([True], events_2[3:])
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
        gevent.sleep()
        self.assertEqual([True], events_1[4:])
        self.assertEqual([True], events_2[4:])
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
        gevent.sleep()
        self.assertEqual([True], events_1[5:])
        self.assertEqual([True], events_2[5:])
        self.assertEqual(
                sorted([
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_1]))
        self.assertEqual(
                sorted([
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor_2]))


if __name__ == '__main__':
    tests.main()
