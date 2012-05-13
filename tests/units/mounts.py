#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent
from gevent import socket

from __init__ import tests

import restful_document as rd
import active_document as ad

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from sugar_network.client import Client
from sugar_network.connection import ServerError
from local_document.mounts import Mounts
from local_document.server import Server
from local_document.socket import SocketFile
from local_document import env, mounts


class MountsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.server = None
        self.mounts = None
        rd.only_sync_notification.value = False
        mounts._RECONNECTION_TIMEOUT = 1

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

    def test_OfflineMount_create(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        self.assertNotEqual(None, guid)

        res = local.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])
        self.assertEqual(False, res['keep'])
        self.assertEqual(False, res['keep_impl'])
        self.assertEqual([-1, -1], res['position'])

    def test_OfflineMount_update(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        context = local.Context(guid)
        context['title'] = 'title_2'
        context['keep'] = True
        context['position'] = (2, 3)
        context.post()

        context = local.Context(guid, ['title', 'keep', 'position'])
        self.assertEqual('title_2', context['title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual([2, 3], context['position'])

    def test_OfflineMount_get(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        context = local.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, context['guid'])
        self.assertEqual('title', context['title'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual([-1, -1], context['position'])

    def test_OfflineMount_find(self):
        self.start_server()
        local = Client('~')

        guid_1 = local.Context(
                type='activity',
                title='title_1',
                summary='summary',
                description='description').post()
        guid_2 = local.Context(
                type='activity',
                title='title_2',
                summary='summary',
                description='description').post()
        guid_3 = local.Context(
                type='activity',
                title='title_3',
                summary='summary',
                description='description').post()

        cursor = local.Context.cursor(reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(3, cursor.total)
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', False, False, [-1, -1]),
                    (guid_2, 'title_2', False, False, [-1, -1]),
                    (guid_3, 'title_3', False, False, [-1, -1]),
                    ]),
                sorted([(i['guid'], i['title'], i['keep'], i['keep_impl'], i['position']) for i in cursor]))

    def test_OnlineMount_GetKeep(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        self.start_server()
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()

        context = remote.Context(guid, ['keep', 'keep_impl'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual(
                [(guid, False, False)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in remote.Context.cursor(reply=['keep', 'keep_impl'])])

        self.mounts.home_volume['context'].create_with_guid(guid, {
            'type': 'activity',
            'title': 'local',
            'summary': 'summary',
            'description': 'description',
            'keep': True,
            'keep_impl': True,
            })

        context = remote.Context(guid, ['keep', 'keep_impl'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(True, context['keep_impl'])
        self.assertEqual(
                [(guid, True, True)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in remote.Context.cursor(reply=['keep', 'keep_impl'])])

    def test_OnlineMount_SetKeep(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        self.start_server()
        remote = Client('/')
        local = Client('~')

        guid = remote.Context(
                type=['activity'],
                title='remote',
                summary='summary',
                description='description').post()

        self.assertRaises(ServerError, lambda: local.Context(guid, reply=['title'])['title'])

        remote.Context(guid, keep=True).post()

        context = local.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('remote', context['title'])

        context = remote.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('remote', context['title'])

        remote.Context(guid, keep=False).post()

        context = local.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('remote', context['title'])

        context = remote.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('remote', context['title'])

        context = local.Context(guid)
        context['title'] = 'local'
        context.post()
        context = local.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual('local', context['title'])

        remote.Context(guid, keep=True).post()

        context = local.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('local', context['title'])

        context = remote.Context(guid, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual('remote', context['title'])

    def test_OfflineSubscription(self):
        self.start_server()
        client = Client('~')

        subscription = SocketFile(socket.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        gevent.sleep()

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        socket.wait_read(subscription.fileno())
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'create', 'guid': guid},
                event)
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context(guid, title='new-title').post()

        socket.wait_read(subscription.fileno())
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid},
                event)
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context.delete(guid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

    def test_OnlineSubscription(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        self.start_server()
        client = Client('/')

        subscription = SocketFile(socket.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        gevent.sleep(1)

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect'},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'create', 'guid': guid},
                event)

        client.Context(guid, title='new-title').post()

        socket.wait_read(subscription.fileno())
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                event)

        client.Context.delete(guid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())

    def test_OfflineSubscription_NotifyOnline(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        self.start_server()
        local = Client('~')
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description',
                keep=True).post()

        subscription = SocketFile(socket.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        gevent.sleep(1)

        local.Context(guid, keep=False).post()
        gevent.sleep(1)

        socket.wait_read(subscription.fileno())
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid},
                event)
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())

    def test_OfflineConnect(self):
        self.start_server()
        client = Client('~')

        self.assertEqual(True, client.connected)

    def test_OnlineConnect(self):
        pid = self.fork(self.restful_server)
        gevent.sleep(1)

        self.start_server()
        client = Client('/')

        subscription = SocketFile(socket.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        gevent.sleep(1)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect'},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'event': 'disconnect'},
                subscription.read_message())
        self.assertEqual(False, client.connected)

        pid = self.fork(self.restful_server)
        gevent.sleep(1)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect'},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'event': 'disconnect'},
                subscription.read_message())
        self.assertEqual(False, client.connected)


if __name__ == '__main__':
    tests.main()
