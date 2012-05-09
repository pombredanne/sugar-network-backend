#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent
from gevent import socket

from __init__ import tests

import active_document as ad

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from sugar_network.client import Client
from local_document.mounts import Mounts
from local_document.server import Server
from local_document.socket import SocketFile
from local_document import env


class MountsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.mounts = Mounts('local', [User, Context])

    def tearDown(self):
        self.mounts.close()
        tests.Test.tearDown(self)

    def test_OfflineMount_create(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
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

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
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

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
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

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
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

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        remote = Client('/')

        self.fork(self.restful_server)
        gevent.sleep(1)

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

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        remote = Client('/')
        local = Client('~')

        guid = remote.Context(
                type=['activity'],
                title='remote',
                summary='summary',
                description='description').post()

        self.assertRaises(KeyError, lambda: local.Context(guid, reply=['title'])['title'])

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
        ad.only_commits_notification.value = False

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
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
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'create', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context(guid, title='new-title').post()

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context.delete(guid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

    def test_OnlineSubscription(self):
        ad.only_commits_notification.value = False

        self.fork(self.restful_server)
        gevent.sleep(1)

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        client = Client('/')

        subscription = SocketFile(socket.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        gevent.sleep(1)

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'create', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context(guid, title='new-title').post()

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'commit'},
                subscription.read_message())

        client.Context.delete(guid)

        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())
        socket.wait_read(subscription.fileno())
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'commit'},
                subscription.read_message())


if __name__ == '__main__':
    tests.main()
