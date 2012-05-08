#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent

from __init__ import tests

import active_document as ad

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from sugar_network.client import Client
from local_document.mounts import Mounts
from local_document.server import Server
from local_document import env


class MountsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.mounts = Mounts('local', [User, Context])
        self.client = None

    def tearDown(self):
        if self.client is not None:
            self.client.close()
        self.mounts.close()
        tests.Test.tearDown(self)

    def create_context(self, title):
        res = self.client.Context()
        res['type'] = 'activity'
        res['title'] = title
        res['summary'] = 'summary'
        res['description'] = 'description'
        res.post()
        return res

    def test_OfflineMount_create(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('~')

        guid = self.create_context('title')['guid']
        self.assertNotEqual(None, guid)

        res = self.client.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
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
        self.client = Client('~')

        guid = self.create_context('title')['guid']

        context = self.client.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
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
        self.client = Client('~')

        guid_1 = self.create_context('title_1')['guid']
        guid_2 = self.create_context('title_2')['guid']
        guid_3 = self.create_context('title_3')['guid']

        cursor = self.client.Context.cursor(reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
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

        self.mounts['~'].volume['context'].create_with_guid(guid, {
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


if __name__ == '__main__':
    tests.main()
