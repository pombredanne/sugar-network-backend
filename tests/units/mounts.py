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

from active_document import SingleFolder
from sugar_network.client import Client
from local_document.mounts import Mounts
from local_document.server import Server
from local_document import env


class MountsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.mounts = Mounts([User, Context])
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

        res = self.client.Context(guid)
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
        self.client = Client('~')

        guid = self.create_context('title')['guid']

        context = self.client.Context(guid)
        context['title'] = 'title_2'
        context['keep'] = True
        context['position'] = (2, 3)
        context.post()

        context = self.client.Context(guid)
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

        context = self.client.Context(guid)
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

        query = self.client.Context.find(reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(3, query.total)
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', False, False, [-1, -1]),
                    (guid_2, 'title_2', False, False, [-1, -1]),
                    (guid_3, 'title_3', False, False, [-1, -1]),
                    ]),
                sorted([(i['guid'], i['title'], i['keep'], i['keep_impl'], i['position']) for i in query]))

    def test_OnlineMount_GetKeep(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('/')

        self.fork(self.restful_server)
        gevent.sleep(1)

        props = {'type': 'activity',
                 'title': 'remote',
                 'summary': 'summary',
                 'description': 'description',
                 }
        online = self.mounts['/']
        guid = online.create('context', props)['guid']

        context = self.client.Context(guid)
        self.assertEqual(False, context['keep'])
        self.assertEqual(False, context['keep_impl'])
        self.assertEqual(
                [(guid, False, False)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in self.client.Context.find()])

        self.mounts['~'].folder['context'].create_with_guid(guid, {
            'type': 'activity',
            'title': 'local',
            'summary': 'summary',
            'description': 'description',
            'keep': True,
            'keep_impl': True,
            })

        context = self.client.Context(guid)
        self.assertEqual(True, context['keep'])
        self.assertEqual(True, context['keep_impl'])
        self.assertEqual(
                [(guid, True, True)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in self.client.Context.find()])

    def test_OnlineMount_SetKeep(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        props = {'type': ['activity'],
                 'title': 'remote',
                 'summary': 'summary',
                 'description': 'description',
                 }
        online = self.mounts['/']
        guid = online.create('context', props)['guid']

        OfflineContext = self.mounts['~'].folder['context']
        assert not OfflineContext(guid).exists

        online.update('context', guid, {'keep': True})

        local = OfflineContext(guid)
        assert local.exists
        self.assertEqual(True, local['keep'])
        self.assertEqual(False, local['keep_impl'])
        self.assertEqual('remote', local['title'])

        remote = OfflineContext(guid)
        self.assertEqual(True, remote['keep'])
        self.assertEqual(False, remote['keep_impl'])

        online.update('context', guid, {'keep': False})

        local = OfflineContext(guid)
        assert local.exists
        self.assertEqual(False, local['keep'])
        self.assertEqual(False, local['keep_impl'])
        self.assertEqual('remote', local['title'])

        remote = OfflineContext(guid)
        self.assertEqual(False, remote['keep'])
        self.assertEqual(False, remote['keep_impl'])

        local = OfflineContext(guid)
        local['title'] = 'local'
        local.post()
        local = OfflineContext(guid)
        self.assertEqual('local', local['title'])

        online.update('context', guid, {'keep': True})

        local = OfflineContext(guid)
        assert local.exists
        self.assertEqual(True, local['keep'])
        self.assertEqual(False, local['keep_impl'])
        self.assertEqual('local', local['title'])

        remote = OfflineContext(guid)
        self.assertEqual(True, remote['keep'])
        self.assertEqual(False, remote['keep_impl'])


if __name__ == '__main__':
    tests.main()
