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
        return res['guid']

    def test_OfflineMount_create(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('~')

        guid = self.create_context('title')
        self.assertNotEqual(None, guid)

        res = self.client.Context(guid, reply=['title'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])

    def test_OfflineMount_update(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('~')

        guid = self.create_context('title')

        res_1 = self.client.Context(guid)
        res_1['title'] = 'title_2'
        res_1.post()

        res_2 = self.client.Context(guid, reply=['title'])
        self.assertEqual('title_2', res_2['title'])

    def test_OfflineMount_get(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('~')

        guid = self.create_context('title')

        context = self.client.Context(guid, reply=['guid', 'title', 'keep'])
        self.assertEqual(guid, context['guid'])
        self.assertEqual('title', context['title'])
        self.assertEqual(True, context['keep'])

    def test_OfflineMount_find(self):

        def server():
            Server(self.mounts).serve_forever()

        gevent.spawn(server)
        gevent.sleep()
        self.client = Client('~')

        guid_1 = self.create_context('title_1')
        guid_2 = self.create_context('title_2')
        guid_3 = self.create_context('title_3')

        query = self.client.Context.find(reply=['guid', 'title'])
        self.assertEqual(3, query.total)
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', True),
                    (guid_2, 'title_2', True),
                    (guid_3, 'title_3', True),
                    ]),
                sorted([(i['guid'], i['title'], i['keep']) for i in query]))

    def test_OnlineMount_GetKeeps(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        ad.data_root.value = tests.tmpdir + '/local'
        env.api_url.value = 'http://localhost:8000'
        online = self.mounts['/']

        props = {'type': 'activity',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }

        guid = online.create('context', props)['guid']

        self.assertEqual(False, online.get('context', guid)['keep'])
        self.assertEqual(
                [(guid, False)],
                [(i['guid'], i['keep']) for i in online.find('context')['result']])

        context = Context(**props)
        context.set('guid', guid, raw=True)
        context.post()

        self.assertEqual(True, online.get('context', guid)['keep'])
        self.assertEqual(
                [(guid, True)],
                [(i['guid'], i['keep']) for i in online.find('context')['result']])

    def test_OnlineMount_SetKeeps(self):
        self.fork(self.restful_server)
        gevent.sleep(1)

        ad.data_root.value = tests.tmpdir + '/local'
        env.api_url.value = 'http://localhost:8000'
        online = self.mounts['/']

        props = {'type': ['activity'],
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }

        guid = online.create('context', props)['guid']
        self.assertEqual(False, online.get('context', guid)['keep'])
        assert not Context(guid).exists

        online.update('context', guid, {'keep': False})
        self.assertEqual(False, online.get('context', guid)['keep'])
        assert not Context(guid).exists

        online.update('context', guid, {'keep': True})
        self.assertEqual(True, online.get('context', guid)['keep'])
        assert Context(guid).exists
        self.assertEqual(props, Context(guid).properties(['type', 'title', 'summary', 'description']))

        online.update('context', guid, {'keep': False})
        self.assertEqual(False, online.get('context', guid)['keep'])
        assert not Context(guid).exists

        props['keep'] = True
        guid_2 = online.create('context', props)['guid']
        assert guid_2 != guid
        self.assertEqual(True, online.get('context', guid_2)['keep'])
        assert Context(guid_2).exists
        self.assertEqual(props, Context(guid_2).properties(['type', 'title', 'summary', 'description']))

        online.delete('context', guid_2)
        self.assertRaises(RuntimeError, online.get, 'context', guid_2)
        assert not Context(guid_2).exists


if __name__ == '__main__':
    tests.main()
