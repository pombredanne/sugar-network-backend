#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent

from __init__ import tests

from active_document import SingleFolder
from sugar_network.ipc_client import OfflineClient
from local_document.commands import OfflineCommands
from local_document.ipc_server import Server
from sugar_network_server import resources


class OfflineCommandsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        def server():
            cp = OfflineCommands({'context': self.folder['context']})
            Server(None, cp).serve_forever()

        self.folder = SingleFolder(resources.path)
        gevent.spawn(server)
        gevent.sleep()

        self.client = OfflineClient()

    def tearDown(self):
        self.client.close()
        self.folder.close()
        tests.Test.tearDown(self)

    def create_context(self, title):
        res = self.client.Context()
        res['type'] = 'activity'
        res['title'] = title
        res['summary'] = 'summary'
        res['description'] = 'description'
        res.post()
        return res['guid']

    def test_create(self):
        guid = self.create_context('title')
        self.assertNotEqual(None, guid)

        res = self.client.Context(guid, reply=['title'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])

    def test_update(self):
        guid = self.create_context('title')

        res_1 = self.client.Context(guid)
        res_1['title'] = 'title_2'
        res_1.post()

        res_2 = self.client.Context(guid, reply=['title'])
        self.assertEqual('title_2', res_2['title'])

    def test_find(self):
        guid_1 = self.create_context('title_1')
        guid_2 = self.create_context('title_2')
        guid_3 = self.create_context('title_3')

        query = self.client.Context.find(reply=['guid', 'title'])
        self.assertEqual(3, query.total)
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1'),
                    (guid_2, 'title_2'),
                    (guid_3, 'title_3'),
                    ]),
                sorted([(i['guid'], i['title']) for i in query]))


if __name__ == '__main__':
    tests.main()
