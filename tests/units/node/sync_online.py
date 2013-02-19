#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
from sugar_network.client import Client, api_url
from sugar_network.node import sync
from sugar_network.node.master import MasterCommands
from sugar_network.node.slave import SlaveCommands
from sugar_network.resources.volume import Volume
from sugar_network.resources.user import User
from sugar_network.resources.feedback import Feedback
from sugar_network.toolkit.router import Request, Router
from sugar_network.toolkit import util, coroutine


class SyncOnlineTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class Document(Feedback):
            pass

        self.master_volume = Volume('master', [User, Document])
        self.master_server = coroutine.WSGIServer(('localhost', 9000), Router(MasterCommands(self.master_volume)))
        coroutine.spawn(self.master_server.serve_forever)
        coroutine.dispatch()
        client = Client('http://localhost:9000')
        client.get(cmd='whoami')

        api_url.value = 'http://localhost:9000'
        self.slave_volume = Volume('slave', [User, Document])
        self.slave_server = coroutine.WSGIServer(('localhost', 9001), Router(SlaveCommands('slave', self.slave_volume)))
        coroutine.spawn(self.slave_server.serve_forever)
        coroutine.dispatch()

    def tearDown(self):
        self.master_server.stop()
        self.slave_server.stop()
        tests.Test.tearDown(self)

    def test_sync_Creaqte(self):
        client = Client('http://localhost:9001')

        guid1 = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        guid2 = client.post(['document'], {'context': '', 'content': '2', 'title': '', 'type': 'idea'})

        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'content']) for i in self.master_volume['document'].find()[0]])

        guid3 = client.post(['document'], {'context': '', 'content': '3', 'title': '', 'type': 'idea'})
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            {'guid': guid3, 'content': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'content']) for i in self.master_volume['document'].find()[0]])

    def test_sync_Update(self):
        client = Client('http://localhost:9001')

        guid = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        client.post(cmd='online_sync')
        coroutine.sleep(1)

        client.put(['document', guid], {'content': '2'})
        client.post(cmd='online_sync')
        self.assertEqual(
                {'guid': guid, 'content': {'en-us': '2'}},
                self.master_volume['document'].get(guid).properties(['guid', 'content']))

    def test_sync_Delete(self):
        client = Client('http://localhost:9001')

        guid1 = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        guid2 = client.post(['document'], {'context': '', 'content': '2', 'title': '', 'type': 'idea'})
        guid3 = client.post(['document'], {'context': '', 'content': '3', 'title': '', 'type': 'idea'})
        client.post(cmd='online_sync')
        coroutine.sleep(1)

        client.delete(['document', guid2])
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}, 'layer': ['public']},
            {'guid': guid2, 'content': {'en-us': '2'}, 'layer': ['deleted']},
            {'guid': guid3, 'content': {'en-us': '3'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.master_volume['document'].find()[0]])



if __name__ == '__main__':
    tests.main()
