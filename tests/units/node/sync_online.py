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

    def test_Push(self):
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

        coroutine.sleep(1)
        client.put(['document', guid2], {'content': '22'})
        client.post(cmd='online_sync')
        self.assertEqual(
                {'guid': guid2, 'content': {'en-us': '22'}},
                self.master_volume['document'].get(guid2).properties(['guid', 'content']))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': '22'}, 'layer': ['public']},
            {'guid': guid3, 'content': {'en-us': '3'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.master_volume['document'].find()[0]])

        coroutine.sleep(1)
        client.put(['document', guid1], {'content': 'a'})
        client.put(['document', guid2], {'content': 'b'})
        client.put(['document', guid3], {'content': 'c'})
        guid4 = client.post(['document'], {'context': '', 'content': 'd', 'title': '', 'type': 'idea'})
        client.delete(['document', guid2])
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': 'a'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': 'b'}, 'layer': ['deleted']},
            {'guid': guid3, 'content': {'en-us': 'c'}, 'layer': ['public']},
            {'guid': guid4, 'content': {'en-us': 'd'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.master_volume['document'].find()[0]])

    def test_Pull(self):
        client = Client('http://localhost:9000')
        slave_client = Client('http://localhost:9001')

        guid1 = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        guid2 = client.post(['document'], {'context': '', 'content': '2', 'title': '', 'type': 'idea'})

        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'content']) for i in self.slave_volume['document'].find()[0]])

        guid3 = client.post(['document'], {'context': '', 'content': '3', 'title': '', 'type': 'idea'})
        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            {'guid': guid3, 'content': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'content']) for i in self.slave_volume['document'].find()[0]])

        coroutine.sleep(1)
        client.put(['document', guid2], {'content': '22'})
        slave_client.post(cmd='online_sync')
        self.assertEqual(
                {'guid': guid2, 'content': {'en-us': '22'}},
                self.slave_volume['document'].get(guid2).properties(['guid', 'content']))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': '22'}, 'layer': ['public']},
            {'guid': guid3, 'content': {'en-us': '3'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.slave_volume['document'].find()[0]])

        coroutine.sleep(1)
        client.put(['document', guid1], {'content': 'a'})
        client.put(['document', guid2], {'content': 'b'})
        client.put(['document', guid3], {'content': 'c'})
        guid4 = client.post(['document'], {'context': '', 'content': 'd', 'title': '', 'type': 'idea'})
        client.delete(['document', guid2])
        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': 'a'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': 'b'}, 'layer': ['deleted']},
            {'guid': guid3, 'content': {'en-us': 'c'}, 'layer': ['public']},
            {'guid': guid4, 'content': {'en-us': 'd'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.slave_volume['document'].find()[0]])


if __name__ == '__main__':
    tests.main()
