#!/usr/bin/env python
# sugar-lint: disable

import os
import json
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.client import Client, api_url
from sugar_network.node import sync, stats_user, files_root
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

        self.stats_commit = []
        self.stats_merge = []
        def stats_diff():
            yield {'stats': 'probe'}
        self.override(stats_user, 'diff', stats_diff)
        def stats_merge(packet):
            self.stats_merge.extend([i for i in packet])
            return 'ok'
        self.override(stats_user, 'merge', stats_merge)
        self.override(stats_user, 'commit', lambda seq: self.stats_commit.append(seq))

        class Document(Feedback):
            pass

        api_url.value = 'http://localhost:9000'

        files_root.value = 'master/files'
        self.master_volume = Volume('master', [User, Document])
        self.master_server = coroutine.WSGIServer(('localhost', 9000), Router(MasterCommands(self.master_volume)))
        coroutine.spawn(self.master_server.serve_forever)
        coroutine.dispatch()
        client = Client('http://localhost:9000')
        client.get(cmd='whoami')

        files_root.value = 'slave/files'
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

        # Sync users
        client.post(cmd='online_sync')
        self.assertEqual([[2, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid1 = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        guid2 = client.post(['document'], {'context': '', 'content': '2', 'title': '', 'type': 'idea'})

        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'content']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[4, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[4, None]], json.load(file('slave/push.sequence')))

        guid3 = client.post(['document'], {'context': '', 'content': '3', 'title': '', 'type': 'idea'})
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            {'guid': guid3, 'content': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'content']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[5, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[5, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid2], {'content': '22'})
        client.post(cmd='online_sync')
        self.assertEqual(
                {'guid': guid2, 'content': {'en-us': '22'}},
                self.master_volume['document'].get(guid2).properties(['guid', 'content']))
        self.assertEqual([[6, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[6, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': '22'}, 'layer': ['public']},
            {'guid': guid3, 'content': {'en-us': '3'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[7, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[7, None]], json.load(file('slave/push.sequence')))

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
        self.assertEqual([[11, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[12, None]], json.load(file('slave/push.sequence')))

    def test_PushStats(self):
        stats_user.stats_user.value = True
        client = Client('http://localhost:9001')
        client.post(cmd='online_sync')
        self.assertEqual(['ok'], self.stats_commit)
        self.assertEqual([{'stats': 'probe'}], self.stats_merge)

    def test_Pull(self):
        client = Client('http://localhost:9000')
        slave_client = Client('http://localhost:9001')

        # Sync users
        slave_client.post(cmd='online_sync')
        self.assertEqual([[2, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid1 = client.post(['document'], {'context': '', 'content': '1', 'title': '', 'type': 'idea'})
        guid2 = client.post(['document'], {'context': '', 'content': '2', 'title': '', 'type': 'idea'})

        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'content']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[4, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid3 = client.post(['document'], {'context': '', 'content': '3', 'title': '', 'type': 'idea'})
        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}},
            {'guid': guid2, 'content': {'en-us': '2'}},
            {'guid': guid3, 'content': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'content']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[5, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid2], {'content': '22'})
        slave_client.post(cmd='online_sync')
        self.assertEqual(
                {'guid': guid2, 'content': {'en-us': '22'}},
                self.slave_volume['document'].get(guid2).properties(['guid', 'content']))
        self.assertEqual([[6, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        slave_client.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'content': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'content': {'en-us': '22'}, 'layer': ['public']},
            {'guid': guid3, 'content': {'en-us': '3'}, 'layer': ['public']},
            ],
            [i.properties(['guid', 'content', 'layer']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[7, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

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
        self.assertEqual([[12, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

    def test_PullFiles(self):
        self.touch(('master/files/1', 'a', 1))
        self.touch(('master/files/2/2', 'bb', 2))
        self.touch(('master/files/3/3/3', 'ccc', 3))
        os.utime('master/files', (1, 1))

        client = Client('http://localhost:9001')
        client.post(cmd='online_sync')

        files, stamp = json.load(file('master/files.index'))
        self.assertEqual(1, stamp)
        self.assertEqual(sorted([
            [2, '1', 1],
            [3, '2/2', 2],
            [4, '3/3/3', 3],
            ]),
            sorted(files))

        self.assertEqual([[5, None]], json.load(file('slave/files.sequence')))
        self.assertEqual('a', file('slave/files/1').read())
        self.assertEqual('bb', file('slave/files/2/2').read())
        self.assertEqual('ccc', file('slave/files/3/3/3').read())


if __name__ == '__main__':
    tests.main()
