#!/usr/bin/env python
# sugar-lint: disable

import os
import json
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection, api_url, keyfile
from sugar_network.node import sync, stats_user, files_root
from sugar_network.node.master import MasterRoutes
from sugar_network.node.slave import SlaveRoutes
from sugar_network.db.volume import Volume
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import coroutine, http


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

        class Document(db.Resource):

            @db.indexed_property(prefix='C')
            def context(self, value):
                return value

            @db.indexed_property(prefix='T')
            def type(self, value):
                return value

            @db.indexed_property(db.Localized, slot=1, prefix='N', full_text=True)
            def title(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='D', full_text=True)
            def message(self, value):
                return value

        api_url.value = 'http://127.0.0.1:9000'

        files_root.value = 'master/files'
        self.master_volume = Volume('master', [User, Document])
        self.master_server = coroutine.WSGIServer(('127.0.0.1', 9000), Router(MasterRoutes('127.0.0.1:9000', self.master_volume)))
        coroutine.spawn(self.master_server.serve_forever)
        coroutine.dispatch()

        files_root.value = 'slave/files'
        self.slave_volume = Volume('slave', [User, Document])
        self.slave_server = coroutine.WSGIServer(('127.0.0.1', 9001), Router(SlaveRoutes('slave/node', self.slave_volume)))
        coroutine.spawn(self.slave_server.serve_forever)
        coroutine.dispatch()

    def tearDown(self):
        self.master_server.stop()
        self.slave_server.stop()
        tests.Test.tearDown(self)

    def test_Push(self):
        client = Connection('http://127.0.0.1:9001', auth=http.SugarAuth(keyfile.value))

        # Sync users
        client.get(cmd='logon')
        client.post(cmd='online-sync')
        self.assertEqual([[4, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid1 = client.post(['document'], {'context': '', 'message': '1', 'title': '', 'type': 'post'})
        guid2 = client.post(['document'], {'context': '', 'message': '2', 'title': '', 'type': 'post'})

        client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}},
            {'guid': guid2, 'message': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'message']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[6, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[4, None]], json.load(file('slave/push.sequence')))

        guid3 = client.post(['document'], {'context': '', 'message': '3', 'title': '', 'type': 'post'})
        client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}},
            {'guid': guid2, 'message': {'en-us': '2'}},
            {'guid': guid3, 'message': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'message']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[7, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[5, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid2], {'message': '22'})
        client.post(cmd='online-sync')
        self.assertEqual(
                {'guid': guid2, 'message': {'en-us': '22'}},
                self.master_volume['document'].get(guid2).properties(['guid', 'message']))
        self.assertEqual([[8, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[6, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'message': {'en-us': '22'}, 'layer': []},
            {'guid': guid3, 'message': {'en-us': '3'}, 'layer': []},
            ],
            [i.properties(['guid', 'message', 'layer']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[9, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[7, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid1], {'message': 'a'})
        client.put(['document', guid2], {'message': 'b'})
        client.put(['document', guid3], {'message': 'c'})
        guid4 = client.post(['document'], {'context': '', 'message': 'd', 'title': '', 'type': 'post'})
        client.delete(['document', guid2])
        client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': 'a'}, 'layer': ['deleted']},
            {'guid': guid2, 'message': {'en-us': 'b'}, 'layer': ['deleted']},
            {'guid': guid3, 'message': {'en-us': 'c'}, 'layer': []},
            {'guid': guid4, 'message': {'en-us': 'd'}, 'layer': []},
            ],
            [i.properties(['guid', 'message', 'layer']) for i in self.master_volume['document'].find()[0]])
        self.assertEqual([[13, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[12, None]], json.load(file('slave/push.sequence')))

    def test_PushStats(self):
        stats_user.stats_user.value = True
        client = Connection('http://127.0.0.1:9001', auth=http.SugarAuth(keyfile.value))
        client.post(cmd='online-sync')
        self.assertEqual(['ok'], self.stats_commit)
        self.assertEqual([{'stats': 'probe'}], self.stats_merge)

    def test_Pull(self):
        client = Connection('http://127.0.0.1:9000', auth=http.SugarAuth(keyfile.value))
        slave_client = Connection('http://127.0.0.1:9001', auth=http.SugarAuth(keyfile.value))

        # Sync users
        slave_client.get(cmd='logon')
        slave_client.post(cmd='online-sync')
        self.assertEqual([[4, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid1 = client.post(['document'], {'context': '', 'message': '1', 'title': '', 'type': 'post'})
        guid2 = client.post(['document'], {'context': '', 'message': '2', 'title': '', 'type': 'post'})

        slave_client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}},
            {'guid': guid2, 'message': {'en-us': '2'}},
            ],
            [i.properties(['guid', 'message']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[6, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid3 = client.post(['document'], {'context': '', 'message': '3', 'title': '', 'type': 'post'})
        slave_client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}},
            {'guid': guid2, 'message': {'en-us': '2'}},
            {'guid': guid3, 'message': {'en-us': '3'}},
            ],
            [i.properties(['guid', 'message']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[7, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid2], {'message': '22'})
        slave_client.post(cmd='online-sync')
        self.assertEqual(
                {'guid': guid2, 'message': {'en-us': '22'}},
                self.slave_volume['document'].get(guid2).properties(['guid', 'message']))
        self.assertEqual([[8, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.delete(['document', guid1])
        slave_client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': '1'}, 'layer': ['deleted']},
            {'guid': guid2, 'message': {'en-us': '22'}, 'layer': []},
            {'guid': guid3, 'message': {'en-us': '3'}, 'layer': []},
            ],
            [i.properties(['guid', 'message', 'layer']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[9, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        coroutine.sleep(1)
        client.put(['document', guid1], {'message': 'a'})
        client.put(['document', guid2], {'message': 'b'})
        client.put(['document', guid3], {'message': 'c'})
        guid4 = client.post(['document'], {'context': '', 'message': 'd', 'title': '', 'type': 'post'})
        client.delete(['document', guid2])
        slave_client.post(cmd='online-sync')
        self.assertEqual([
            {'guid': guid1, 'message': {'en-us': 'a'}, 'layer': ['deleted']},
            {'guid': guid2, 'message': {'en-us': 'b'}, 'layer': ['deleted']},
            {'guid': guid3, 'message': {'en-us': 'c'}, 'layer': []},
            {'guid': guid4, 'message': {'en-us': 'd'}, 'layer': []},
            ],
            [i.properties(['guid', 'message', 'layer']) for i in self.slave_volume['document'].find()[0]])
        self.assertEqual([[14, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

    def test_PullFiles(self):
        self.touch(('master/files/1', 'a', 1))
        self.touch(('master/files/2/2', 'bb', 2))
        self.touch(('master/files/3/3/3', 'ccc', 3))
        os.utime('master/files', (1, 1))

        client = Connection('http://127.0.0.1:9001', auth=http.SugarAuth(keyfile.value))
        client.post(cmd='online-sync')

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

    def test_PullFromPreviouslyMergedRecord(self):
        master = Connection('http://127.0.0.1:9000', auth=http.SugarAuth(keyfile.value))
        slave = Connection('http://127.0.0.1:9001', auth=http.SugarAuth(keyfile.value))

        # Sync users
        slave.get(cmd='logon')
        slave.post(cmd='online-sync')
        self.assertEqual([[4, None]], json.load(file('slave/pull.sequence')))
        self.assertEqual([[2, None]], json.load(file('slave/push.sequence')))

        guid = slave.post(['document'], {'context': '', 'message': '1', 'title': '1', 'type': 'post'})
        slave.post(cmd='online-sync')

        coroutine.sleep(1)
        master.put(['document', guid], {'message': '1_'})
        slave.put(['document', guid], {'title': '1_'})
        slave.post(cmd='online-sync')

        self.assertEqual(
                {'message': {'en-us': '1_'}, 'title': {'en-us': '1_'}},
                self.master_volume['document'].get(guid).properties(['message', 'title']))
        self.assertEqual(
                {'message': {'en-us': '1_'}, 'title': {'en-us': '1_'}},
                self.slave_volume['document'].get(guid).properties(['message', 'title']))


if __name__ == '__main__':
    tests.main()
