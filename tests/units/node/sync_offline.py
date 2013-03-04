#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import uuid
from os.path import exists, join

import rrdtool

from __init__ import tests

from sugar_network import db, node
from sugar_network.toolkit.rrd import Rrd
from sugar_network.client import api_url
from sugar_network.node import sync, stats_user, files_root
from sugar_network.node.slave import SlaveCommands
from sugar_network.resources.volume import Volume
from sugar_network.toolkit import coroutine


class statvfs(object):

    f_bfree = None
    f_frsize = 1


class SyncOfflineTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(db, 'uuid', self.next_uuid)
        self.override(os, 'statvfs', lambda *args: statvfs())
        statvfs.f_bfree = 999999999
        stats_user.stats_user_step.value = 1
        stats_user.stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']
        node.sync_layers.value = 'pilot'

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_FailOnFullDump(self):

        class Document(db.Document):
            pass

        volume = Volume('node', [Document])
        cp = SlaveCommands('node', volume)

        node.sync_layers.value = None
        self.assertRaises(RuntimeError, cp.offline_sync, tests.tmpdir + '/mnt')
        node.sync_layers.value = 'public'
        self.assertRaises(RuntimeError, cp.offline_sync, tests.tmpdir + '/mnt')
        node.sync_layers.value = ['public']
        self.assertRaises(RuntimeError, cp.offline_sync, tests.tmpdir + '/mnt')
        node.sync_layers.value = ['public', 'foo']
        self.assertRaises(RuntimeError, cp.offline_sync, tests.tmpdir + '/mnt')

    def test_Export(self):

        class Document(db.Document):
            pass

        volume = Volume('node', [Document])
        cp = SlaveCommands('node', volume)
        stats_user.stats_user.value = True

        volume['document'].create(guid='1', prop='value1', ctime=1, mtime=1)
        volume['document'].create(guid='2', prop='value2', ctime=2, mtime=2)
        self.utime('node', 0)

        ts = int(time.time())
        rrd = Rrd('stats/user/dir/user', stats_user.stats_user_step.value, stats_user.stats_user_rras.value)
        rrd['db'].put({'field': 1}, ts)

        cp.offline_sync(tests.tmpdir + '/mnt')
        assert cp._offline_session is None

        self.assertEqual([
            ({'packet': 'diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'filename': '2.sneakernet'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    }},
                {'guid': '2', 'diff': {
                    'guid': {'value': '2', 'mtime': 0},
                    'ctime': {'value': 2, 'mtime': 0},
                    'mtime': {'value': 2, 'mtime': 0},
                    }},
                {'commit': [[1, 2]]},
                ]),
            ({'packet': 'stats_diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'filename': '2.sneakernet'}, [
                {'db': 'db', 'user': 'user'},
                {'timestamp': ts, 'values': {'field': 1.0}},
                {'commit': {'user': {'db': [[1, ts]]}}},
                ]),
            ({'packet': 'files_pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'sequence': [[1, None]], 'filename': '2.sneakernet'}, []),
            ({'packet': 'pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'sequence': [[1, None]], 'filename': '2.sneakernet', 'layer':  ['pilot']}, []),
            ],
            sorted([(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('mnt')]))
        assert not exists('node/pull.sequence')
        assert not exists('node/push.sequence')

    def test_ContinuesExport(self):
        payload = ''.join([str(uuid.uuid4()) for i in xrange(5000)])

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('node', [Document])
        cp = SlaveCommands('node', volume)
        stats_user.stats_user.value = True

        volume['document'].create(guid='1', prop=payload, ctime=1, mtime=1)
        volume['document'].create(guid='2', prop=payload, ctime=2, mtime=2)
        self.utime('node', 0)

        ts = int(time.time())
        rrd = Rrd('stats/user/dir/user', stats_user.stats_user_step.value, stats_user.stats_user_rras.value)
        rrd['db'].put({'field': 1}, ts)

        statvfs.f_bfree = len(payload) * 1.5 + sync._SNEAKERNET_RESERVED_SIZE
        cp.offline_sync(tests.tmpdir + '/1')
        assert cp._offline_session is not None

        self.assertEqual([
            ({'packet': 'diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'filename': '2.sneakernet'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    'prop': {'value': payload, 'mtime': 0},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ({'packet': 'files_pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'sequence': [[1, None]], 'filename': '2.sneakernet'}, []),
            ({'packet': 'pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'sequence': [[1, None]], 'filename': '2.sneakernet', 'layer': ['pilot']}, []),
            ],
            sorted([(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('1')]))

        statvfs.f_bfree = 999999999
        cp.offline_sync(tests.tmpdir + '/2')
        assert cp._offline_session is None

        self.assertEqual([
            ({'packet': 'diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'filename': '3.sneakernet'}, [
                {'document': 'document'},
                {'guid': '2', 'diff': {
                    'guid': {'value': '2', 'mtime': 0},
                    'ctime': {'value': 2, 'mtime': 0},
                    'mtime': {'value': 2, 'mtime': 0},
                    'prop': {'value': payload, 'mtime': 0},
                    }},
                {'commit': [[2, 2]]},
                ]),
            ({'packet': 'stats_diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '1', 'filename': '3.sneakernet'}, [
                {'db': 'db', 'user': 'user'},
                {'timestamp': ts, 'values': {'field': 1.0}},
                {'commit': {'user': {'db': [[1, ts]]}}},
                ]),
            ],
            sorted([(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('2')]))

        statvfs.f_bfree = 999999999
        cp.offline_sync(tests.tmpdir + '/3')
        assert cp._offline_session is None

        self.assertEqual([
            ({'packet': 'diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '4', 'filename': '5.sneakernet'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    'prop': {'value': payload, 'mtime': 0},
                    }},
                {'guid': '2', 'diff': {
                    'guid': {'value': '2', 'mtime': 0},
                    'ctime': {'value': 2, 'mtime': 0},
                    'mtime': {'value': 2, 'mtime': 0},
                    'prop': {'value': payload, 'mtime': 0},
                    }},
                {'commit': [[1, 2]]},
                ]),
            ({'packet': 'stats_diff', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '4', 'filename': '5.sneakernet'}, [
                {'db': 'db', 'user': 'user'},
                {'timestamp': ts, 'values': {'field': 1.0}},
                {'commit': {'user': {'db': [[1, ts]]}}},
                ]),
            ({'packet': 'files_pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '4', 'sequence': [[1, None]], 'filename': '5.sneakernet'}, []),
            ({'packet': 'pull', 'src': 'node', 'dst': 'localhost:8888', 'api_url': 'http://localhost:8888', 'session': '4', 'sequence': [[1, None]], 'filename': '5.sneakernet', 'layer': ['pilot']}, []),
            ],
            sorted([(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('3')]))

    def test_Import(self):
        ts = int(time.time())
        self.touch(('blob-1', 'a'))
        self.touch(('blob-2', 'bb'))
        sync.sneakernet_encode([
            ('diff', {'src': 'localhost:8888'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    }},
                {'guid': '2', 'diff': {
                    'guid': {'value': '2', 'mtime': 0},
                    'ctime': {'value': 2, 'mtime': 0},
                    'mtime': {'value': 2, 'mtime': 0},
                    }},
                {'commit': [[1, 2]]},
                ]),
            ('files_diff', {'src': 'localhost:8888'}, [
                {'op': 'update', 'blob': 'blob-1', 'path': '1'},
                {'op': 'update', 'blob': 'blob-2', 'path': '2'},
                {'op': 'commit', 'sequence': [[1, 2]]},
                ]),
            ('ack', {'ack': [[101, 103]], 'sequence': [[1, 3]], 'src': 'localhost:8888', 'dst': 'node'}, []),
            ('stats_ack', {'sequence': {'user': {'db': [[1, ts]]}}, 'src': 'localhost:8888', 'dst': 'node'}, []),
            ],
            root='mnt')

        class Document(db.Document):
            pass

        volume = Volume('node', [Document])
        cp = SlaveCommands('node', volume)
        stats_user.stats_user.value = True
        files_root.value = 'files'

        cp.offline_sync(tests.tmpdir + '/mnt')
        assert cp._offline_session is None

        self.assertEqual(
                ['1', '2'],
                [i.guid for i in volume['document'].find()[0]])
        self.assertEqual('a', file('files/1').read())
        self.assertEqual('bb', file('files/2').read())
        self.assertEqual([[4, None]], json.load(file('node/push.sequence')))
        self.assertEqual([[3, 100], [104, None]], json.load(file('node/pull.sequence')))
        self.assertEqual([[3, None]], json.load(file('node/files.sequence')))


if __name__ == '__main__':
    tests.main()
