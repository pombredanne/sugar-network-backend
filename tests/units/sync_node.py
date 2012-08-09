#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from os.path import exists, join

import rrdtool

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.sneakernet import InPacket, OutFilePacket
from sugar_network.local import api_url
from sugar_network.node import sync_node
from sugar_network.toolkit import sneakernet
from sugar_network.resources.volume import Volume
from active_toolkit import coroutine


class SyncNodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Export(self):
        node = SyncCommands('node', 'master')

        node.volume['document'].create(guid='1', prop='value1')
        node.volume['document'].create(guid='2', prop='value2')

        node.sync('mnt')
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-2.packet', 'cmd': 'sn_pull', 'src': 'node', 'dst': 'master', 'session': '1', 'sequence': [[1, None]]},
            {'api_url': api_url.value, 'filename': 'node-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '1', 'session': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': 'value1',    'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '2', 'session': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': 'value2',    'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-2.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': '1', 'sequence': [[1, 2]]},
            ],
            self.read_packets('mnt'))

    def test_Export_NoPullForExistingSession(self):
        node = SyncCommands('node', 'master')

        node.volume['document'].create(guid='1')

        node.sync('mnt', session='session')
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '1', 'session': 'session', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': 'session', 'sequence': [[1, 1]]},
            ],
            self.read_packets('mnt'))

    def test_LimittedExport(self):
        node = SyncCommands('node', 'master')

        node.volume['document'].create(guid='1', prop='*' * 1024)
        node.volume['document'].create(guid='2', prop='*' * 1024)
        node.volume['document'].create(guid='3', prop='*' * 1024)
        node.volume['document'].create(guid='4', prop='*' * 1024)
        node.volume['document'].create(guid='5', prop='*' * 1024)
        node.volume['document'].create(guid='6', prop='*' * 1024)

        kwargs = node.sync('mnt', accept_length=1024, session=0)
        self.assertEqual(0, kwargs['session'])
        self.assertEqual([[1, None]], kwargs['diff_sequence'])
        self.assertEqual([], self.read_packets('mnt'))

        kwargs = node.sync('mnt', accept_length=1024, diff_sequence=kwargs['diff_sequence'], session=0)
        self.assertEqual([[1, None]], kwargs['diff_sequence'])
        self.assertEqual([], self.read_packets('mnt'))

        kwargs = node.sync('mnt', accept_length=1024 * 2, diff_sequence=kwargs['diff_sequence'], session=1)
        self.assertEqual([[2, None]], kwargs['diff_sequence'])
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '1', 'session': 1, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': 1, 'sequence': [[1, 1]]},
            ],
            self.read_packets('mnt'))

        kwargs = node.sync('mnt', accept_length=1024 * 3, diff_sequence=kwargs['diff_sequence'], session=2)
        self.assertEqual([[4, None]], kwargs['diff_sequence'])
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '2', 'session': 2, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '3', 'session': 2, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/3/3/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/3/3/layer').st_mtime},
                'guid':  {'value': '3',         'mtime': os.stat('db/document/3/3/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/3/3/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/3/3/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/3/3/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': 2, 'sequence': [[2, 3]]},
            ],
            self.read_packets('mnt'))

        kwargs = node.sync('mnt', diff_sequence=kwargs['diff_sequence'], session=3)
        self.assertEqual(None, kwargs)
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '4', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/4/4/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/4/4/layer').st_mtime},
                'guid':  {'value': '4',         'mtime': os.stat('db/document/4/4/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/4/4/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/4/4/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/4/4/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '5', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/5/5/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/5/5/layer').st_mtime},
                'guid':  {'value': '5',         'mtime': os.stat('db/document/5/5/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/5/5/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/5/5/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/5/5/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '6', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/6/6/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/6/6/layer').st_mtime},
                'guid':  {'value': '6',         'mtime': os.stat('db/document/6/6/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/6/6/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/6/6/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/6/6/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-6.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': 3, 'sequence': [[4, 6]]},
            ],
            self.read_packets('mnt'))

    def test_Import(self):
        node = SyncCommands('node', 'master')

        master_packet = OutFilePacket('mnt', src='master')
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': 'node', 'sequence': [[1, 2]], 'merged': [[3, 4]]},
            {'cmd': 'sn_ack', 'dst': 'other', 'sequence': [[5, 6]], 'merged': [[7, 8]]},
            {'cmd': 'sn_push', 'document': 'document', 'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 2}}},
            {'cmd': 'sn_commit', 'sequence': [[11, 12]]},
            ])
        master_packet.close()

        our_packet = OutFilePacket('mnt', src='node', dst='master', session='stale')
        our_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[9, 10]]},
            ])
        our_packet.close()

        other_node_packet = OutFilePacket('mnt', src='other', dst='master')
        other_node_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 3}}},
            {'cmd': 'sn_commit', 'sequence': [[13, 14]]},
            ])
        other_node_packet.close()

        node.sync('mnt', session='new')

        assert exists(master_packet.path)
        assert exists(other_node_packet.path)
        assert not exists(our_packet.path)

        self.assertEqual(
                [[3, None]],
                json.load(file('sync/push')))
        self.assertEqual(
                [[1, 2], [5, 10], [13, None]],
                json.load(file('sync/pull')))
        self.assertEqual(
                ['2', '3'],
                [i.guid for i in node.volume['document'].find()[0]])

    def test_TakeIntoAccountJustReadAckPacket(self):
        node = SyncCommands('node', 'master')

        node.volume['document'].create(guid='1', prop='prop1')
        node.volume['document'].create(guid='2', prop='prop2')
        node.volume['document'].create(guid='3', prop='prop3')

        master_packet = OutFilePacket('mnt', src='master')
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': 'node', 'sequence': [[1, 2]], 'merged': []},
            ])
        master_packet.close()

        node.sync('mnt', session='session')

        self.assertEqual([
            {'filename': 'master.packet', 'content_type': 'records', 'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'sequence': [[1, 2]], 'merged': []},
            {'api_url': api_url.value, 'filename': 'node-4.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '3', 'session': 'session', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/3/3/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/3/3/layer').st_mtime},
                'guid':  {'value': '3',         'mtime': os.stat('db/document/3/3/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/3/3/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/3/3/mtime').st_mtime},
                'prop':  {'value': 'prop3',     'mtime': os.stat('db/document/3/3/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-4.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': 'session', 'sequence': [[3, 3]]},
            ],
            self.read_packets('mnt'))

    def test_Import_DoNotDeletePacketsFromCurrentSession(self):
        node = SyncCommands('node', 'master')
        node.volume['document'].create(guid='1')

        existing_session = OutFilePacket('mnt', src='node', dst='master', session='the same')
        existing_session.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'range': [1, 1], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            ])
        existing_session.close()

        self.assertEqual(1, len([i for i in sneakernet.walk('mnt')]))
        node.sync('mnt', session='the same')
        self.assertEqual(
                ['the same', 'the same'],
                [i.header['session'] for i in sneakernet.walk('mnt')])
        assert exists(existing_session.path)

        node.sync('mnt', session='new one')
        self.assertEqual(
                ['new one'],
                [i.header['session'] for i in sneakernet.walk('mnt')])

    def test_Import_StatsAcks(self):
        node = SyncCommands('node', 'master')

        master_packet = OutFilePacket('mnt', src='master')
        master_packet.push(data=[
            {'cmd': 'stats_ack', 'dst': 'node', 'sequence': {
                'user1': {
                    'db1': [[1, 2]],
                    'db2': [[3, 4]],
                    },
                'user2': {
                    'db3': [[5, 6]],
                    },
                }},
            ])
        master_packet.close()

        node.sync('mnt', session=0)
        assert exists(master_packet.path)

        self.assertEqual([[3, None]], json.load(file('stats/us/user1/db1.push')))
        self.assertEqual([[1, 2], [5, None]], json.load(file('stats/us/user1/db2.push')))
        self.assertEqual([[1, 4], [7, None]], json.load(file('stats/us/user2/db3.push')))

    def test_sync_session(self):
        node = SyncCommands('node', 'master')
        node.volume['document'].create(guid='1', prop='*' * 1024)
        coroutine.dispatch()

        self.override(os, 'statvfs', lambda x: Statvfs(1024 - 512))

        node.events = []
        node.sync_session('mnt')
        self.assertEqual([
            {'event': 'sync_start', 'path': 'mnt'},
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
            {'event': 'sync_continue'},
            ],
            node.events)
        records = self.read_packets('mnt')
        self.assertEqual(1, len(records))
        self.assertEqual('sn_pull', records[0]['cmd'])
        session = records[0]['session']

        node.events = []
        node.sync_session('mnt')
        self.assertEqual([
            {'path': 'mnt', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
            {'event': 'sync_continue'},
            ],
            node.events)
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'cmd': 'sn_pull', 'src': 'node', 'dst': 'master', 'session': session, 'sequence': [[1, None]]},
            ],
            self.read_packets('mnt'))

        self.override(os, 'statvfs', lambda x: Statvfs(1024 + 512))

        node.events = []
        node.sync_session('mnt')
        self.assertEqual([
            {'path': 'mnt', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
            {'event': 'sync_complete'},
            ],
            node.events)
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'cmd': 'sn_pull', 'src': 'node', 'dst': 'master', 'session': session, 'sequence': [[1, None]]},
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'node', 'dst': 'master', 'document': 'document', 'guid': '1', 'session': session, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'cmd': 'sn_commit', 'src': 'node', 'dst': 'master', 'session': session, 'sequence': [[1, 1]]},
            ],
            self.read_packets('mnt'))

    def test_ExportStats(self):
        node = SyncCommands('node', 'master')

        ts = int(time.time())
        os.makedirs('stats/1/1')
        rrdtool.create('stats/1/1/db1.rrd', '--start', str(ts), '-s', '1', 'DS:f:GAUGE:1:U:U', 'RRA:AVERAGE:0.5:1:100')
        rrdtool.update('stats/1/1/db1.rrd', '%s:1' % (ts + 1), '%s:2' % (ts + 2))
        rrdtool.create('stats/1/1/db2.rrd', '--start', str(ts + 2), '-s', '1', 'DS:f:GAUGE:1:U:U', 'RRA:AVERAGE:0.5:1:100')
        rrdtool.update('stats/1/1/db2.rrd', '%s:3' % (ts + 3), '%s:4' % (ts + 4))
        os.makedirs('stats/2/2')
        rrdtool.create('stats/2/2/db3.rrd', '--start', str(ts + 4), '-s', '1', 'DS:f:GAUGE:1:U:U', 'RRA:AVERAGE:0.5:1:100')
        rrdtool.update('stats/2/2/db3.rrd', '%s:5' % (ts + 5))

        node.sync('mnt', session=0)

        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-0.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 0,
                'cmd': 'stats_push', 'user': '1', 'db': 'db1', 'sequence': [[1, ts + 2]], 'timestamp': ts + 1, 'values': {'f': 1},
                },
            {'api_url': api_url.value, 'filename': 'node-0.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 0,
                'cmd': 'stats_push', 'user': '1', 'db': 'db1', 'sequence': [[1, ts + 2]], 'timestamp': ts + 2, 'values': {'f': 2},
                },
            {'api_url': api_url.value, 'filename': 'node-0.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 0,
                'cmd': 'stats_push', 'user': '1', 'db': 'db2', 'sequence': [[1, ts + 4]], 'timestamp': ts + 3, 'values': {'f': 3},
                },
            {'api_url': api_url.value, 'filename': 'node-0.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 0,
                'cmd': 'stats_push', 'user': '1', 'db': 'db2', 'sequence': [[1, ts + 4]], 'timestamp': ts + 4, 'values': {'f': 4},
                },
            {'api_url': api_url.value, 'filename': 'node-0.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 0,
                'cmd': 'stats_push', 'user': '2', 'db': 'db3', 'sequence': [[1, ts + 5]], 'timestamp': ts + 5, 'values': {'f': 5},
                },
            ],
            self.read_packets('mnt'))

    def test_LimittedExportStats(self):
        node = SyncCommands('node', 'master')

        ts = int(time.time())
        os.makedirs('stats/us/user')
        rrdtool.create('stats/us/user/db.rrd', '--start', str(ts), '-s', '1', 'DS:f:GAUGE:1:U:U', 'RRA:AVERAGE:0.5:1:100')
        rrdtool.update('stats/us/user/db.rrd', '%s:1' % (ts + 1), '%s:2' % (ts + 2))

        node.volume['document'].create(guid='1', prop='*' * 1024)

        kwargs = node.sync('mnt1', accept_length=1024 + 512, session=0)
        self.assertEqual({'user': {'db': [[1, None]]}}, kwargs['stats_sequence'])
        self.assertEqual([], self.read_packets('mnt1')[3:])

        kwargs = node.sync('mnt2', stats_sequence={'user': {'db': [[ts + 2, None]]}}, session=1)
        self.assertEqual(None, kwargs)
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'content_type': 'records', 'src': 'node', 'dst': 'master', 'session': 1,
                'cmd': 'stats_push', 'user': 'user', 'db': 'db', 'sequence': [[ts + 2, ts + 2]], 'timestamp': ts + 2, 'values': {'f': 2},
                },
            ],
            self.read_packets('mnt2')[2:])

    def read_packets(self, path):
        result = []
        for filename in sorted(os.listdir(path)):
            with InPacket(join(path, filename)) as packet:
                result.extend([i for i in packet])
        return result


class Statvfs(object):

    f_bfree = 0
    f_frsize = 1

    def __init__(self, f_bfree):
        self.f_bfree = f_bfree


class SyncCommands(sync_node.SyncCommands):

    def __init__(self, node, master):
        sync_node.SyncCommands.__init__(self, 'sync')
        self.node_guid = node
        self.master_guid = master
        self.volume = new_volume('db')
        self.node_mount = self
        self.events = []

    def publish(self, event):
        self.events.append(event)


def new_volume(root):

    class Document(ad.Document):

        @ad.active_property(slot=1, default='')
        def prop(self, value):
            return value

    return Volume(root, [Document])


if __name__ == '__main__':
    tests.main()
