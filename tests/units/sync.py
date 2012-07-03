#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import hashlib
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.sneakernet import InPacket, OutPacket
from sugar_network.node.sync import SyncCommands
from sugar_network.local.sync import NodeMount, _DEFAULT_MASTER
from sugar_network.toolkit import sneakernet
from sugar_network.toolkit.collection import Sequences
from sugar_network.resources.volume import Volume
from active_toolkit import coroutine


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)
        self.override(time, 'time', lambda: -1)

    def new_volume(self, root):

        class Document(ad.Document):

            @ad.active_property(slot=1, default='')
            def prop(self, value):
                return value

        return Volume(root, [Document])

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Master_MisaddressedPacket(self):
        master = SyncCommands('master')
        master.volume = self.new_volume('db')
        response = ad.Response()

        packet = OutPacket('push')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node', dst='fake')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='master', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node', dst='master', sequence={})
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('pull', src='node', dst='master', sequence={})
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('fake', src='node', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

    def test_Master_PushPacket(self):
        master = SyncCommands('master')
        master.volume = self.new_volume('db')
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('push',
                src='node',
                dst='master',
                sequence='sequence')
        packet.push_messages(document='document', items=[
            {'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 1}}},
            {'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            ])
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual(
                ['1', '2', '3'],
                [i.guid for i in master.volume['document'].find()[0]])

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header['dst'])
        self.assertEqual('sequence', packet.header['push_sequence'])
        self.assertEqual({'document': [[1, 1]]}, packet.header['pull_sequence'])

    def test_Master_PullPacket(self):
        master = SyncCommands('master')
        master.volume = self.new_volume('db')
        request = ad.Request()
        response = ad.Response()

        master.volume['document'].create_with_guid('1', {})
        master.volume['document'].create_with_guid('2', {})

        packet = OutPacket('pull',
                src='node',
                dst='master',
                sequence={'document': [[1, None]]})
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 2]]}},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Master_AvoidEmptyPacketsOnPull(self):
        master = SyncCommands('master')
        master.volume = self.new_volume('db')
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('pull',
                src='node',
                dst='master',
                sequence={'document': [[1, None]]})
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual(None, reply)

    def test_Master_LimittedPull(self):
        master = SyncCommands('master')
        master.volume = self.new_volume('db')
        response = ad.Response()

        master.volume['document'].create_with_guid('1', {'prop': '*' * 1024})
        master.volume['document'].create_with_guid('2', {'prop': '*' * 1024})

        def rewind():
            request = ad.Request()
            packet = OutPacket('pull',
                    src='node',
                    dst='master',
                    sequence={'document': [[1, None]]})
            request.content_stream, request.content_length = packet.pop_content()
            return request

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 2)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packet(InPacket(stream=reply)))

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 3)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 2]]}},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Node_Export(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create_with_guid('1', {'prop': 'value1'})
        node.volume['document'].create_with_guid('2', {'prop': 'value2'})

        node.sync('sync')
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': '1', 'sequence': {'document': [[1, None]]}},
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': '1', 'sequence': {'document': [[1, 2]]}},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': 'value1',    'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': 'value2',    'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

        assert exists('db/push.sequence')
        self.assertEqual({'document': [[1, None]]}, json.load(file('db/push.sequence')))

        assert exists('db/pull.sequence')
        self.assertEqual({'document': [[1, None]]}, json.load(file('db/pull.sequence')))

    def test_Node_Export_NoPullForExistingSession(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create_with_guid('1', {})

        node.sync('sync', session='session')
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': 'session'},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_Import(self):
        node = NodeMount(self.new_volume('db'), None)

        ack = OutPacket('ack', root='sync',
                src=_DEFAULT_MASTER,
                dst=tests.UID,
                push_sequence={'document': [[1, 2]]},
                pull_sequence={'document': [[3, 4]]})
        ack.close()

        other_node_ack = OutPacket('ack', root='sync',
                src=_DEFAULT_MASTER,
                dst='other',
                push_sequence={'document': [[5, 6]]},
                pull_sequence={'document': [[7, 8]]})
        other_node_ack.close()

        our_push = OutPacket('push', root='sync',
                src=tests.UID,
                dst=_DEFAULT_MASTER,
                sequence={'document': [[9, 10]]},
                session='stale')
        our_push.push_messages(document='document', items=[
            {'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            ])
        our_push.close()

        master_push = OutPacket('push', root='sync',
                src=_DEFAULT_MASTER,
                sequence={'document': [[11, 12]]})
        master_push.push_messages(document='document', items=[
            {'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 2}}},
            ])
        master_push.close()

        other_node_push = OutPacket('push', root='sync',
                src='other',
                dst=_DEFAULT_MASTER,
                sequence={'document': [[13, 14]]})
        other_node_push.push_messages(document='document', items=[
            {'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 3}}},
            ])
        other_node_push.close()

        node.sync('sync', session='new')

        assert not exists(ack.path)
        assert exists(other_node_ack.path)
        assert not exists(our_push.path)
        assert exists(master_push.path)
        assert exists(other_node_push.path)

        self.assertEqual(
                {'document': [[3, None]]},
                json.load(file('db/push.sequence')))
        self.assertEqual(
                {'document': [[1, 2], [5, 10], [13, None]]},
                json.load(file('db/pull.sequence')))
        self.assertEqual(
                ['2', '3'],
                [i.guid for i in node.volume['document'].find()[0]])

    def test_Node_Import_DoNotDeletePacketsFromCurrentSession(self):
        node = NodeMount(self.new_volume('db'), None)
        node.volume['document'].create_with_guid('1', {})

        existing_push = OutPacket('push', root='sync',
                src=tests.UID,
                dst=_DEFAULT_MASTER,
                sequence={},
                session='the same')
        existing_push.close()

        self.assertEqual(1, len([i for i in sneakernet.walk('sync')]))
        node.sync('sync', session='the same')
        files = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(2, len(files))
        assert exists(existing_push.path)

        node.sync('sync', session='new one')
        new_fiels = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(1, len(new_fiels))
        assert not (set(new_fiels) & set(files))

    def test_Node_LimittedExport(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create_with_guid('1', {'prop': '*' * 1024})
        node.volume['document'].create_with_guid('2', {'prop': '*' * 1024})
        node.volume['document'].create_with_guid('3', {'prop': '*' * 1024})
        node.volume['document'].create_with_guid('4', {'prop': '*' * 1024})
        node.volume['document'].create_with_guid('5', {'prop': '*' * 1024})
        node.volume['document'].create_with_guid('6', {'prop': '*' * 1024})

        kwargs = node.sync('sync', accept_length=1024, session=0)
        self.assertEqual(0, kwargs['session'])
        self.assertEqual({'document': [[1, None]]}, kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024, push_sequence=kwargs['push_sequence'], session=0)
        self.assertEqual({'document': [[1, None]]}, kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024 * 2, push_sequence=kwargs['push_sequence'], session=1)
        self.assertEqual({'document': [[2, None]]}, kwargs['push_sequence'])
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': 1},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024 * 3, push_sequence=kwargs['push_sequence'], session=2)
        self.assertEqual({'document': [[4, None]]}, kwargs['push_sequence'])
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[2, 3]]}, 'session': 2},
            {'type': 'messages', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '3', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/3/3/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/3/3/layer').st_mtime},
                'guid':  {'value': '3',         'mtime': os.stat('db/document/3/3/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/3/3/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/3/3/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/3/3/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', push_sequence=kwargs['push_sequence'], session=3)
        self.assertEqual(None, kwargs)
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[4, 6]]}, 'session': 3},
            {'type': 'messages', 'document': 'document', 'guid': '4', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/4/4/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/4/4/layer').st_mtime},
                'guid':  {'value': '4',         'mtime': os.stat('db/document/4/4/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/4/4/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/4/4/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/4/4/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '5', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/5/5/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/5/5/layer').st_mtime},
                'guid':  {'value': '5',         'mtime': os.stat('db/document/5/5/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/5/5/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/5/5/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/5/5/prop').st_mtime},
                }},
            {'type': 'messages', 'document': 'document', 'guid': '6', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/6/6/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/6/6/layer').st_mtime},
                'guid':  {'value': '6',         'mtime': os.stat('db/document/6/6/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/6/6/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/6/6/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/6/6/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_sync_session(self):
        node = NodeMount(self.new_volume('db'), None)
        node.volume['document'].create_with_guid('1', {'prop': '*' * 1024})
        coroutine.dispatch()

        node.publisher = lambda x: events.append(x)
        self.override(os, 'statvfs', lambda x: Statvfs(1024 - 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'event': 'sync_start', 'path': 'sync'},
            {'event': 'sync_progress', 'progress': "Generating 'push/3.push.packet' PUSH packet"},
            {'event': 'sync_continue'},
            ],
            events)
        records = self.read_packets('sync')
        self.assertEqual(1, len(records))
        self.assertEqual('pull', records[0]['type'])
        session = records[0]['session']

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating 'push/4.push.packet' PUSH packet"},
            {'event': 'sync_continue'},
            ],
            events)
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': {'document': [[1, None]]}},
            ],
            self.read_packets('sync'))

        self.override(os, 'statvfs', lambda x: Statvfs(1024 + 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating 'push/5.push.packet' PUSH packet"},
            {'event': 'sync_complete'},
            ],
            events)
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': {'document': [[1, None]]}},
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': session},
            {'type': 'messages', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': -1,          'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': -1,          'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def read_packets(self, path):
        result = []
        for dirname in ('pull', 'push', 'ack'):
            if not exists(join(path, dirname)):
                continue
            for filename in sorted(os.listdir(join(path, dirname))):
                with InPacket(join(path, dirname, filename)) as packet:
                    result.extend(self.read_packet(packet))
        return result

    def read_packet(self, packet):
        result = [packet.header]
        result.extend([i for i in packet])
        return result


class Statvfs(object):

    f_bfree = 0
    f_frsize = 1

    def __init__(self, f_bfree):
        self.f_bfree = f_bfree


if __name__ == '__main__':
    tests.main()
