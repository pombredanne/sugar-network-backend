#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import shutil
import hashlib
from cStringIO import StringIO
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.sneakernet import InPacket, OutPacket, OutBufferPacket, OutFilePacket
from sugar_network.node.commands import MasterCommands
from sugar_network.local.mounts import NodeMount, _DEFAULT_MASTER
from sugar_network.toolkit import sneakernet
from sugar_network.resources.volume import Volume
from active_toolkit import coroutine, sockets


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)

    def new_volume(self, root):

        class Document(ad.Document):

            @ad.active_property(slot=1, default='')
            def prop(self, value):
                return value

        return Volume(root, [Document])

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Master_push_MisaddressedPackets(self):
        master = MasterCommands('master', self.new_volume('db'))
        response = ad.Response()

        packet = OutBufferPacket()
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node', dst='fake')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='master', dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node', dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.push(request, response)

    def test_Master_push_ProcessPushes(self):
        master = MasterCommands('master', self.new_volume('db'))
        request = ad.Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'range': [1, 1], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_push', 'range': [2, 2], 'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 1}}},
            {'cmd': 'sn_push', 'range': [3, 3], 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)

        self.assertEqual(
                ['1', '2', '3'],
                [i.guid for i in master.volume['document'].find()[0]])

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'in_sequence': [[1, 3]], 'out_sequence': [[1, 3]]},
            ],
            [i for i in packet])

    def test_Master_push_ProcessPulls(self):
        master = MasterCommands('master', self.new_volume('db'))
        request = ad.Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[1, 1]]},
            {'cmd': 'sn_pull', 'sequence': [[3, 4]]},
            {'cmd': 'sn_pull', 'sequence': [[7, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)

        packet = InPacket(stream=reply)
        self.assertEqual('node', packet.header['src'])
        self.assertEqual('master', packet.header.get('dst'))
        self.assertEqual([
            {'src': 'node', 'dst': 'master', 'cmd': 'sn_pull', 'sequence': [[1, 1], [3, 4], [7, None]]},
            ],
            [i for i in packet])

    def test_Master_push_ProcessPushesAndPulls(self):
        master = MasterCommands('master', self.new_volume('db'))
        request = ad.Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'range': [1, 1], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_push', 'range': [3, 3], 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)
        assert response.content_type.startswith('multipart/mixed; boundary=')

        self.assertEqual(
                sorted(['2.packet', 'continue']),
                sorted(decode_multipart(reply, response)))

        packet = InPacket('2.packet')
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'in_sequence': [[1, 1], [3, 3]], 'out_sequence': [[1, 2]]},
            ],
            [i for i in packet])

        packet = InPacket('continue')
        self.assertEqual('node', packet.header['src'])
        self.assertEqual('master', packet.header.get('dst'))
        self.assertEqual([
            {'src': 'node', 'dst': 'master', 'cmd': 'sn_pull', 'sequence': [[3, None]]},
            ],
            [i for i in packet])

    def test_Master_pull_MisaddressedPackets(self):
        master = MasterCommands('master', self.new_volume('db'))
        response = ad.Response()

        packet = OutBufferPacket()
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='node')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.pull(request, response)

        packet = OutBufferPacket(src='node', dst='fake')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='master', dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='node', dst='master')
        request = ad.Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.pull(request, response)

    def test_Master_pull_ProcessPulls(self):
        master = MasterCommands('master', self.new_volume('db'))
        request = ad.Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(data=[
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.pull(request, response)

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'range': [1, 1], 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'range': [2, 2], 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            [i for i in packet])

    def test_Master_pull_AvoidEmptyPacketsOnPull(self):
        master = MasterCommands('master', self.new_volume('db'))
        request = ad.Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(data=[
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.pull(request, response)
        self.assertEqual(None, reply)

    def test_Master_pull_LimittedPull(self):
        master = MasterCommands('master', self.new_volume('db'))
        response = ad.Response()

        master.volume['document'].create(guid='1', prop='*' * 1024)
        master.volume['document'].create(guid='2', prop='*' * 1024)

        def rewind():
            request = ad.Request()
            packet = OutBufferPacket(src='node', dst='master')
            packet.push(data=[
                {'cmd': 'sn_pull', 'sequence': [[1, None]]},
                ])
            request.content_stream = packet.pop()
            request.content_length = len(request.content_stream.getvalue())
            return request

        request = rewind()
        reply = master.pull(request, response, accept_length=1024 * 2)
        assert response.content_type.startswith('multipart/mixed; boundary=')
        self.assertEqual(
                sorted(['2.packet', 'continue']),
                sorted(decode_multipart(reply, response)))

        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'range': [1, 1], 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            [i for i in InPacket('2.packet')])

        packet = InPacket('continue')
        self.assertEqual(None, packet.header.get('src'))
        self.assertEqual('master', packet.header.get('dst'))
        self.assertEqual([
            {'dst': 'master', 'cmd': 'sn_pull', 'sequence': [[2, None]]},
            ],
            [i for i in packet])

        request = rewind()
        reply = master.pull(request, response, accept_length=1024 * 3)

        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'range': [1, 1], 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'range': [2, 2], 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            ],
            [i for i in InPacket(stream=reply)])

    def test_Node_Export(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create(guid='1', prop='value1')
        node.volume['document'].create(guid='2', prop='value2')

        node.sync('sync')
        self.assertEqual([
            {'cmd': 'sn_pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': '1', 'sequence': [[1, None]]},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [1, 1], 'document': 'document', 'guid': '1', 'session': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': 'value1',    'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [2, 2], 'document': 'document', 'guid': '2', 'session': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': 'value2',    'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_Export_NoPullForExistingSession(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create(guid='1')

        node.sync('sync', session='session')
        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [1, 1], 'document': 'document', 'guid': '1', 'session': 'session', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_LimittedExport(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create(guid='1', prop='*' * 1024)
        node.volume['document'].create(guid='2', prop='*' * 1024)
        node.volume['document'].create(guid='3', prop='*' * 1024)
        node.volume['document'].create(guid='4', prop='*' * 1024)
        node.volume['document'].create(guid='5', prop='*' * 1024)
        node.volume['document'].create(guid='6', prop='*' * 1024)

        kwargs = node.sync('sync', accept_length=1024, session=0)
        self.assertEqual(0, kwargs['session'])
        self.assertEqual([[1, None]], kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024, push_sequence=kwargs['push_sequence'], session=0)
        self.assertEqual([[1, None]], kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024 * 2, push_sequence=kwargs['push_sequence'], session=1)
        self.assertEqual([[2, None]], kwargs['push_sequence'])
        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [1, 1], 'document': 'document', 'guid': '1', 'session': 1, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024 * 3, push_sequence=kwargs['push_sequence'], session=2)
        self.assertEqual([[4, None]], kwargs['push_sequence'])
        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [2, 2], 'document': 'document', 'guid': '2', 'session': 2, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [3, 3], 'document': 'document', 'guid': '3', 'session': 2, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/3/3/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/3/3/layer').st_mtime},
                'guid':  {'value': '3',         'mtime': os.stat('db/document/3/3/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/3/3/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/3/3/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/3/3/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', push_sequence=kwargs['push_sequence'], session=3)
        self.assertEqual(None, kwargs)
        self.assertEqual([
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [4, 4], 'document': 'document', 'guid': '4', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/4/4/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/4/4/layer').st_mtime},
                'guid':  {'value': '4',         'mtime': os.stat('db/document/4/4/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/4/4/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/4/4/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/4/4/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [5, 5], 'document': 'document', 'guid': '5', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/5/5/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/5/5/layer').st_mtime},
                'guid':  {'value': '5',         'mtime': os.stat('db/document/5/5/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/5/5/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/5/5/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/5/5/prop').st_mtime},
                }},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [6, 6], 'document': 'document', 'guid': '6', 'session': 3, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/6/6/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/6/6/layer').st_mtime},
                'guid':  {'value': '6',         'mtime': os.stat('db/document/6/6/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/6/6/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/6/6/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/6/6/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_Import(self):
        node = NodeMount(self.new_volume('db'), None)

        master_packet = OutFilePacket('sync', src=_DEFAULT_MASTER)
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': tests.UID, 'in_sequence': [[1, 2]], 'out_sequence': [[3, 4]]},
            {'cmd': 'sn_ack', 'dst': 'other', 'in_sequence': [[5, 6]], 'out_sequence': [[7, 8]]},
            {'cmd': 'sn_push', 'document': 'document', 'range': [11, 12], 'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 2}}},
            ])
        master_packet.close()

        our_packet = OutFilePacket('sync', src=tests.UID, dst=_DEFAULT_MASTER, session='stale')
        our_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'range': [9, 10], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            ])
        our_packet.close()

        other_node_packet = OutFilePacket('sync', src='other', dst=_DEFAULT_MASTER)
        other_node_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'range': [13, 14], 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 3}}},
            ])
        other_node_packet.close()

        node.sync('sync', session='new')

        assert exists(master_packet.path)
        assert exists(other_node_packet.path)
        assert not exists(our_packet.path)

        self.assertEqual(
                [[3, None]],
                json.load(file('db/push.sequence')))
        self.assertEqual(
                [[1, 2], [5, 10], [13, None]],
                json.load(file('db/pull.sequence')))
        self.assertEqual(
                ['2', '3'],
                [i.guid for i in node.volume['document'].find()[0]])

    def test_Node_TakeIntoAccountJustReadAckPacket(self):
        node = NodeMount(self.new_volume('db'), None)

        node.volume['document'].create(guid='1', prop='prop1')
        node.volume['document'].create(guid='2', prop='prop2')
        node.volume['document'].create(guid='3', prop='prop3')

        master_packet = OutFilePacket('sync', src=_DEFAULT_MASTER)
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': tests.UID, 'in_sequence': [[1, 2]], 'out_sequence': []},
            ])
        master_packet.close()

        node.sync('sync', session='session')

        self.assertEqual([
            {'content_type': 'records', 'src': _DEFAULT_MASTER, 'dst': tests.UID, 'cmd': 'sn_ack', 'in_sequence': [[1, 2]], 'out_sequence': []},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [3, 3], 'document': 'document', 'guid': '3', 'session': 'session', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/3/3/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/3/3/layer').st_mtime},
                'guid':  {'value': '3',         'mtime': os.stat('db/document/3/3/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/3/3/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/3/3/mtime').st_mtime},
                'prop':  {'value': 'prop3',     'mtime': os.stat('db/document/3/3/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

    def test_Node_Import_DoNotDeletePacketsFromCurrentSession(self):
        node = NodeMount(self.new_volume('db'), None)
        node.volume['document'].create(guid='1')

        existing_session = OutFilePacket('sync', src=tests.UID, dst=_DEFAULT_MASTER, session='the same')
        existing_session.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'range': [1, 1], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            ])
        existing_session.close()

        self.assertEqual(1, len([i for i in sneakernet.walk('sync')]))
        node.sync('sync', session='the same')
        files = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(2, len(files))
        assert exists(existing_session.path)

        node.sync('sync', session='new one')
        new_fiels = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(1, len(new_fiels))
        assert not (set(new_fiels) & set(files))

    def test_Node_sync_session(self):
        node = NodeMount(self.new_volume('db'), None)
        node.volume['document'].create(guid='1', prop='*' * 1024)
        coroutine.dispatch()

        node.publisher = lambda x: events.append(x)
        self.override(os, 'statvfs', lambda x: Statvfs(1024 - 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'event': 'sync_start', 'path': 'sync'},
            {'event': 'sync_progress', 'progress': "Generating '3.packet' packet"},
            {'event': 'sync_continue'},
            ],
            events)
        records = self.read_packets('sync')
        self.assertEqual(1, len(records))
        self.assertEqual('sn_pull', records[0]['cmd'])
        session = records[0]['session']

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating '4.packet' packet"},
            {'event': 'sync_continue'},
            ],
            events)
        self.assertEqual([
            {'cmd': 'sn_pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': [[1, None]]},
            ],
            self.read_packets('sync'))

        self.override(os, 'statvfs', lambda x: Statvfs(1024 + 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating '5.packet' packet"},
            {'event': 'sync_complete'},
            ],
            events)
        self.assertEqual([
            {'cmd': 'sn_pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': [[1, None]]},
            {'content_type': 'records', 'cmd': 'sn_push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'range': [1, 1], 'document': 'document', 'guid': '1', 'session': session, 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            ],
            self.read_packets('sync'))

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


def decode_multipart(reply, response):
    stream = StringIO()
    for chunk in reply:
        stream.write(chunk)
    stream.seek(0)
    filenames = []
    for filename, stream in sockets.decode_multipart(stream,
            response.content_length, response.content_type.split('"')[1]):
        shutil.move(stream.name, filename)
        filenames.append(filename)
    return filenames


if __name__ == '__main__':
    tests.main()
