#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import shutil
import base64
import hashlib
from cStringIO import StringIO
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.sneakernet import InPacket, OutPacket, OutBufferPacket, OutFilePacket
from sugar_network.node import commands
from sugar_network.local import mounts, api_url
from sugar_network.toolkit import sneakernet
from sugar_network.resources.volume import Volume
from active_toolkit import coroutine, sockets


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Master_push_MisaddressedPackets(self):
        master = MasterCommands('master')
        response = ad.Response()

        packet = OutBufferPacket()
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node', dst='fake')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='master', dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.push, request, response)

        packet = OutBufferPacket(src='node', dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.push(request, response)

    def test_Master_push_ProcessPushes(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_push', 'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 1}}},
            {'cmd': 'sn_push', 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[1, 3]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)
        self.assertEqual(
                ['sn_pull=sn_pull_unset; Max-Age=1; HttpOnly'],
                response.get('Set-Cookie'))

        self.assertEqual(
                ['1', '2', '3'],
                [i.guid for i in master.volume['document'].find()[0]])

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'ack.node.packet', 'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'sequence': [[1, 3]],'merged': [[1, 3]]},
            ],
            [i for i in packet])

    def test_Master_push_ProcessPulls(self):
        master = MasterCommands('master')
        request = Request()
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
        assert reply is None
        self.assertEqual(
                ['sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1], [3, 4], [7, None]]))],
                response.get('Set-Cookie'))

    def test_Master_push_ProcessPushesAndPulls(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_push', 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[1, 3]]},
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'ack.node.packet', 'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'sequence': [[1, 3]], 'merged': [[1, 2]]},
            ],
            [i for i in packet])
        self.assertEqual(
                ['sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[3, None]]))],
                response.get('Set-Cookie'))

    def test_Master_push_ReusePullSeqFromCookies(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[10, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        request.environ['HTTP_COOKIE'] = \
                'sn_pull=%s' % base64.b64encode(json.dumps([[1, 2]]))

        reply = master.push(request, response)
        assert reply is None
        self.assertEqual(
                ['sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 2], [10, None]]))],
                response.get('Set-Cookie'))

    def test_Master_pull_MisaddressedPackets(self):
        master = MasterCommands('master')
        response = ad.Response()

        packet = OutBufferPacket()
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='node')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.pull(request, response)

        packet = OutBufferPacket(src='node', dst='fake')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='master', dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        self.assertRaises(RuntimeError, master.pull, request, response)

        packet = OutBufferPacket(src='node', dst='master')
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())
        master.pull(request, response)

    def test_Master_pull_ProcessPulls(self):
        master = MasterCommands('master')
        request = Request()
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
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_Master_pull_AvoidEmptyPacketsOnPull(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(data=[
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_Master_pull_LimittedPull(self):
        master = MasterCommands('master')

        master.volume['document'].create(guid='1', prop='*' * 1024)
        master.volume['document'].create(guid='2', prop='*' * 1024)

        def rewind():
            request = Request()
            packet = OutBufferPacket(src='node', dst='master')
            packet.push(data=[
                {'cmd': 'sn_pull', 'sequence': [[1, None]]},
                ])
            request.content_stream = packet.pop()
            request.content_length = len(request.content_stream.getvalue())
            return request, ad.Response()

        request, response = rewind()
        reply = master.pull(request, response, accept_length=1024 * 2)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request, response = rewind()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response, accept_length=1024 * 2)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, None]])),
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 1]]},
            ],
            [i for i in packet])

        master._pull_queue.clear()
        request, response = rewind()
        reply = master.pull(request, response, accept_length=1024 * 3)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request, response = rewind()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response, accept_length=1024 * 2)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_Master_pull_ReusePullSeqFromCookies(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(data=[
            {'cmd': 'sn_pull', 'sequence': [[2, 2]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        request.environ['HTTP_COOKIE'] = \
                'sn_pull=%s' % base64.b64encode(json.dumps([[1, 1]]))

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 2]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_Master_clone(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        reply = master.clone(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = ad.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'sn_delay=sn_delay_unset; Max-Age=1; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_Node_Export(self):
        node = NodeMount('node', 'master')

        node.volume['document'].create(guid='1', prop='value1')
        node.volume['document'].create(guid='2', prop='value2')

        node.sync('sync')
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
            self.read_packets('sync'))

    def test_Node_Export_NoPullForExistingSession(self):
        node = NodeMount('node', 'master')

        node.volume['document'].create(guid='1')

        node.sync('sync', session='session')
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
            self.read_packets('sync'))

    def test_Node_LimittedExport(self):
        node = NodeMount('node', 'master')

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
            self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=1024 * 3, push_sequence=kwargs['push_sequence'], session=2)
        self.assertEqual([[4, None]], kwargs['push_sequence'])
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
            self.read_packets('sync'))

        kwargs = node.sync('sync', push_sequence=kwargs['push_sequence'], session=3)
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
            self.read_packets('sync'))

    def test_Node_Import(self):
        node = NodeMount('node', 'master')

        master_packet = OutFilePacket('sync', src='master')
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': 'node', 'sequence': [[1, 2]], 'merged': [[3, 4]]},
            {'cmd': 'sn_ack', 'dst': 'other', 'sequence': [[5, 6]], 'merged': [[7, 8]]},
            {'cmd': 'sn_push', 'document': 'document', 'guid': '2', 'diff': {'guid': {'value': '2', 'mtime': 2}}},
            {'cmd': 'sn_commit', 'sequence': [[11, 12]]},
            ])
        master_packet.close()

        our_packet = OutFilePacket('sync', src='node', dst='master', session='stale')
        our_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[9, 10]]},
            ])
        our_packet.close()

        other_node_packet = OutFilePacket('sync', src='other', dst='master')
        other_node_packet.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 3}}},
            {'cmd': 'sn_commit', 'sequence': [[13, 14]]},
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
        node = NodeMount('node', 'master')

        node.volume['document'].create(guid='1', prop='prop1')
        node.volume['document'].create(guid='2', prop='prop2')
        node.volume['document'].create(guid='3', prop='prop3')

        master_packet = OutFilePacket('sync', src='master')
        master_packet.push(data=[
            {'cmd': 'sn_ack', 'dst': 'node', 'sequence': [[1, 2]], 'merged': []},
            ])
        master_packet.close()

        node.sync('sync', session='session')

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
            self.read_packets('sync'))

    def test_Node_Import_DoNotDeletePacketsFromCurrentSession(self):
        node = NodeMount('node', 'master')
        node.volume['document'].create(guid='1')

        existing_session = OutFilePacket('sync', src='node', dst='master', session='the same')
        existing_session.push(data=[
            {'cmd': 'sn_push', 'document': 'document', 'range': [1, 1], 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            ])
        existing_session.close()

        self.assertEqual(1, len([i for i in sneakernet.walk('sync')]))
        node.sync('sync', session='the same')
        self.assertEqual(
                ['the same', 'the same'],
                [i.header['session'] for i in sneakernet.walk('sync')])
        assert exists(existing_session.path)

        node.sync('sync', session='new one')
        self.assertEqual(
                ['new one'],
                [i.header['session'] for i in sneakernet.walk('sync')])

    def test_Node_sync_session(self):
        node = NodeMount('node', 'master')
        node.volume['document'].create(guid='1', prop='*' * 1024)
        coroutine.dispatch()

        node.publisher = lambda x: events.append(x)
        self.override(os, 'statvfs', lambda x: Statvfs(1024 - 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'event': 'sync_start', 'path': 'sync'},
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
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
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
            {'event': 'sync_continue'},
            ],
            events)
        self.assertEqual([
            {'api_url': api_url.value, 'filename': 'node-1.packet', 'cmd': 'sn_pull', 'src': 'node', 'dst': 'master', 'session': session, 'sequence': [[1, None]]},
            ],
            self.read_packets('sync'))

        self.override(os, 'statvfs', lambda x: Statvfs(1024 + 512))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync_start'},
            {'event': 'sync_progress', 'progress': "Generating 'node-1.packet' packet"},
            {'event': 'sync_complete'},
            ],
            events)
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
            self.read_packets('sync'))

    def read_packets(self, path):
        result = []
        for filename in sorted(os.listdir(path)):
            with InPacket(join(path, filename)) as packet:
                result.extend([i for i in packet])
        return result


class Request(ad.Request):

    def __init__(self, environ=None):
        ad.Request.__init__(self)
        self.environ = environ or {}


class Statvfs(object):

    f_bfree = 0
    f_frsize = 1

    def __init__(self, f_bfree):
        self.f_bfree = f_bfree


class MasterCommands(commands.MasterCommands):

    def __init__(self, master):
        os.makedirs('db')
        with file('db/master', 'w') as f:
            f.write(master)
        commands.MasterCommands.__init__(self, new_volume('db'))


class NodeMount(mounts.NodeMount):

    def __init__(self, node, master):
        os.makedirs('db')
        with file('db/node', 'w') as f:
            f.write(node)
        with file('db/master', 'w') as f:
            f.write(master)
        mounts.NodeMount.__init__(self, new_volume('db'), None)


def new_volume(root):

    class Document(ad.Document):

        @ad.active_property(slot=1, default='')
        def prop(self, value):
            return value

    return Volume(root, [Document])


if __name__ == '__main__':
    tests.main()
