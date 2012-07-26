#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import base64
import hashlib
from os.path import join, exists

from __init__ import tests

import active_document as ad
from active_document.directory import Directory
from sugar_network.toolkit.sneakernet import InPacket, OutPacket, OutBufferPacket
from sugar_network.node import commands
from sugar_network.resources.volume import Volume
from active_toolkit import coroutine


class SyncMasterTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)
        commands._PULL_QUEUE_SIZE = 256

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_push_MisaddressedPackets(self):
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

    def test_push_ProcessPushes(self):
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
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly'
            ],
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

    def test_push_ProcessPulls(self):
        master = MasterCommands('master')

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[1, 1]]},
            {'cmd': 'sn_pull', 'sequence': [[3, 4]]},
            {'cmd': 'sn_pull', 'sequence': [[7, None]]},
            ])
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        response = ad.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1], [3, 4], [7, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'files_pull', 'sequence': [[1, 1]]},
            {'cmd': 'files_pull', 'sequence': [[3, 4]]},
            {'cmd': 'files_pull', 'sequence': [[7, None]]},
            ])
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        response = ad.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1], [3, 4], [7, None]])),
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[1, 1]]},
            {'cmd': 'files_pull', 'sequence': [[1, 1]]},
            ])
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        response = ad.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1]])),
            'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1]])),
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_ProcessPushesAndPulls(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_push', 'guid': '3', 'diff': {'guid': {'value': '3', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[1, 3]]},
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            {'cmd': 'files_pull', 'sequence': [[1, None]]},
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
        self.assertEqual([
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[3, None]])),
            'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_ReusePullSeqFromCookies(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[10, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        request.environ['HTTP_COOKIE'] = 'sn_pull=%s; files_pull=%s' % (
                base64.b64encode(json.dumps([[1, 2]])),
                base64.b64encode(json.dumps([[3, 4]])),
                )

        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 2], [10, None]])),
            'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[3, 4]])),
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_pull_ProcessPulls(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
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
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

        response = ad.Response()
        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual(3, len([i for i in packet]))

    def test_pull_AvoidEmptyPacketsOnPull(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
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
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_pull_LimittedPull(self):
        master = MasterCommands('master')

        master.volume['document'].create(guid='1', prop='*' * 1024)
        master.volume['document'].create(guid='2', prop='*' * 1024)

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, accept_length=1024 * 2, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        response = ad.Response()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        reply = master.pull(request, response, accept_length=1024 * 2, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'postponed_sequence': [[2, None]], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'postponed_sequence': [[2, None]], 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 1]]},
            ],
            [i for i in packet])

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, accept_length=1024 * 2, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual(2, len([i for i in packet]))

        for i in master._pull_queue.values():
            i.unlink()
        master._pull_queue.clear()

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, accept_length=1024 * 3, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        response = ad.Response()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        reply = master.pull(request, response, accept_length=1024 * 2, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * 1024,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_pull_ReusePullSeqFromCookies(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')

        request.environ['HTTP_COOKIE'] = 'sn_pull=%s files_pull=%s' % ( \
                base64.b64encode(json.dumps([[1, 1]])),
                base64.b64encode(json.dumps([[2, 2]])),
                )

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, 1]])),
                'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, 2]])),
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
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
            'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=1; HttpOnly',
            'files_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, 2]])),
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'postponed_sequence': [], 'filename': 'master-1.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-1.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 1]]},
            ],
            [i for i in packet])

    def test_pull_AskForNotYetReadyPull(self):
        master = MasterCommands('master')

        def diff(*args, **kwargs):
            for i in range(1024):
                yield {'guid': str(i), 'seqno': i}, {}
                coroutine.sleep(.1)

        self.override(Directory, 'diff', diff)

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        coroutine.sleep(1)

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_clone(self):
        master = MasterCommands('master')
        request = Request()
        response = ad.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
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
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/1/1/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/1/1/layer').st_mtime},
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'user':  {'value': [],          'mtime': os.stat('db/document/2/2/user').st_mtime},
                'layer': {'value': ['public'],  'mtime': os.stat('db/document/2/2/layer').st_mtime},
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'postponed_sequence': [], 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_ReuseCachedPulls(self):
        master = MasterCommands('master')

        cached_pull = join('tmp', pull_hash([[1, None]]) + '.pull')
        with OutPacket(stream=file(cached_pull, 'w'), probe='test', postponed_sequence=[]) as packet:
            packet.push(data=[None])

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=sn_pull_unset; Max-Age=0; HttpOnly',
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_BREAK=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('test', packet.header['probe'])

        for i in master._pull_queue.values():
            i.unlink()
        master._pull_queue.clear()

        cached_pull = join('tmp', pull_hash([[1, None]]) + '.pull')
        with OutPacket(stream=file(cached_pull, 'w'), probe='test', postponed_sequence=[[2, None]]) as packet:
            packet.push(data=[None])

        request = Request()
        response = ad.Response()
        reply = master.pull(request, response, sequence='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sn_delay=sn_delay_unset; Max-Age=0; HttpOnly',
            'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, None]])),
            'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
            'SN_CONTINUE=; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('test', packet.header['probe'])

    def test_UnlinkCachedPullsOnEjectionFromQueue(self):
        commands._PULL_QUEUE_SIZE = 1
        master = MasterCommands('master')

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        response = ad.Response()
        reply = master.pull(Request(), response, sequence='[[1, null]]')
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[1, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        assert exists(join('tmp', pull_hash([[1, None]]) + '.pull'))

        response = ad.Response()
        reply = master.pull(Request(), response, sequence='[[2, null]]')
        cookie = [
                'sn_delay=sn_delay:30; Max-Age=3600; HttpOnly',
                'sn_pull=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps([[2, None]])),
                'files_pull=files_pull_unset; Max-Age=0; HttpOnly',
                'SN_CONTINUE=; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        assert not exists(join('tmp', pull_hash([[1, None]]) + '.pull'))
        assert exists(join('tmp', pull_hash([[2, None]]) + '.pull'))


class Request(ad.Request):

    def __init__(self, environ=None):
        ad.Request.__init__(self)
        self.environ = environ or {}


class MasterCommands(commands.MasterCommands):

    def __init__(self, master):
        os.makedirs('db')
        with file('db/master', 'w') as f:
            f.write(master)
        commands.MasterCommands.__init__(self, new_volume('db'))


def new_volume(root):

    class Document(ad.Document):

        @ad.active_property(slot=1, default='')
        def prop(self, value):
            return value

    return Volume(root, [Document])


def pull_hash(seq):
    return hashlib.sha1(json.dumps(seq)).hexdigest()


if __name__ == '__main__':
    tests.main()
