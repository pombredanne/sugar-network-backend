#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import base64
import hashlib
from os.path import join, exists

import rrdtool

from __init__ import tests

from sugar_network.db.directory import Directory
from sugar_network import db, node
from sugar_network.toolkit.sneakernet import InPacket, OutPacket, OutBufferPacket
from sugar_network.toolkit.files_sync import Seeder
from sugar_network.toolkit.router import Request
from sugar_network.node import sync_master
from sugar_network.resources.volume import Volume
from sugar_network.toolkit import coroutine, util


CHUNK = 100000


class SyncMasterTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(db, 'uuid', self.next_uuid)
        sync_master._PULL_QUEUE_SIZE = 256

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_push_MisaddressedPackets(self):
        master = MasterCommands('master')
        response = db.Response()

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
        response = db.Response()

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
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
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

        response = db.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, 1], [3, 4], [7, None]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'files_pull', 'directory': 'dir1', 'sequence': [[1, 1]]},
            {'cmd': 'files_pull', 'directory': 'dir1', 'sequence': [[3, 4]]},
            {'cmd': 'files_pull', 'directory': 'dir2', 'sequence': [[7, None]]},
            ])
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        response = db.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'dir1': [[1, 1], [3, 4]], 'dir2': [[7, None]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_pull', 'sequence': [[1, 1]]},
            {'cmd': 'files_pull', 'directory': 'dir', 'sequence': [[1, 1]]},
            ])
        request = Request()
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        response = db.Response()
        reply = master.push(request, response)
        assert reply is None
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, 1]], 'dir': [[1, 1]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_TweakPullAccordingToPush(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[1, 1]]},
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'ack.node.packet', 'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'sequence': [[1, 1]], 'merged': [[1, 1]]},
            ],
            [i for i in packet])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[2, None]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_DoNotTweakPullAccordingToPushIfCookieWasPassed(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        packet = OutBufferPacket(src='node', dst='master')
        packet.push(document='document', data=[
            {'cmd': 'sn_push', 'guid': '1', 'diff': {'guid': {'value': '1', 'mtime': 1}}},
            {'cmd': 'sn_commit', 'sequence': [[1, 1]]},
            {'cmd': 'sn_pull', 'sequence': [[1, None]]},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps({'sn_pull': [[1, None]]}))

        reply = master.push(request, response)

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'ack.node.packet', 'src': 'master', 'dst': 'node', 'cmd': 'sn_ack', 'sequence': [[1, 1]], 'merged': [[1, 1]]},
            ],
            [i for i in packet])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_ProcessStatsPushes(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        ts = int(time.time()) - 1000
        packet = OutBufferPacket(src='node', dst='master')
        packet.push(cmd='stats_push', user='user1', db='db1', sequence=[[1, ts + 2]], data=[
            {'timestamp': ts + 1, 'values': {'f': 1}},
            {'timestamp': ts + 2, 'values': {'f': 2}},
            ])
        packet.push(cmd='stats_push', user='user1', db='db2', sequence=[[2, ts + 3]], data=[
            {'timestamp': ts + 3, 'values': {'f': 3}},
            ])
        packet.push(cmd='stats_push', user='user2', db='db3', sequence=[[ts + 4, ts + 4]], data=[
            {'timestamp': ts + 4, 'values': {'f': 4}},
            ])
        request.content_stream = packet.pop()
        request.content_length = len(request.content_stream.getvalue())

        reply = master.push(request, response)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header.get('dst'))
        self.assertEqual([
            {'filename': 'ack.node.packet', 'src': 'master', 'dst': 'node', 'cmd': 'stats_ack',
                'sequence': {
                    'user1': {'db1': [[1, ts + 2]], 'db2': [[2, ts + 3]]},
                    'user2': {'db3': [[ts + 4, ts + 4]]},
                    },
                }
            ],
            [i for i in packet])

        __, __, values = rrdtool.fetch('stats/user/us/user1/db1.rrd', 'AVERAGE', '-s', str(ts), '-e', str(ts + 2))
        self.assertEqual([(1,), (2,), (None,)], values)

        __, __, values = rrdtool.fetch('stats/user/us/user1/db2.rrd', 'AVERAGE', '-s', str(ts), '-e', str(ts + 3))
        self.assertEqual([(None,), (None,), (3,), (None,)], values)

        __, __, values = rrdtool.fetch('stats/user/us/user2/db3.rrd', 'AVERAGE', '-s', str(ts), '-e', str(ts + 4))
        self.assertEqual([(None,), (None,), (None,), (4,), (None,)], values)

    def test_pull_ProcessPulls(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = db.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

        response = db.Response()
        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual(3, len([i for i in packet]))

    def test_pull_AvoidEmptyPacketsOnPull(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = db.Response()
        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_pull_LimittedPull(self):
        master = MasterCommands('master')

        master.volume['document'].create(guid='1', prop='*' * CHUNK)
        master.volume['document'].create(guid='2', prop='*' * CHUNK)

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, accept_length=CHUNK * 1.5, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        response = db.Response()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        reply = master.pull(request, response, accept_length=CHUNK * 1.5, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[2, None]]})),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'cookie': {'sn_pull': [[2, None]]}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * CHUNK,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'cookie': {'sn_pull': [[2, None]]}, 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 1]]},
            ],
            [i for i in packet])

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, accept_length=CHUNK * 1.5, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[2, None]]})),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
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
        response = db.Response()
        reply = master.pull(request, response, accept_length=CHUNK * 2.5, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        response = db.Response()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        reply = master.pull(request, response, accept_length=CHUNK * 2.5, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '*' * CHUNK,  'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '*' * CHUNK,  'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_pull_ReusePullSeqFromCookies(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        master.volume['document'].create(guid='1')

        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps({'sn_pull': [[1, 1]], 'foo': [[2, 2]]}))

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, 1]], 'foo': [[2, 2]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = db.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'cookie': {}, 'filename': 'master-1.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-1.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 1]]},
            ],
            [i for i in packet])

    def test_pull_AskForNotYetReadyPull(self):
        master = MasterCommands('master')

        def diff(*args, **kwargs):
            for i in range(1024):
                yield str(i), i, {}
                coroutine.sleep(.1)

        self.override(Directory, 'diff', diff)

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        coroutine.sleep(1)

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_clone(self):
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        reply = master.pull(request, response)
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = db.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))
        self.assertEqual([
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '1', 'diff': {
                'guid':  {'value': '1',         'mtime': os.stat('db/document/1/1/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/1/1/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/1/1/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/1/1/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'content_type': 'records', 'cmd': 'sn_push', 'src': 'master', 'document': 'document', 'guid': '2', 'diff': {
                'guid':  {'value': '2',         'mtime': os.stat('db/document/2/2/guid').st_mtime},
                'ctime': {'value': 0,           'mtime': os.stat('db/document/2/2/ctime').st_mtime},
                'mtime': {'value': 0,           'mtime': os.stat('db/document/2/2/mtime').st_mtime},
                'prop':  {'value': '',          'mtime': os.stat('db/document/2/2/prop').st_mtime},
                }},
            {'cookie': {}, 'filename': 'master-2.packet', 'cmd': 'sn_commit', 'src': 'master', 'sequence': [[1, 2]]},
            ],
            [i for i in packet])

    def test_pull_ProcessFilePulls(self):
        node.sync_dirs.value = ['files']
        seqno = util.Seqno('seqno')
        master = MasterCommands('master')
        request = Request()
        response = db.Response()

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)

        reply = master.pull(request, response, files='[[1, null]]')
        assert reply is None
        self.assertEqual(None, response.content_type)
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'files': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        coroutine.sleep(1)

        request = Request()
        request.environ['HTTP_COOKIE'] = ';'.join(cookie)
        response = db.Response()
        reply = master.pull(request, response)
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet.header['src'])
        self.assertEqual(None, packet.header.get('dst'))

        response = db.Response()
        reply = master.pull(request, response, files='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))
        self.assertEqual(
                sorted([
                    {'filename': 'master-0.packet', 'src': 'master', 'cookie': {}, 'cmd': 'files_push', 'directory': 'files', 'blob': '1', 'content_type': 'blob', 'path': '1'},
                    {'filename': 'master-0.packet', 'src': 'master', 'cookie': {}, 'cmd': 'files_push', 'directory': 'files', 'blob': '2', 'content_type': 'blob', 'path': '2'},
                    {'filename': 'master-0.packet', 'src': 'master', 'cookie': {}, 'cmd': 'files_push', 'directory': 'files', 'blob': '3', 'content_type': 'blob', 'path': '3'},
                    {'filename': 'master-0.packet', 'src': 'master', 'cookie': {}, 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 3]]},
                    ]),
                read_records(reply))

    def test_ReuseCachedPulls(self):
        master = MasterCommands('master')

        cached_pull = join('tmp', pull_hash({'sn_pull': [[1, None]]}) + '.pull')
        with OutPacket(stream=file(cached_pull, 'w'), probe='test', cookie={}) as packet:
            packet.push(data=[None])

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('test', packet.header['probe'])

        for i in master._pull_queue.values():
            i.unlink()
        master._pull_queue.clear()

        cached_pull = join('tmp', pull_hash({'sn_pull': [[1, None]]}) + '.pull')
        with OutPacket(stream=file(cached_pull, 'w'), probe='test', cookie={'sn_pull': [[2, None]]}) as packet:
            packet.push(data=[None])

        request = Request()
        response = db.Response()
        reply = master.pull(request, response, sn_pull='[[1, null]]')
        assert reply is not None
        self.assertEqual('application/x-tar', response.content_type)
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[2, None]]})),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        packet = InPacket(stream=reply)
        self.assertEqual('test', packet.header['probe'])

    def test_UnlinkCachedPullsOnEjectionFromQueue(self):
        sync_master._PULL_QUEUE_SIZE = 1
        master = MasterCommands('master')

        master.volume['document'].create(guid='1')
        master.volume['document'].create(guid='2')

        response = db.Response()
        reply = master.pull(Request(), response, sn_pull='[[1, null]]')
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[1, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        assert exists(join('tmp', pull_hash({'sn_pull': [[1, None]]}) + '.pull'))

        response = db.Response()
        reply = master.pull(Request(), response, sn_pull='[[2, null]]')
        cookie = [
                'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % base64.b64encode(json.dumps({'sn_pull': [[2, None]]})),
                'sugar_network_delay=30; Max-Age=3600; HttpOnly',
                ]
        self.assertEqual(cookie, response.get('Set-Cookie'))
        assert not exists(join('tmp', pull_hash({'sn_push': [[1, None]]}) + '.pull'))
        assert exists(join('tmp', pull_hash({'sn_pull': [[2, None]]}) + '.pull'))


class Request(db.Request):

    def __init__(self, environ=None):
        db.Request.__init__(self)
        self.environ = environ or {}


class MasterCommands(sync_master.SyncCommands):

    def __init__(self, master, **kwargs):
        os.makedirs('db')
        with file('db/master', 'w') as f:
            f.write(master)
        sync_master.SyncCommands._guid = master
        sync_master.SyncCommands.volume = new_volume('db')
        sync_master.SyncCommands.__init__(self, **kwargs)


def new_volume(root):

    class Document(db.Document):

        @db.indexed_property(slot=1, default='')
        def prop(self, value):
            return value

    return Volume(root, [Document])


def pull_hash(seq):
    return hashlib.sha1(json.dumps(seq)).hexdigest()


def read_records(reply):
    records = []
    for i in InPacket(stream=reply):
        if i.get('content_type') == 'blob':
            i['blob'] = i['blob'].read()
        records.append(i)
    return sorted(records)


if __name__ == '__main__':
    tests.main()
