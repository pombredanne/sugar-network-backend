#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import time
import json
import base64
import hashlib
from glob import glob
from os.path import join, exists
from StringIO import StringIO

import rrdtool

from __init__ import tests

from sugar_network.db.directory import Directory
from sugar_network import db, node
from sugar_network.node import sync
from sugar_network.node.master import MasterCommands
from sugar_network.resources.volume import Volume
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import coroutine, util
from sugar_network.toolkit.rrd import Rrd


class statvfs(object):

    f_bfree = None
    f_frsize = 1


class SyncMasterTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.uuid = 0
        self.override(db, 'uuid', self.next_uuid)
        self.override(os, 'statvfs', lambda *args: statvfs())
        statvfs.f_bfree = 999999999

        class Document(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

        node.files_root.value = 'sync'
        self.volume = Volume('master', [Document])
        self.master = MasterCommands(self.volume)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_sync_ExcludeRecentlyMergedDiffFromPull(self):
        request = Request()
        for chunk in sync.encode(
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1},
                        'ctime': {'value': 1, 'mtime': 1},
                        'mtime': {'value': 1, 'mtime': 1},
                        'prop': {'value': 'value', 'mtime': 1},
                        }},
                    {'commit': [[1, 1]]},
                    ]),
                ('pull', {'sequence': [[1, None]]}, None),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = StringIO()
        for chunk in self.master.sync(request):
            response.write(chunk)
        response.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[1, 1]], 'dst': None}, []),
            ({'packet': 'diff', 'src': 'localhost:8888'}, [{'document': 'document'}, {'commit': []}]),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.decode(response)])

        request = Request()
        for chunk in sync.encode(
                ('pull', {'sequence': [[1, None]]}, None),
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '2', 'diff': {
                        'guid': {'value': '2', 'mtime': 2},
                        'ctime': {'value': 2, 'mtime': 2},
                        'mtime': {'value': 2, 'mtime': 2},
                        'prop': {'value': 'value', 'mtime': 2},
                        }},
                    {'commit': [[2, 2]]},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = StringIO()
        for chunk in self.master.sync(request):
            response.write(chunk)
        response.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[2, 2]], 'src': 'localhost:8888', 'sequence': [[2, 2]], 'dst': None}, []),
            ({'packet': 'diff', 'src': 'localhost:8888'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'prop': {'value': 'value', 'mtime': 1},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.decode(response)])

    def test_sync_MisaddressedPackets(self):
        request = Request()
        for chunk in sync.encode(('pull', {'sequence': [[1, None]]}, None)):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)
        self.assertRaises(RuntimeError, lambda: next(self.master.sync(request)))

        request = Request()
        for chunk in sync.encode(('pull', {'sequence': [[1, None]]}, None), dst='fake'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)
        self.assertRaises(RuntimeError, lambda: next(self.master.sync(request)))

        request = Request()
        for chunk in sync.encode(('pull', {'sequence': [[1, None]]}, None), dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)
        next(self.master.sync(request))

    def test_push_WithoutCookies(self):
        ts = int(time.time())

        request = Request()
        for chunk in sync.package_encode(
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1},
                        'ctime': {'value': 1, 'mtime': 1},
                        'mtime': {'value': 1, 'mtime': 1},
                        'prop': {'value': 'value', 'mtime': 1},
                        }},
                    {'commit': [[1, 1]]},
                    ]),
                ('stats_diff', {'dst': 'localhost:8888'}, [
                    {'db': 'db', 'user': 'user'},
                    {'timestamp': ts, 'values': {'field': 1.0}},
                    {'commit': {'user': {'db': [[1, ts]]}}},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[1, 1]], 'dst': None, 'filename': '2.sneakernet'}, []),
            ({'packet': 'stats_ack', 'sequence': {'user': {'db': [[1, ts]]}}, 'src': 'localhost:8888', 'dst': None, 'filename': '2.sneakernet'}, []),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_WithCookies(self):
        ts = int(time.time())

        request = Request()
        for chunk in sync.package_encode(
                ('pull', {'sequence': [[1, None]]}, None),
                ('files_pull', {'sequence': [[1, None]]}, None),
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '2', 'diff': {
                        'guid': {'value': '2', 'mtime': 2},
                        'ctime': {'value': 2, 'mtime': 2},
                        'mtime': {'value': 2, 'mtime': 2},
                        'prop': {'value': 'value', 'mtime': 2},
                        }},
                    {'commit': [[2, 2]]},
                    ]),
                ('stats_diff', {'dst': 'localhost:8888'}, [
                    {'db': 'db', 'user': 'user'},
                    {'timestamp': ts + 1, 'values': {'field': 2.0}},
                    {'commit': {'user': {'db': [[2, ts]]}}},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[2, 2]], 'dst': None, 'filename': '2.sneakernet'}, []),
            ({'packet': 'stats_ack', 'sequence': {'user': {'db': [[2, ts]]}}, 'src': 'localhost:8888', 'dst': None, 'filename': '2.sneakernet'}, []),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[2, None]]), ('files_pull', None, [[1, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_CollectCookies(self):
        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[10, None]]), ('files_pull', None, [[10, None]])]))
        for chunk in sync.package_encode(
                ('pull', {'sequence': [[11, None]]}, None),
                ('files_pull', {'sequence': [[11, None]]}, None),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([], [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[10, None]]), ('files_pull', None, [[10, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[10, None]]), ('files_pull', None, [[10, None]])]))
        for chunk in sync.package_encode(
                ('pull', {'sequence': [[1, 5]]}, None),
                ('files_pull', {'sequence': [[1, 5]]}, None),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([], [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[1, 5], [10, None]]), ('files_pull', None, [[1, 5], [10, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_ExcludeAcksFromCookies(self):
        ts = int(time.time())

        request = Request()
        for chunk in sync.package_encode(
                ('pull', {'sequence': [[1, None]]}, None),
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1},
                        'ctime': {'value': 1, 'mtime': 1},
                        'mtime': {'value': 1, 'mtime': 1},
                        'prop': {'value': 'value', 'mtime': 1},
                        }},
                    {'commit': [[10, 10]]},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[10, 10]], 'dst': None, 'filename': '2.sneakernet'}, []),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[2, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_DoNotExcludeAcksFromExistingCookies(self):
        ts = int(time.time())

        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[1, None]])]))
        for chunk in sync.package_encode(
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1},
                        'ctime': {'value': 1, 'mtime': 1},
                        'mtime': {'value': 1, 'mtime': 1},
                        'prop': {'value': 'value', 'mtime': 1},
                        }},
                    {'commit': [[10, 10]]},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[10, 10]], 'dst': None, 'filename': '2.sneakernet'}, []),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[1, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_push_ExcludeAcksFromCookiesUsingProperLayer(self):
        ts = int(time.time())

        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[1, None]])]))
        for chunk in sync.package_encode(
                ('pull', {'sequence': [[1, None]], 'layer': 'hidden'}, None),
                ('diff', None, [
                    {'document': 'document'},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1},
                        'ctime': {'value': 1, 'mtime': 1},
                        'mtime': {'value': 1, 'mtime': 1},
                        'prop': {'value': 'value', 'mtime': 1},
                        }},
                    {'commit': [[10, 10]]},
                    ]),
                dst='localhost:8888'):
            request.content_stream.write(chunk)
        request.content_stream.seek(0)

        response = db.Response()
        reply = StringIO()
        for chunk in self.master.push(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'ack', 'ack': [[1, 1]], 'src': 'localhost:8888', 'sequence': [[10, 10]], 'dst': None, 'filename': '2.sneakernet'}, []),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.package_decode(reply)])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', 'hidden', [[2, None]]), ('pull', None, [[1, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_pull(self):
        self.volume['document'].create(guid='1', prop='1', ctime=1, mtime=1)
        self.volume['document'].create(guid='2', prop='2', ctime=2, mtime=2)
        self.utime('master', 0)
        self.touch(('sync/1', 'file1'))
        self.touch(('sync/2', 'file2'))

        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[1, None]]), ('files_pull', None, [[1, None]])]))
        response = db.Response()
        self.assertEqual(None, self.master.pull(request, response))
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[1, None]]), ('files_pull', None, [[1, None]])])),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        coroutine.sleep(.5)

        request = Request()
        request.environ['HTTP_COOKIE'] = response.get('Set-Cookie')[0]
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.pull(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'diff'}, [
                {'document': 'document'},
                {'guid': '1', 'diff': {
                    'prop': {'value': '1', 'mtime': 0},
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0}},
                    },
                {'guid': '2', 'diff': {
                    'prop': {'value': '2', 'mtime': 0},
                    'guid': {'value': '2', 'mtime': 0},
                    'ctime': {'value': 2, 'mtime': 0},
                    'mtime': {'value': 2, 'mtime': 0}},
                    },
                {'commit': [[1, 2]]},
                ])
            ],
            [(packet.props, [i for i in packet]) for packet in sync.decode(gzip.GzipFile(mode='r', fileobj=reply))])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('files_pull', None, [[1, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        request = Request()
        request.environ['HTTP_COOKIE'] = response.get('Set-Cookie')[0]
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.pull(request, response):
            reply.write(chunk)
        reply.seek(0)
        packets_iter = sync.decode(gzip.GzipFile(mode='r', fileobj=reply))
        with next(packets_iter) as packet:
            self.assertEqual('files_diff', packet.name)
            records_iter = iter(packet)
            self.assertEqual('file1', next(records_iter)['blob'].read())
            self.assertEqual('file2', next(records_iter)['blob'].read())
            self.assertEqual({'op': 'commit', 'sequence': [[1, 4]]}, next(records_iter))
            self.assertRaises(StopIteration, records_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def test_pull_EmptyPackets(self):
        self.master._pulls = {
            'pull': lambda layer, seq, out_seq=None: \
                ('diff', None, [{'layer': layer, 'seq': seq}]),
            }

        request = Request()
        request.environ['HTTP_COOKIE'] = 'sugar_network_sync=%s' % \
                base64.b64encode(json.dumps([('pull', None, [[1, None]])]))
        response = db.Response()
        self.assertEqual(None, self.master.pull(request, response))
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[1, None]])])),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        coroutine.sleep(.5)
        self.assertEqual(1, len([i for i in glob('tmp/pulls/*.tag')]))

        request = Request()
        request.environ['HTTP_COOKIE'] = response.get('Set-Cookie')[0]
        response = db.Response()
        self.assertEqual(None, self.master.pull(request, response))
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))
        self.assertEqual(0, len([i for i in glob('tmp/pulls/*.tag')]))

    def test_pull_FullClone(self):

        def diff(layer, seq, out_seq):
            out_seq.include(1, 10)
            yield {'layer': layer, 'seq': seq}

        self.master._pulls = {
            'pull': lambda layer, seq, out_seq: ('diff', None, diff(layer, seq, out_seq)),
            'files_pull': lambda layer, seq, out_seq: ('files_diff', None, diff(layer, seq, out_seq)),
            }

        request = Request()
        response = db.Response()
        self.assertEqual(None, self.master.pull(request, response))
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('pull', None, [[1, None]]), ('files_pull', None, [[1, None]])])),
            'sugar_network_delay=30; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))
        coroutine.sleep(.5)

        request = Request()
        request.environ['HTTP_COOKIE'] = response.get('Set-Cookie')[0]
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.pull(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'diff'}, [{'layer': None, 'seq': [[1, None]]}]),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.decode(gzip.GzipFile(mode='r', fileobj=reply))])
        self.assertEqual([
            'sugar_network_sync=%s; Max-Age=3600; HttpOnly' % \
                    base64.b64encode(json.dumps([('files_pull', None, [[1, None]])])),
            'sugar_network_delay=0; Max-Age=3600; HttpOnly',
            ],
            response.get('Set-Cookie'))

        request = Request()
        request.environ['HTTP_COOKIE'] = response.get('Set-Cookie')[0]
        response = db.Response()
        reply = StringIO()
        for chunk in self.master.pull(request, response):
            reply.write(chunk)
        reply.seek(0)
        self.assertEqual([
            ({'packet': 'files_diff'}, [{'layer': None, 'seq': [[1, None]]}]),
            ],
            [(packet.props, [i for i in packet]) for packet in sync.decode(gzip.GzipFile(mode='r', fileobj=reply))])
        self.assertEqual([
            'sugar_network_sync=unset_sugar_network_sync; Max-Age=0; HttpOnly',
            'sugar_network_delay=unset_sugar_network_delay; Max-Age=0; HttpOnly',
            ],
            response.get('Set-Cookie'))

    def __test_pull_LimittedPull(self):
        pass

    def __test_pull_ReusePullSeqFromCookies(self):
        pass

    def __test_pull_AskForNotYetReadyPull(self):
        pass

    def __test_pull_ProcessFilePulls(self):
        pass

    def __test_ReuseCachedPulls(self):
        pass


class Request(object):

    def __init__(self):
        self.content_stream = StringIO()
        self.environ = {}


if __name__ == '__main__':
    tests.main()
