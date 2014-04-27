#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import uuid
import json
import hashlib
from StringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit, client
from sugar_network.toolkit.router import File, route, Router
from sugar_network.toolkit import packets, http, coroutine


class PacketsTest(tests.Test):

    def test_decode_Zipped(self):
        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual('foo', packet['bar'])
            packet_iter = iter(packet)
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_NotZipped(self):
        stream = StringIO(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = StringIO(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual('foo', packet['bar'])
            packet_iter = iter(packet)
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = StringIO(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = StringIO(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = StringIO(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_ZippedWithLimit(self):
        payload = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 'first'}) + '\n'
            ).getvalue()
        tail = '.' * 100

        stream = StringIO(payload + tail)
        for i in packets.decode(stream):
            pass
        self.assertEqual(len(payload + tail), stream.tell())

        stream = StringIO(payload + tail)
        for i in packets.decode(stream, limit=len(payload)):
            pass
        self.assertEqual(len(payload), stream.tell())

    def test_decode_NotZippedWithLimit(self):
        payload = StringIO(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 'first'}) + '\n'
            ).getvalue()
        tail = '.' * 100

        stream = StringIO(payload + tail)
        self.assertRaises(ValueError, lambda: [i for i in packets.decode(stream)])
        self.assertEqual(len(payload + tail), stream.tell())

        stream = StringIO(payload + tail)
        for i in packets.decode(stream, limit=len(payload)):
            pass
        self.assertEqual(len(payload), stream.tell())

    def test_decode_Empty(self):
        self.assertRaises(http.BadRequest, packets.decode, StringIO())

        stream = zips(
            ''
            )
        self.assertRaises(StopIteration, iter(packets.decode(stream)).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        self.assertRaises(StopIteration, iter(packets.decode(stream)).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        self.assertRaises(StopIteration, iter(packets.decode(stream)).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipPackets(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'payload': 11}) + '\n' +
            json.dumps({'payload': 111}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = iter(packets.decode(stream))
        next(packets_iter)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream.seek(0)
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_Blobs(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 2}) + '\n' +
            'bb' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([
                (1, hashlib.sha1('a').hexdigest(), 'a'),
                (2, hashlib.sha1('bb').hexdigest(), 'bb'),
                ],
                [(i.meta['num'], i.digest, file(i.path).read()) for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([
                (3, hashlib.sha1('ccc').hexdigest(), 'ccc'),
                ],
                [(i.meta['num'], i.digest, file(i.path).read()) for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_EmptyBlobs(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 0}) + '\n' +
            '' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([
                (1, 'a'),
                (2, ''),
                ],
                [(i.meta['num'], file(i.path).read()) for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([
                (3, 'ccc'),
                ],
                [(i.meta['num'], file(i.path).read()) for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipNotReadBlobs(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 2}) + '\n' +
            'bb' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc'
            )
        packets_iter = iter(packets.decode(stream))
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([1, 2], [i.meta['num'] for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([3], [i.meta['num'] for i in packet])
        self.assertRaises(StopIteration, next, packets_iter)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_encode_Zipped(self):
        stream = ''.join([i for i in packets.encode([])])
        self.assertEqual(
                json.dumps({}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in packets.encode([(None, None, None)], header={'foo': 'bar'})])
        self.assertEqual(
                json.dumps({'foo': 'bar'}) + '\n' +
                json.dumps({'segment': None}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in packets.encode([
            (1, {}, None),
            ('2', {'n': 2}, []),
            ('3', {'n': 3}, iter([])),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'segment': '2', 'n': 2}) + '\n' +
                json.dumps({'segment': '3', 'n': 3}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in packets.encode([
            (1, None, [{1: 1}]),
            (2, None, [{2: 2}, {2: 2}]),
            (3, None, [{3: 3}, {3: 3}, {3: 3}]),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({1: 1}) + '\n' +
                json.dumps({'segment': 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({'segment': 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n',
                unzips(stream))

    def test_encode_NotZipped(self):
        stream = ''.join([i for i in packets.encode([], compresslevel=0)])
        self.assertEqual(
                json.dumps({}) + '\n',
                stream)

        stream = ''.join([i for i in packets.encode([(None, None, None)], header={'foo': 'bar'}, compresslevel=0)])
        self.assertEqual(
                json.dumps({'foo': 'bar'}) + '\n' +
                json.dumps({'segment': None}) + '\n',
                stream)

        stream = ''.join([i for i in packets.encode([
            (1, {}, None),
            ('2', {'n': 2}, []),
            ('3', {'n': 3}, iter([])),
            ], compresslevel=0)])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'segment': '2', 'n': 2}) + '\n' +
                json.dumps({'segment': '3', 'n': 3}) + '\n',
                stream)

        stream = ''.join([i for i in packets.encode([
            (1, None, [{1: 1}]),
            (2, None, [{2: 2}, {2: 2}]),
            (3, None, [{3: 3}, {3: 3}, {3: 3}]),
            ], compresslevel=0)])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({1: 1}) + '\n' +
                json.dumps({'segment': 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({'segment': 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n',
                stream)

    def test_limited_encode(self):
        RECORD = 1024 * 1024

        def content():
            yield {'record': '.' * RECORD}
            yield {'record': '.' * RECORD}

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2)]))
        assert len(stream) < RECORD
        self.assertEqual(3, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5)]))
        assert len(stream) > RECORD
        assert len(stream) < RECORD * 2
        self.assertEqual(4, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5)]))
        assert len(stream) > RECORD * 2
        assert len(stream) < RECORD * 3
        self.assertEqual(5, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5)]))
        assert len(stream) > RECORD * 3
        assert len(stream) < RECORD * 4
        self.assertEqual(6, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5)]))
        assert len(stream) > RECORD * 4
        self.assertEqual(7, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            )]))
        assert len(stream) > RECORD * 4

    def test_limited_encode_FinalRecords(self):
        RECORD = 1024 * 1024

        def content():
            try:
                yield {'record': '.' * RECORD}
                yield {'record': '.' * RECORD}
            except StopIteration:
                pass
            yield None
            yield {'record': '.' * RECORD}
            yield {'record': '.' * RECORD}

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2)]))
        assert len(stream) > RECORD * 4
        assert len(stream) < RECORD * 5
        self.assertEqual(7, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5)]))
        assert len(stream) > RECORD * 5
        assert len(stream) < RECORD * 6
        self.assertEqual(8, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(9, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(9, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(9, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 5.5)]))
        assert len(stream) > RECORD * 7
        assert len(stream) < RECORD * 8
        self.assertEqual(10, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 6.5)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(11, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            )]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(11, len(stream.strip().split('\n')))

    def test_encode_Blobs(self):
        self.touch(('a', 'a'))
        self.touch(('b', 'bb'))
        self.touch(('c', 'ccc'))

        stream = ''.join([i for i in packets.encode([
            (1, None, [
                File('a', 'digest', [('num', 1)]),
                File('b', 'digest', [('num', 2)]),
                ]),
            (2, None, [
                File('c', 'digest', [('num', 3)]),
                ]),
            ])])

        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'num': 1}) + '\n' +
                'a' + '\n' +
                json.dumps({'num': 2}) + '\n' +
                'bb' + '\n' +
                json.dumps({'segment': 2}) + '\n' +
                json.dumps({'num': 3}) + '\n' +
                'ccc' + '\n',
                unzips(stream))

    def test_encode_BlobWithUrls(self):

        class Routes(object):

            @route('GET')
            def probe(self):
                return 'probe'

        server = coroutine.WSGIServer(('127.0.0.1', client.ipc_port.value), Router(Routes()))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()
        url = 'http://127.0.0.1:%s' % client.ipc_port.value

        stream = ''.join([i for i in packets.encode([
            (1, None, [File(None, meta={'location': 'fake'})]),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'location': 'fake'}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in packets.encode([
            (1, None, [File(None, meta={'location': 'fake', 'content-length': '0'})]),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'location': 'fake', 'content-length': '0'}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in packets.encode([
            (1, None, [File(None, meta={'location': url, 'content-length': str(len('probe'))})]),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'location': url, 'content-length': str(len('probe'))}) + '\n' +
                'probe' + '\n',
                unzips(stream))

        def encode():
            stream = ''.join([i for i in packets.encode([
                (1, None, [File(None, meta={'location': 'http://127.0.0.1:108', 'content-length': str(len('probe'))})]),
                ])])
        self.assertRaises(http.ConnectionError, encode)

    def test_limited_encode_Blobs(self):
        RECORD = 1024 * 1024
        self.touch(('blob', '.' * RECORD))

        def content():
            yield File('blob', 'digest')
            yield File('blob', 'digest')

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2)]))
        assert len(stream) < RECORD
        self.assertEqual(3, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5)]))
        assert len(stream) > RECORD
        assert len(stream) < RECORD * 2
        self.assertEqual(5, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5)]))
        assert len(stream) > RECORD * 2
        assert len(stream) < RECORD * 3
        self.assertEqual(7, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5)]))
        assert len(stream) > RECORD * 3
        assert len(stream) < RECORD * 4
        self.assertEqual(9, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5)]))
        assert len(stream) > RECORD * 4
        self.assertEqual(11, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            )]))
        assert len(stream) > RECORD * 4
        self.assertEqual(11, len(stream.strip().split('\n')))

    def test_limited_encode_FinalBlobs(self):
        RECORD = 1024 * 1024
        self.touch(('blob', '.' * RECORD))

        def content():
            try:
                yield File('blob', 'digest')
                yield File('blob', 'digest')
            except StopIteration:
                pass
            yield None
            yield File('blob', 'digest')
            yield File('blob', 'digest')

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2)]))
        assert len(stream) > RECORD * 4
        assert len(stream) < RECORD * 5
        self.assertEqual(11, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5)]))
        assert len(stream) > RECORD * 5
        assert len(stream) < RECORD * 6
        self.assertEqual(13, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(15, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(15, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(15, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 5.5)]))
        assert len(stream) > RECORD * 7
        assert len(stream) < RECORD * 8
        self.assertEqual(17, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 6.5)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(19, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in packets.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            )]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(19, len(stream.strip().split('\n')))

    def test_decode_dir(self):
        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': '8'}) + '\n' +
            'content1' + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        self.touch(('packets/1.packet', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'num': 2, 'content-length': '8'}) + '\n' +
            'content2' + '\n' +
            json.dumps({'payload': 3}) + '\n'
            )
        self.touch(('packets/2.packet', stream.getvalue()))

        packets_iter = iter(packets.decode_dir('packets'))
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual({'segment': 2}, packet.header)
            items = iter(packet)
            blob = next(items)
            self.assertEqual({'num': 2, 'content-length': '8'}, blob.meta)
            self.assertEqual('content2', file(blob.path).read())
            self.assertEqual({'payload': 3}, next(items))
            self.assertRaises(StopIteration, items.next)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual({'foo': 'bar', 'segment': 1}, packet.header)
            items = iter(packet)
            self.assertEqual({'payload': 1}, next(items))
            blob = next(items)
            self.assertEqual({'num': 1, 'content-length': '8'}, blob.meta)
            self.assertEqual('content1', file(blob.path).read())
            self.assertEqual({'payload': 2}, next(items))
            self.assertRaises(StopIteration, items.next)
        self.assertRaises(StopIteration, packets_iter.next)

    def test_decode_dir_RemoveOutdatedPackets(self):
        stream = zips(
            json.dumps({'from': 'principal'}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        self.touch(('packets/1.packet', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        self.touch(('packets/2.packet', stream.getvalue()))

        packets_iter = iter(packets.decode_dir('packets', recipient='principal'))
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert not exists('packets/1.packet')
        assert exists('packets/2.packet')

        stream = zips(
            json.dumps({'from': 'principal', 'session': 'old'}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        self.touch(('packets/3.packet', stream.getvalue()))

        packets_iter = iter(packets.decode_dir('packets', recipient='principal', session='new'))
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert not exists('packets/1.packet')
        assert exists('packets/2.packet')
        assert not exists('packets/3.packet')

    def test_decode_dir_SkipTheSameSessionPackets(self):
        stream = zips(
            json.dumps({'from': 'principal', 'session': 'new'}) + '\n' +
            json.dumps({'segment': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        self.touch(('packets/1.packet', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'segment': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        self.touch(('packets/2.packet', stream.getvalue()))

        packets_iter = iter(packets.decode_dir('packets', recipient='principal', session='new'))
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert exists('packets/1.packet')
        assert exists('packets/2.packet')

    def test_encode_dir(self):
        self.touch(('blob', 'content'))
        packets.encode_dir([
            (1, None, [
                {'payload': 1},
                File('blob', 'digest', [('num', 1)]),
                {'payload': 2},
                ]),
            (2, None, [
                File('blob', 'digest', [('num', 2)]),
                {'payload': 3},
                ]),
            ], path='./packets', limit=99999999)

        assert exists('packets')

        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'segment': 1}) + '\n' +
                json.dumps({'payload': 1}) + '\n' +
                json.dumps({'num': 1}) + '\n' +
                'content' + '\n' +
                json.dumps({'payload': 2}) + '\n' +
                json.dumps({'segment': 2}) + '\n' +
                json.dumps({'num': 2}) + '\n' +
                'content' + '\n' +
                json.dumps({'payload': 3}) + '\n',
                unzips(file('packets').read()))

    def test_decode_WithoutSegments(self):
        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'n': 1}) + '\n' +
            json.dumps({'n': 2}) + '\n' +
            json.dumps({'n': 3}) + '\n'
            )
        packet = packets.decode(stream)
        self.assertEqual({'foo': 'bar'}, packet.header)
        self.assertEqual([{'n': 1}, {'n': 2}, {'n': 3}], [i for i in packet])

    def test_encode_WithoutSegments(self):
        stream = ''.join([i for i in packets.encode([{'n': 1}, {'n': 2}, {'n': 3}], header={'foo': 'bar'})])
        self.assertEqual(
                json.dumps({'foo': 'bar'}) + '\n' +
                json.dumps({'n': 1}) + '\n' +
                json.dumps({'n': 2}) + '\n' +
                json.dumps({'n': 3}) + '\n',
                unzips(stream))


def zips(data):
    result = StringIO()
    f = gzip.GzipFile(fileobj=result, mode='wb')
    f.write(data)
    f.close()
    result.seek(0)
    return result


def unzips(data):
    return gzip.GzipFile(fileobj=StringIO(data)).read()


if __name__ == '__main__':
    tests.main()
