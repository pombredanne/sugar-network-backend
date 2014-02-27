#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import uuid
import json
from StringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.toolkit.router import File
from sugar_network.toolkit import parcel, http


class ParcelTest(tests.Test):

    def test_decode(self):
        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 1, 'bar': 'foo'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual('foo', packet['bar'])
            packet_iter = iter(packet)
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 1, 'bar': 'foo'}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
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

    def test_decode_WithLimit(self):
        payload = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 'first'}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            ).getvalue()
        tail = '.' * 100

        stream = StringIO(payload + tail)
        for i in parcel.decode(stream):
            pass
        self.assertEqual(len(payload + tail), stream.tell())

        stream = StringIO(payload + tail)
        for i in parcel.decode(stream, limit=len(payload)):
            pass
        self.assertEqual(len(payload), stream.tell())

    def test_decode_Empty(self):
        self.assertRaises(http.BadRequest, parcel.decode(StringIO()).next)

        stream = zips(
            ''
            )
        self.assertRaises(EOFError, parcel.decode(stream).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n'
            )
        self.assertRaises(EOFError, parcel.decode(stream).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.assertRaises(StopIteration, parcel.decode(stream).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipPackets(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'payload': 11}) + '\n' +
            json.dumps({'payload': 111}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        next(packets_iter)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream.seek(0)
        packets_iter = parcel.decode(stream)
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
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 2}) + '\n' +
            'bb' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([
                (1, 'a'),
                (2, 'bb'),
                ],
                [(i['num'], file(i.path).read()) for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([
                (3, 'ccc'),
                ],
                [(i['num'], file(i.path).read()) for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_EmptyBlobs(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 0}) + '\n' +
            '' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([
                (1, 'a'),
                (2, ''),
                ],
                [(i['num'], file(i.path).read()) for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([
                (3, 'ccc'),
                ],
                [(i['num'], file(i.path).read()) for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipNotReadBlobs(self):
        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': 1}) + '\n' +
            'a' +
            json.dumps({'num': 2, 'content-length': 2}) + '\n' +
            'bb' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'num': 3, 'content-length': 3}) + '\n' +
            'ccc' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        packets_iter = parcel.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([1, 2], [i['num'] for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([3], [i['num'] for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_encode(self):
        stream = ''.join([i for i in parcel.encode([])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'packet': 'last'}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in parcel.encode([(None, None, None)], header={'foo': 'bar'})])
        self.assertEqual(
                json.dumps({'foo': 'bar'}) + '\n' +
                json.dumps({'packet': None}) + '\n' +
                json.dumps({'packet': 'last'}) + '\n',
                unzips(stream))

        stream = ''.join([i for i in parcel.encode([
            (1, {}, None),
            ('2', {'n': 2}, []),
            ('3', {'n': 3}, iter([])),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'packet': 1}) + '\n' +
                json.dumps({'packet': '2', 'n': 2}) + '\n' +
                json.dumps({'packet': '3', 'n': 3}) + '\n' +
                json.dumps({'packet': 'last'})  + '\n',
                unzips(stream))

        stream = ''.join([i for i in parcel.encode([
            (1, None, [{1: 1}]),
            (2, None, [{2: 2}, {2: 2}]),
            (3, None, [{3: 3}, {3: 3}, {3: 3}]),
            ])])
        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'packet': 1}) + '\n' +
                json.dumps({1: 1}) + '\n' +
                json.dumps({'packet': 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({2: 2}) + '\n' +
                json.dumps({'packet': 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({3: 3}) + '\n' +
                json.dumps({'packet': 'last'}) + '\n',
                unzips(stream))

    def test_limited_encode(self):
        RECORD = 1024 * 1024

        def content():
            yield {'record': '.' * RECORD}
            yield {'record': '.' * RECORD}

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2, compresslevel=0)]))
        assert len(stream) < RECORD
        self.assertEqual(4, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5, compresslevel=0)]))
        assert len(stream) > RECORD
        assert len(stream) < RECORD * 2
        self.assertEqual(5, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5, compresslevel=0)]))
        assert len(stream) > RECORD * 2
        assert len(stream) < RECORD * 3
        self.assertEqual(6, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5, compresslevel=0)]))
        assert len(stream) > RECORD * 3
        assert len(stream) < RECORD * 4
        self.assertEqual(7, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5, compresslevel=0)]))
        assert len(stream) > RECORD * 4
        self.assertEqual(8, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            compresslevel=0)]))
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

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2, compresslevel=0)]))
        assert len(stream) > RECORD * 4
        assert len(stream) < RECORD * 5
        self.assertEqual(8, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5, compresslevel=0)]))
        assert len(stream) > RECORD * 5
        assert len(stream) < RECORD * 6
        self.assertEqual(9, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(10, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(10, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(10, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 5.5, compresslevel=0)]))
        assert len(stream) > RECORD * 7
        assert len(stream) < RECORD * 8
        self.assertEqual(11, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 6.5, compresslevel=0)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(12, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            compresslevel=0)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(12, len(stream.strip().split('\n')))

    def test_encode_Blobs(self):
        self.touch(('a', 'a'))
        self.touch(('b', 'bb'))
        self.touch(('c', 'ccc'))

        stream = ''.join([i for i in parcel.encode([
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
                json.dumps({'packet': 1}) + '\n' +
                json.dumps({'num': 1}) + '\n' +
                'a' + '\n' +
                json.dumps({'num': 2}) + '\n' +
                'bb' + '\n' +
                json.dumps({'packet': 2}) + '\n' +
                json.dumps({'num': 3}) + '\n' +
                'ccc' + '\n' +
                json.dumps({'packet': 'last'}) + '\n',
                unzips(stream))

    def test_limited_encode_Blobs(self):
        RECORD = 1024 * 1024
        self.touch(('blob', '.' * RECORD))

        def content():
            yield File('blob', 'digest')
            yield File('blob', 'digest')

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2, compresslevel=0)]))
        assert len(stream) < RECORD
        self.assertEqual(4, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5, compresslevel=0)]))
        assert len(stream) > RECORD
        assert len(stream) < RECORD * 2
        self.assertEqual(6, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5, compresslevel=0)]))
        assert len(stream) > RECORD * 2
        assert len(stream) < RECORD * 3
        self.assertEqual(8, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5, compresslevel=0)]))
        assert len(stream) > RECORD * 3
        assert len(stream) < RECORD * 4
        self.assertEqual(10, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5, compresslevel=0)]))
        assert len(stream) > RECORD * 4
        self.assertEqual(12, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            compresslevel=0)]))
        assert len(stream) > RECORD * 4
        self.assertEqual(12, len(stream.strip().split('\n')))

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

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD / 2, compresslevel=0)]))
        assert len(stream) > RECORD * 4
        assert len(stream) < RECORD * 5
        self.assertEqual(12, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 1.5, compresslevel=0)]))
        assert len(stream) > RECORD * 5
        assert len(stream) < RECORD * 6
        self.assertEqual(14, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 2.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(16, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 3.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(16, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 4.5, compresslevel=0)]))
        assert len(stream) > RECORD * 6
        assert len(stream) < RECORD * 7
        self.assertEqual(16, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 5.5, compresslevel=0)]))
        assert len(stream) > RECORD * 7
        assert len(stream) < RECORD * 8
        self.assertEqual(18, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            limit=RECORD * 6.5, compresslevel=0)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(20, len(stream.strip().split('\n')))

        stream = unzips(''.join([i for i in parcel.encode([
            ('first', None, content()),
            ('second', None, content()),
            ],
            compresslevel=0)]))
        assert len(stream) > RECORD * 8
        assert len(stream) < RECORD * 9
        self.assertEqual(20, len(stream.strip().split('\n')))

    def test_decode_dir(self):
        stream = zips(
            json.dumps({'foo': 'bar'}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'num': 1, 'content-length': '8'}) + '\n' +
            'content1' + '\n' +
            json.dumps({'payload': 2}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/1.parcel', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'num': 2, 'content-length': '8'}) + '\n' +
            'content2' + '\n' +
            json.dumps({'payload': 3}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/2.parcel', stream.getvalue()))

        packets_iter = parcel.decode_dir('parcels')
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual({'packet': 2}, packet.props)
            items = iter(packet)
            blob = next(items)
            self.assertEqual({'num': 2, 'content-length': '8'}, blob)
            self.assertEqual('content2', file(blob.path).read())
            self.assertEqual({'payload': 3}, next(items))
            self.assertRaises(StopIteration, items.next)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual({'foo': 'bar', 'packet': 1}, packet.props)
            items = iter(packet)
            self.assertEqual({'payload': 1}, next(items))
            blob = next(items)
            self.assertEqual({'num': 1, 'content-length': '8'}, blob)
            self.assertEqual('content1', file(blob.path).read())
            self.assertEqual({'payload': 2}, next(items))
            self.assertRaises(StopIteration, items.next)
        self.assertRaises(StopIteration, packets_iter.next)

    def test_decode_dir_RemoveOutdatedParcels(self):
        stream = zips(
            json.dumps({'from': 'principal'}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/1.parcel', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/2.parcel', stream.getvalue()))

        packets_iter = parcel.decode_dir('parcels', recipient='principal')
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert not exists('parcels/1.parcel')
        assert exists('parcels/2.parcel')

        stream = zips(
            json.dumps({'from': 'principal', 'session': 'old'}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/3.parcel', stream.getvalue()))

        packets_iter = parcel.decode_dir('parcels', recipient='principal', session='new')
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert not exists('parcels/1.parcel')
        assert exists('parcels/2.parcel')
        assert not exists('parcels/3.parcel')

    def test_decode_dir_SkipTheSameSessionParcels(self):
        stream = zips(
            json.dumps({'from': 'principal', 'session': 'new'}) + '\n' +
            json.dumps({'packet': 1}) + '\n' +
            json.dumps({'payload': 1}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/1.parcel', stream.getvalue()))

        stream = zips(
            json.dumps({}) + '\n' +
            json.dumps({'packet': 2}) + '\n' +
            json.dumps({'payload': 2}) + '\n' +
            json.dumps({'packet': 'last'}) + '\n'
            )
        self.touch(('parcels/2.parcel', stream.getvalue()))

        packets_iter = parcel.decode_dir('parcels', recipient='principal', session='new')
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
        self.assertRaises(StopIteration, packets_iter.next)
        assert exists('parcels/1.parcel')
        assert exists('parcels/2.parcel')

    def test_encode_dir(self):
        self.touch(('blob', 'content'))
        parcel.encode_dir([
            (1, None, [
                {'payload': 1},
                File('blob', 'digest', [('num', 1)]),
                {'payload': 2},
                ]),
            (2, None, [
                File('blob', 'digest', [('num', 2)]),
                {'payload': 3},
                ]),
            ], path='./parcel')

        assert exists('parcel')

        self.assertEqual(
                json.dumps({}) + '\n' +
                json.dumps({'packet': 1}) + '\n' +
                json.dumps({'payload': 1}) + '\n' +
                json.dumps({'num': 1}) + '\n' +
                'content' + '\n' +
                json.dumps({'payload': 2}) + '\n' +
                json.dumps({'packet': 2}) + '\n' +
                json.dumps({'num': 2}) + '\n' +
                'content' + '\n' +
                json.dumps({'payload': 3}) + '\n' +
                json.dumps({'packet': 'last'}) + '\n',
                unzips(file('parcel').read()))


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
