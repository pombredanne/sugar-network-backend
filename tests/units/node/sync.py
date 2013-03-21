#!/usr/bin/env python
# sugar-lint: disable

import os
import uuid
import json
from StringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.node import sync
from sugar_network.toolkit import BUFFER_SIZE


class SyncTest(tests.Test):

    def test_decode(self):
        stream = StringIO()
        dump({'foo': 'bar'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        dump({'packet': 1, 'bar': 'foo'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual('foo', packet['bar'])
            packet_iter = iter(packet)
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        dump({'payload': 1}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        dump({'packet': 2}, stream)
        dump({'payload': 2}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
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

        dump({'packet': 'last'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
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

    def test_decode_Empty(self):
        stream = StringIO()
        self.assertRaises(EOFError, sync.decode(stream).next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream = StringIO()
        dump({'foo': 'bar'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        dump({'packet': 'last'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipPackets(self):
        stream = StringIO()
        dump({'packet': 1}, stream)
        dump({'payload': 1}, stream)
        dump({'payload': 11}, stream)
        dump({'payload': 111}, stream)
        dump({'packet': 2}, stream)
        dump({'payload': 2}, stream)
        dump({'packet': 'last'}, stream)

        stream.seek(0)
        packets_iter = sync.decode(stream)
        next(packets_iter)
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 2}, next(packet_iter))
            self.assertRaises(StopIteration, packet_iter.next)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        stream.seek(0)
        packets_iter = sync.decode(stream)
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

    def test_encode(self):
        self.assertEqual([
            dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode([])])

        self.assertEqual([
            dumps({'packet': None, 'foo': 'bar'}),
            dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode([(None, None, None)], foo='bar')])

        self.assertEqual([
            dumps({'packet': 1}),
            dumps({'packet': '2', 'n': 2}),
            dumps({'packet': '3', 'n': 3}),
            dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode([
                (1, {}, None),
                ('2', {'n': 2}, []),
                ('3', {'n': 3}, iter([])),
                ])])

        self.assertEqual([
            dumps({'packet': 1}),
            dumps({1: 1}),
            dumps({'packet': 2}),
            dumps({2: 2}),
            dumps({2: 2}),
            dumps({'packet': 3}),
            dumps({3: 3}),
            dumps({3: 3}),
            dumps({3: 3}),
            dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode([
                (1, None, [{1: 1}]),
                (2, None, [{2: 2}, {2: 2}]),
                (3, None, [{3: 3}, {3: 3}, {3: 3}]),
                ])])

    def test_limited_encode(self):
        header_size = len(dumps({'packet': 'first'}))
        record_size = len(dumps({'record': 0}))

        def content():
            yield {'record': 1}
            yield {'record': 2}
            yield {'record': 3}

        i = sync.limited_encode(header_size + record_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'record': 1}, json.loads(i.send(header_size)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + record_size)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + record_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + 1)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + record_size * 2, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'record': 1}, json.loads(i.send(header_size)))
        self.assertEqual({'record': 2}, json.loads(i.send(header_size + record_size)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + record_size * 2)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + record_size * 2, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'record': 1}, json.loads(i.send(header_size)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + record_size + 1)))
        self.assertRaises(StopIteration, i.next)

    def test_limited_encode_FinalRecords(self):
        header_size = len(dumps({'packet': 'first'}))
        record_size = len(dumps({'record': 0}))

        def content():
            try:
                yield {'record': 1}
                yield {'record': 2}
                yield {'record': 3}
            except StopIteration:
                pass
            yield {'record': 4}
            yield {'record': 5}

        i = sync.limited_encode(header_size + record_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'record': 4}, json.loads(i.send(header_size + 1)))
        self.assertEqual({'record': 5}, json.loads(i.send(999999999)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(999999999)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + record_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'record': 1}, json.loads(i.send(header_size)))
        self.assertEqual({'record': 4}, json.loads(i.send(header_size + record_size * 2 - 1)))
        self.assertEqual({'record': 5}, json.loads(i.send(999999999)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(999999999)))
        self.assertRaises(StopIteration, i.next)

    def test_limited_encode_Blobs(self):
        header_size = len(dumps({'packet': 'first'}))
        blob_header_size = len(dumps({'blob_size': 100}))
        record_size = len(dumps({'record': 2}))

        def content():
            yield {'blob_size': 100, 'blob': ['*' * 100]}
            yield {'record': 2}
            yield {'record': 3}

        i = sync.limited_encode(header_size + blob_header_size + 99, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + blob_header_size + 100, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'blob_size': 100}, json.loads(i.send(header_size)))
        self.assertEqual('*' * 100, i.send(header_size + blob_header_size))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + blob_header_size + 100)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + blob_header_size + 100 + record_size - 1, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'blob_size': 100}, json.loads(i.send(header_size)))
        self.assertEqual('*' * 100, i.send(header_size + blob_header_size))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + blob_header_size + 100)))
        self.assertRaises(StopIteration, i.next)

        i = sync.limited_encode(header_size + blob_header_size + 100 + record_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'blob_size': 100}, json.loads(i.send(header_size)))
        self.assertEqual('*' * 100, i.send(header_size + blob_header_size))
        self.assertEqual({'record': 2}, json.loads(i.send(header_size + blob_header_size + 100)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(header_size + blob_header_size + 100 + record_size)))
        self.assertRaises(StopIteration, i.next)

    def test_limited_encode_FinalBlobs(self):
        header_size = len(dumps({'packet': 'first'}))
        blob_header_size = len(dumps({'blob_size': 100}))
        record_size = len(dumps({'record': 2}))

        def content():
            try:
                yield {'record': 1}
            except StopIteration:
                pass
            yield {'blob_size': 100, 'blob': ['*' * 100]}
            yield {'record': 3}

        i = sync.limited_encode(header_size, [('first', None, content()), ('second', None, content())])
        self.assertEqual({'packet': 'first'}, json.loads(i.send(None)))
        self.assertEqual({'blob_size': 100}, json.loads(i.send(header_size)))
        self.assertEqual('*' * 100, i.send(999999999))
        self.assertEqual({'record': 3}, json.loads(i.send(999999999)))
        self.assertEqual({'packet': 'last'}, json.loads(i.send(999999999)))
        self.assertRaises(StopIteration, i.next)

    def test_chunked_encode(self):
        output = sync.chunked_encode([])
        self.assertEqual({'packet': 'last'}, json.loads(decode_chunked(output.read(100))))

        data = [{'foo': 1}, {'bar': 2}]
        data_stream = dumps({'packet': 'packet'})
        for record in data:
            data_stream += dumps(record)
        data_stream += dumps({'packet': 'last'})

        output = sync.chunked_encode([('packet', None, iter(data))])
        pauload = StringIO()
        while True:
            chunk = output.read(1)
            if not chunk:
                break
            pauload.write(chunk)
        self.assertEqual(data_stream, decode_chunked(pauload.getvalue()))

        output = sync.chunked_encode([('packet', None, iter(data))])
        pauload = StringIO()
        while True:
            chunk = output.read(2)
            if not chunk:
                break
            pauload.write(chunk)
        self.assertEqual(data_stream, decode_chunked(pauload.getvalue()))

        output = sync.chunked_encode([('packet', None, iter(data))])
        pauload = StringIO()
        while True:
            chunk = output.read(1000)
            if not chunk:
                break
            pauload.write(chunk)
        self.assertEqual(data_stream, decode_chunked(pauload.getvalue()))

    def test_encode_Blobs(self):
        self.assertEqual([
            dumps({'packet': 1}),
            dumps({'num': 1, 'blob_size': 1}),
            'a',
            dumps({'num': 2, 'blob_size': 2}),
            'bb',
            dumps({'packet': 2}),
            dumps({'num': 3, 'blob_size': 3}),
            'ccc',
            dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode([
                (1, None, [{'num': 1, 'blob_size': 1, 'blob': ['a']}, {'num': 2, 'blob_size': 2, 'blob': ['bb']}]),
                (2, None, [{'num': 3, 'blob_size': 3, 'blob': ['ccc']}]),
                ])])

    def test_decode_Blobs(self):
        stream = StringIO()
        dump({'packet': 1}, stream)
        dump({'num': 1, 'blob_size': 1}, stream)
        stream.write('a')
        dump({'num': 2, 'blob_size': 2}, stream)
        stream.write('bb')
        dump({'packet': 2}, stream)
        dump({'num': 3, 'blob_size': 3}, stream)
        stream.write('ccc')
        dump({'packet': 'last'}, stream)
        stream.seek(0)

        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([
                (1, 1, 'a'),
                (2, 2, 'bb'),
                ],
                [(i['num'], i['blob_size'], i['blob'].read()) for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([
                (3, 3, 'ccc'),
                ],
                [(i['num'], i['blob_size'], i['blob'].read()) for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipNotReadBlobs(self):
        stream = StringIO()
        dump({'packet': 1}, stream)
        dump({'num': 1, 'blob_size': 1}, stream)
        stream.write('a')
        dump({'num': 2, 'blob_size': 2}, stream)
        stream.write('bb')
        dump({'packet': 2}, stream)
        dump({'num': 3, 'blob_size': 3}, stream)
        stream.write('ccc')
        dump({'packet': 'last'}, stream)
        stream.seek(0)

        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([1, 2], [i['num'] for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([3], [i['num'] for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipNotReadBlobsForNotSeekableStreams(self):

        class Stream(object):

            def __init__(self):
                self.value = StringIO()

            def read(self, size):
                return self.value.read(size)

        stream = Stream()
        dump({'packet': 1}, stream.value)
        dump({'num': 1, 'blob_size': 1}, stream.value)
        stream.value.write('a')
        dump({'num': 2, 'blob_size': 2}, stream.value)
        stream.value.write('bb')
        dump({'packet': 2}, stream.value)
        dump({'num': 3, 'blob_size': 3}, stream.value)
        stream.value.write('ccc')
        dump({'packet': 'last'}, stream.value)
        stream.value.seek(0)

        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual([1, 2], [i['num'] for i in packet])
        with next(packets_iter) as packet:
            self.assertEqual(2, packet.name)
            self.assertEqual([3], [i['num'] for i in packet])
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.value.getvalue()), stream.value.tell())

    def test_sneakernet_decode(self):
        self.override(toolkit, 'uuid', lambda: 'uuid')

        sync.sneakernet_encode([
            ('first', {'packet_prop': 1}, [
                {'record': 1},
                {'record': 2},
                ]),
            ('second', {'packet_prop': 2}, [
                {'record': 3},
                {'record': 4},
                ]),
            ],
            root='.', package_prop=1, limit=999999999)
        sync.sneakernet_encode([
            ('third', {'packet_prop': 3}, [
                {'record': 5},
                {'record': 6},
                ]),
            ],
            root='.', package_prop=2, limit=999999999)

        self.assertEqual([
            ({'packet_prop': 1, 'package_prop': 1, 'packet': 'first', 'filename': 'uuid.sneakernet'}, [{'record': 1}, {'record': 2}]),
            ({'packet_prop': 2, 'package_prop': 1, 'packet': 'second', 'filename': 'uuid.sneakernet'}, [{'record': 3}, {'record': 4}]),
            ({'packet_prop': 3, 'package_prop': 2, 'packet': 'third', 'filename': 'uuid.sneakernet'}, [{'record': 5}, {'record': 6}]),
            ],
            sorted([(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('.')]))

    def test_sneakernet_decode_CleanupOutdatedFiles(self):
        sync.sneakernet_encode([('first', None, None)], path='.sneakernet', src='node', session='session', limit=999999999)

        self.assertEqual(1, len([i for i in sync.sneakernet_decode('.')]))
        assert exists('.sneakernet')

        self.assertEqual(1, len([i for i in sync.sneakernet_decode('.', node='foo')]))
        assert exists('.sneakernet')

        self.assertEqual(0, len([i for i in sync.sneakernet_decode('.', node='node', session='session')]))
        assert exists('.sneakernet')

        self.assertEqual(0, len([i for i in sync.sneakernet_decode('.', node='node', session='session2')]))
        assert not exists('.sneakernet')

    def test_sneakernet_encode(self):
        self.override(toolkit, 'uuid', lambda: 'uuid')
        payload = ''.join([str(uuid.uuid4()) for i in xrange(5000)])

        def content():
            yield {'record': payload}
            yield {'record': payload}

        class statvfs(object):
            f_bfree = None
            f_frsize = 1
        self.override(os, 'statvfs', lambda *args: statvfs())

        statvfs.f_bfree = sync._SNEAKERNET_RESERVED_SIZE
        self.assertEqual(False, sync.sneakernet_encode([('first', None, content())], root='1'))
        self.assertEqual(
                [({'packet': 'first', 'filename': 'uuid.sneakernet'}, [])],
                [(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('1')])

        statvfs.f_bfree += len(payload) + len(payload) / 2
        self.assertEqual(False, sync.sneakernet_encode([('first', None, content())], root='2'))
        self.assertEqual(
                [({'packet': 'first', 'filename': 'uuid.sneakernet'}, [{'record': payload}])],
                [(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('2')])

        statvfs.f_bfree += len(payload)
        self.assertEqual(True, sync.sneakernet_encode([('first', None, content())], root='3'))
        self.assertEqual(
                [({'packet': 'first', 'filename': 'uuid.sneakernet'}, [{'record': payload}, {'record': payload}])],
                [(packet.props, [i for i in packet]) for packet in sync.sneakernet_decode('3')])


def decode_chunked(encdata):
    offset = 0
    newdata = ''
    while (encdata != ''):
        off = int(encdata[:encdata.index("\r\n")],16)
        if off == 0:
            break
        encdata = encdata[encdata.index("\r\n") + 2:]
        newdata = "%s%s" % (newdata, encdata[:off])
        encdata = encdata[off+2:]
    return newdata


def dump(value, stream):
    stream.write(json.dumps(value))
    stream.write('\n')


def dumps(value):
    return json.dumps(value) + '\n'


if __name__ == '__main__':
    tests.main()
