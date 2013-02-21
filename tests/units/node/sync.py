#!/usr/bin/env python
# sugar-lint: disable

import os
from os.path import exists
from cStringIO import StringIO
import cPickle as pickle

from __init__ import tests

from sugar_network import db
from sugar_network.node import sync
from sugar_network.resources.volume import Volume
from sugar_network.resources.user import User
from sugar_network.toolkit.router import Request
from sugar_network.toolkit import util


class SyncTest(tests.Test):

    def test_diff(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='2', seqno=2, prop='b')
        for i in os.listdir('db/document/2/2'):
            os.utime('db/document/2/2/%s' % i, (2, 2))

        in_seq = util.Sequence([[1, None]])
        self.assertEqual([
            {'document': 'document'},
            {'guid': '1',
                'diff': {
                    'guid': {'value': '1', 'mtime': 1},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'value': 'a', 'mtime': 1},
                    },
                },
            {'guid': '2',
                'diff': {
                    'guid': {'value': '2', 'mtime': 2},
                    'mtime': {'value': 0, 'mtime': 2},
                    'ctime': {'value': 0, 'mtime': 2},
                    'prop': {'value': 'b', 'mtime': 2},
                    },
                },
            {'commit': [[1, 2]]},
            ],
            [i for i in sync.diff(volume, in_seq)])
        self.assertEqual([[1, None]], in_seq)

    def test_diff_Partial(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='2', seqno=2, prop='b')
        for i in os.listdir('db/document/2/2'):
            os.utime('db/document/2/2/%s' % i, (2, 2))

        in_seq = util.Sequence([[1, None]])
        diff = sync.diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, diff.send(None))
        self.assertEqual('1', diff.send(None)['guid'])
        self.assertEqual({'commit': []}, diff.send(sync._EOF))
        try:
            diff.send(None)
            assert False
        except StopIteration:
            pass

        diff = sync.diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, diff.send(None))
        self.assertEqual('1', diff.send(None)['guid'])
        self.assertEqual('2', diff.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, diff.send(sync._EOF))
        try:
            diff.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_Collapsed(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        volume['document'].create(guid='1', seqno=1, prop='a')
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (1, 1))
        volume['document'].create(guid='3', seqno=3, prop='c')
        for i in os.listdir('db/document/3/3'):
            os.utime('db/document/3/3/%s' % i, (3, 3))
        volume['document'].create(guid='5', seqno=5, prop='f')
        for i in os.listdir('db/document/5/5'):
            os.utime('db/document/5/5/%s' % i, (5, 5))

        in_seq = util.Sequence([[1, None]])
        diff = sync.diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, diff.send(None))
        self.assertEqual('1', diff.send(None)['guid'])
        self.assertEqual('3', diff.send(None)['guid'])
        self.assertEqual('5', diff.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3]]}, diff.send(sync._EOF))
        try:
            diff.send(None)
            assert False
        except StopIteration:
            pass

        diff = sync.diff(volume, in_seq)
        self.assertEqual({'document': 'document'}, diff.send(None))
        self.assertEqual('1', diff.send(None)['guid'])
        self.assertEqual('3', diff.send(None)['guid'])
        self.assertEqual('5', diff.send(None)['guid'])
        self.assertEqual({'commit': [[1, 5]]}, diff.send(None))
        try:
            diff.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_TheSameInSeqForAllDocuments(self):

        class Document1(db.Document):
            pass

        class Document2(db.Document):
            pass

        class Document3(db.Document):
            pass

        volume = Volume('db', [Document1, Document2, Document3])
        volume['document1'].create(guid='3', seqno=3)
        for i in os.listdir('db/document1/3/3'):
            os.utime('db/document1/3/3/%s' % i, (3, 3))
        volume['document2'].create(guid='2', seqno=2)
        for i in os.listdir('db/document2/2/2'):
            os.utime('db/document2/2/2/%s' % i, (2, 2))
        volume['document3'].create(guid='1', seqno=1)
        for i in os.listdir('db/document3/1/1'):
            os.utime('db/document3/1/1/%s' % i, (1, 1))

        in_seq = util.Sequence([[1, None]])
        diff = sync.diff(volume, in_seq)
        self.assertEqual({'document': 'document1'}, diff.send(None))
        self.assertEqual('3', diff.send(None)['guid'])
        self.assertEqual({'document': 'document2'}, diff.send(None))
        self.assertEqual('2', diff.send(None)['guid'])
        self.assertEqual({'document': 'document3'}, diff.send(None))
        self.assertEqual('1', diff.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, diff.send(None))
        try:
            diff.send(None)
            assert False
        except StopIteration:
            pass

    def test_merge_Create(self):

        class Document1(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        class Document2(db.Document):
            pass

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document1, Document2])

        records = [
                {'document': 'document1'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1.0},
                    'ctime': {'value': 2, 'mtime': 2.0},
                    'mtime': {'value': 3, 'mtime': 3.0},
                    'prop': {'value': '4', 'mtime': 4.0},
                    }},
                {'document': 'document2'},
                {'guid': '5', 'diff': {
                    'guid': {'value': '5', 'mtime': 5.0},
                    'ctime': {'value': 6, 'mtime': 6.0},
                    'mtime': {'value': 7, 'mtime': 7.0},
                    }},
                {'commit': [[1, 2]]},
                ]
        self.assertEqual(([[1, 2]], [[101, 102]]), sync.merge(volume, records))

        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 2, 'mtime': 3},
                volume['document1'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(1, os.stat('db/document1/1/1/guid').st_mtime)
        self.assertEqual(2, os.stat('db/document1/1/1/ctime').st_mtime)
        self.assertEqual(3, os.stat('db/document1/1/1/mtime').st_mtime)
        self.assertEqual(4, os.stat('db/document1/1/1/prop').st_mtime)

        self.assertEqual(
                {'guid': '5', 'ctime': 6, 'mtime': 7},
                volume['document2'].get('5').properties(['guid', 'ctime', 'mtime']))
        self.assertEqual(5, os.stat('db/document2/5/5/guid').st_mtime)
        self.assertEqual(6, os.stat('db/document2/5/5/ctime').st_mtime)
        self.assertEqual(7, os.stat('db/document2/5/5/mtime').st_mtime)

    def test_merge_Update(self):

        class Document(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document])
        volume['document'].create(guid='1', prop='1', ctime=1, mtime=1)
        for i in os.listdir('db/document/1/1'):
            os.utime('db/document/1/1/%s' % i, (2, 2))

        records = [
                {'document': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '2', 'mtime': 1.0}}},
                {'commit': [[1, 1]]},
                ]
        self.assertEqual(([[1, 1]], []), sync.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
                {'document': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '3', 'mtime': 2.0}}},
                {'commit': [[2, 2]]},
                ]
        self.assertEqual(([[2, 2]], []), sync.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('db/document/1/1/prop').st_mtime)

        records = [
            {'document': 'document'},
            {'guid': '1', 'diff': {'prop': {'value': '4', 'mtime': 3.0}}},
            {'commit': [[3, 3]]},
            ]
        self.assertEqual(([[3, 3]], [[102, 102]]), sync.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(3, os.stat('db/document/1/1/prop').st_mtime)

    def test_merge_MultipleCommits(self):

        class Document(db.Document):
            pass

        self.touch(('db/seqno', '100'))
        volume = Volume('db', [Document])

        def generator():
            for i in [
                    {'document': 'document'},
                    {'commit': [[1, 1]]},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1.0},
                        'ctime': {'value': 2, 'mtime': 2.0},
                        'mtime': {'value': 3, 'mtime': 3.0},
                        'prop': {'value': '4', 'mtime': 4.0},
                        }},
                    {'commit': [[2, 3]]},
                    ]:
                yield i

        records = generator()
        self.assertEqual(([[1, 3]], [[101, 101]]), sync.merge(volume, records))
        assert volume['document'].exists('1')

    def test_decode(self):
        stream = StringIO()
        pickle.dump({'foo': 'bar'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        pickle.dump({'packet': 1, 'bar': 'foo'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            self.assertEqual('foo', packet['bar'])
            packet_iter = iter(packet)
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        pickle.dump({'payload': 1}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        with next(packets_iter) as packet:
            self.assertEqual(1, packet.name)
            packet_iter = iter(packet)
            self.assertEqual({'payload': 1}, next(packet_iter))
            self.assertRaises(EOFError, packet_iter.next)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        pickle.dump({'packet': 2}, stream)
        pickle.dump({'payload': 2}, stream)
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

        pickle.dump({'packet': 'last'}, stream)
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
        pickle.dump({'foo': 'bar'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(EOFError, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

        pickle.dump({'packet': 'last'}, stream)
        stream.seek(0)
        packets_iter = sync.decode(stream)
        self.assertRaises(StopIteration, packets_iter.next)
        self.assertEqual(len(stream.getvalue()), stream.tell())

    def test_decode_SkipPackets(self):
        stream = StringIO()
        pickle.dump({'packet': 1}, stream)
        pickle.dump({'payload': 1}, stream)
        pickle.dump({'payload': 11}, stream)
        pickle.dump({'payload': 111}, stream)
        pickle.dump({'packet': 2}, stream)
        pickle.dump({'payload': 2}, stream)
        pickle.dump({'packet': 'last'}, stream)

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
            pickle.dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode()])

        self.assertEqual([
            pickle.dumps({'packet': None}),
            pickle.dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode(
                (None, None, None)
                )])

        self.assertEqual([
            pickle.dumps({'packet': 1}),
            pickle.dumps({'packet': '2', 'n': 2}),
            pickle.dumps({'packet': '3', 'n': 3}),
            pickle.dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode(
                (1, {}, None),
                ('2', {'n': 2}, []),
                ('3', {'n': 3}, iter([])),
                )])

        self.assertEqual([
            pickle.dumps({'packet': 1}),
            pickle.dumps(1),
            pickle.dumps({'packet': 2}),
            pickle.dumps(2),
            pickle.dumps(2),
            pickle.dumps({'packet': 3}),
            pickle.dumps(3),
            pickle.dumps(3),
            pickle.dumps(3),
            pickle.dumps({'packet': 'last'}),
            ],
            [i for i in sync.encode(
                (1, None, [1]),
                (2, None, [2, 2]),
                (3, None, [3, 3, 3]),
                )])

    def test_chunked_encode(self):
        output = sync.chunked_encode()
        self.assertEqual({'packet': 'last'}, pickle.loads(decode_chunked(output.read(100))))

        data = [{'foo': 1}, {'bar': 2}, 3]
        data_stream = pickle.dumps({'packet': 'packet'})
        for record in data:
            data_stream += pickle.dumps(record)
        data_stream += pickle.dumps({'packet': 'last'})

        output = sync.chunked_encode(('packet', None, iter(data)))
        dump = StringIO()
        while True:
            chunk = output.read(1)
            if not chunk:
                break
            dump.write(chunk)
        self.assertEqual(data_stream, decode_chunked(dump.getvalue()))

        output = sync.chunked_encode(('packet', None, iter(data)))
        dump = StringIO()
        while True:
            chunk = output.read(2)
            if not chunk:
                break
            dump.write(chunk)
        self.assertEqual(data_stream, decode_chunked(dump.getvalue()))

        output = sync.chunked_encode(('packet', None, iter(data)))
        dump = StringIO()
        while True:
            chunk = output.read(1000)
            if not chunk:
                break
            dump.write(chunk)
        self.assertEqual(data_stream, decode_chunked(dump.getvalue()))


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


if __name__ == '__main__':
    tests.main()
