#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import tarfile
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network.toolkit import sneakernet
from sugar_network.toolkit.sneakernet import InPacket, OutPacket, DiskFull


class SneakernetTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.override(os, 'statvfs', lambda *args: statvfs())
        statvfs.f_bfree = 1024 * 1024

    def test_InPacket_Empty(self):
        self.touch('file')
        self.assertRaises(RuntimeError, InPacket, 'file')

        tarball = tarfile.open('tarball', 'w:gz')
        tarball.close()
        self.assertRaises(RuntimeError, InPacket, 'tarball')

        tarball = tarfile.open('tarball', 'w:gz')
        tarball.add('file')
        tarball.close()
        self.assertRaises(RuntimeError, InPacket, 'tarball')

        self.touch(('header', '{}'))
        tarball = tarfile.open('tarball', 'w:gz')
        tarball.add('header')
        tarball.close()
        InPacket('tarball')

    def test_InPacket_Header(self):
        self.touch(('header', json.dumps({'a': 2, 'b': '4', 'c': None})))
        tarball = tarfile.open('tarball', 'w:gz')
        tarball.add('header')
        tarball.close()

        with InPacket('tarball') as packet:
            self.assertEqual(2, packet.header['a'])
            self.assertEqual('4', packet.header['b'])
            self.assertEqual(None, packet.header['c'])

    def test_InPacket_Content(self):
        self.touch(('header', '{}'))
        self.touch(('record_1', [
            json.dumps({'a': 1, 'b': '2', 'c': None}),
            json.dumps({'d': 3, 'e': '4', 'f': None}),
            ]))
        self.touch(('record_1.record', json.dumps({'content_type': 'records', 'num': 1})))
        self.touch(('record_2', 'blob'))
        self.touch(('record_2.record', json.dumps({'content_type': 'blob', 'num': 2})))
        self.touch(('record_3', json.dumps({'probe': None})))
        self.touch(('record_4.record', json.dumps({'content_type': 'foo', 'num': 4})))
        self.touch(('record_5.record', json.dumps({'num': 5})))

        tarball = tarfile.open('tarball', 'w:gz')
        tarball.add('header')
        tarball.add('record_1')
        tarball.add('record_1.record')
        tarball.add('record_2')
        tarball.add('record_2.record')
        tarball.add('record_3')
        tarball.add('record_4.record')
        tarball.add('record_5.record')
        tarball.close()

        packet = InPacket('tarball')

        records = []
        for i in packet:
            if i.get('content_type') == 'blob':
                i['blob'] = i['blob'].read()
            records.append(i)

        self.assertEqual([
            {'content_type': 'records', 'num': 1, 'a': 1, 'b': '2',  'c': None},
            {'content_type': 'records', 'num': 1, 'd': 3, 'e': '4',  'f': None},
            {'content_type': 'blob', 'num': 2, 'blob': 'blob'},
            {'content_type': 'foo', 'num': 4},
            {'num': 5},
            ], records)

    def test_InPacket_SaveStream(self):

        class Stream(object):

            def __init__(self_):
                self.touch(('header', '{"probe": "ok"}'))
                tarball = tarfile.open('tarball', 'w:gz')
                tarball.add('header')
                tarball.close()
                self_.data = file('tarball', 'rb').read()

            def read(self, size):
                data = self.data
                self.data = None
                return data

        packet = InPacket(stream=Stream())
        self.assertEqual('ok', packet.header['probe'])

    def test_InPacket_FilterRecords(self):
        with OutPacket(root='.') as packet:
            packet.push(a=1, b=1)
            packet.push(a=2, b=2)
            packet.push(a=1, b=3)

        self.assertEqual([
            {'a': 1, 'b': 1},
            {'a': 2, 'b': 2},
            {'a': 1, 'b': 3},
            ],
            [i for i in InPacket(packet.path).records()])

        self.assertEqual([
            {'a': 1, 'b': 1},
            {'a': 1, 'b': 3},
            ],
            [i for i in InPacket(packet.path).records(a=1)])

        self.assertEqual([
            {'a': 1, 'b': 3},
            ],
            [i for i in InPacket(packet.path).records(a=1, b=3)])

        self.assertEqual([
            ],
            [i for i in InPacket(packet.path).records(foo='bar')])

    def test_InPacket_SubPackets(self):
        with OutPacket(root='.') as packet:
            packet.push(num=1, packet=1)

            with OutPacket(root='.') as sub_packet_1:
                sub_packet_1.push(num=2, packet=2)

                with OutPacket(root='.') as sub_packet_2:
                    sub_packet_2.push(num=3, packet=3)
                sub_packet_1.push(sub_packet_2)

                sub_packet_1.push(num=4, packet=2)
            packet.push(sub_packet_1)

            packet.push(num=5, packet=1)

        self.assertEqual([
            {'num': 1, 'packet': 1},
            {'num': 2, 'packet': 2},
            {'num': 3, 'packet': 3},
            {'num': 4, 'packet': 2},
            {'num': 5, 'packet': 1},
            ],
            [i for i in InPacket(packet.path)])

    def test_InPacket_MixingHeaderToRecords(self):
        with OutPacket(root='.', header_prop=1) as packet:
            packet.push(record_prop=2)

        self.assertEqual([
            {'header_prop': 1, 'record_prop': 2},
            ],
            [i for i in InPacket(packet.path)])

    def test_OutPacket_Header(self):
        with OutPacket(root='.', a=1) as out_packet:
            out_packet.header['b'] = '2'
            out_packet.header['c'] = None
            out_packet.push()
        with InPacket(out_packet.path) as in_packet:
            self.assertEqual(1, in_packet.header['a'])
            self.assertEqual('2', in_packet.header['b'])
            self.assertEqual(None, in_packet.header['c'])

        with OutPacket(a=1) as out_packet:
            out_packet.header['b'] = '2'
            out_packet.header['c'] = None
            out_packet.push()
            stream, length = out_packet.pop_content()
            assert length > 0
            with InPacket(stream=stream) as in_packet:
                self.assertEqual(1, in_packet.header['a'])
                self.assertEqual('2', in_packet.header['b'])
                self.assertEqual(None, in_packet.header['c'])

    def test_OutPacket_Content(self):
        packet = OutPacket(root='.')
        packet.push(num=1, data=[
            {'a': 1, 'b': '2', 'c': None},
            {'d': 3, 'e': '4', 'f': None},
            ])
        self.touch(('blob', 'blob'))
        packet.push(file('blob'), num=2)
        packet.push(num=3, data=[
            {'g': 5, 'h': '6', 'i': None},
            ])
        packet.close()

        records = []
        for i in InPacket(packet.path):
            if i['content_type'] == 'blob':
                i['blob'] = i['blob'].read()
            records.append(i)

        self.assertEqual([
            {'content_type': 'records', 'num': 1, 'a': 1, 'b': '2',  'c': None},
            {'content_type': 'records', 'num': 1, 'd': 3, 'e': '4',  'f': None},
            {'content_type': 'blob', 'num': 2, 'blob': 'blob'},
            {'content_type': 'records', 'num': 3, 'g': 5, 'h': '6',  'i': None},
            ], records)

    def test_OutPacket_LimitOnPushBlobs(self):
        self.touch(('blob', '0' * 100))

        packet = OutPacket(root='.', limit=100)
        self.assertRaises(DiskFull, packet.push, file('blob'))
        packet.close()
        assert not exists(packet.path)

        packet = OutPacket(root='.', limit=200)
        packet.push(file('blob'))
        self.assertRaises(DiskFull, packet.push, file('blob'))
        packet.close()
        self.assertEqual(
                ['0' * 100],
                [i['blob'].read() for i in InPacket(packet.path)])

    def test_OutPacket_LimitOnPushMessages(self):
        packet = OutPacket(root='.', limit=100)
        self.assertRaises(DiskFull, packet.push, ['1' * 100])
        packet.close()
        assert not exists(packet.path)

        packet = OutPacket(root='.', limit=200)
        packet.push([{'probe': '1' * 100}])
        self.assertRaises(DiskFull, packet.push, ['1' * 100])
        packet.close()
        self.assertEqual(
                [{'content_type': 'records', 'probe': '1' * 100}],
                [i for i in InPacket(packet.path)])

        packet = OutPacket(root='.', limit=300)
        packet.push([
            {'probe': '1' * 100},
            {'probe': '2' * 100},
            ])
        self.assertRaises(DiskFull, packet.push, ['3' * 100])
        packet.close()
        self.assertEqual(
                [
                    {'content_type': 'records', 'probe': '1' * 100},
                    {'content_type': 'records', 'probe': '2' * 100},
                    ],
                [i for i in InPacket(packet.path)])

    def test_OutPacket_DiskFull(self):
        self.touch(('blob', '0' * 100))

        statvfs.f_bfree = 100
        packet = OutPacket(root='.')
        self.assertRaises(DiskFull, packet.push, file('blob'))
        packet.close()
        assert not exists(packet.path)

        statvfs.f_bfree = 200
        packet = OutPacket(root='.')
        packet.push(file('blob'))
        statvfs.f_bfree = 100
        self.assertRaises(DiskFull, packet.push, file('blob'))
        packet.close()
        self.assertEqual(
                ['0' * 100],
                [i['blob'].read() for i in InPacket(packet.path)])

    def test_OutPacket_Empty(self):
        packet = OutPacket(root='.')
        assert packet.empty
        packet.push()
        assert not packet.empty
        packet.close()
        assert packet.empty

        packet = OutPacket(root='.')
        self.touch(('blob', 'blob'))
        packet.push(file('blob'))
        assert not packet.empty
        packet.clear()
        assert packet.empty

    def test_OutPacket_DoNotSaveEmptyPackets(self):
        packet = OutPacket(root='.', foo='bar')
        packet.close()
        assert not exists(packet.path)

        packet = OutPacket(root='.', foo='bar')
        packet.push()
        packet.close()
        assert exists(packet.path)

        packet = OutPacket(root='.', foo='bar')
        self.touch(('blob', 'blob'))
        packet.push(file('blob'))
        packet.close()
        assert exists(packet.path)

    def test_OutPacket_ClearOnExceptions(self):
        try:
            with OutPacket(root='.') as packet:
                packet.push()
                raise Exception()
        except Exception:
            pass
        assert not exists(packet.path)

        try:
            with OutPacket(root='.') as packet:
                packet.push()
                raise DiskFull()
        except Exception:
            pass
        assert exists(packet.path)


class statvfs(object):

    f_bfree = 0
    f_frsize = 1


if __name__ == '__main__':
    tests.main()
