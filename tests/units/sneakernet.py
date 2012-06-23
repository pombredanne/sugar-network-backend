#!/usr/bin/env python
# sugar-lint: disable

import json
import tarfile
from cStringIO import StringIO

from __init__ import tests

from sugar_network.node import sneakernet
from sugar_network.node.sneakernet import InPacket, OutPacket, DiskFull


class SneakernetTest(tests.Test):

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
            self.assertEqual(2, packet['a'])
            self.assertEqual('4', packet['b'])
            self.assertEqual(None, packet['c'])

    def test_InPacket_Content(self):
        self.touch(('header', '{}'))
        self.touch(('record_1', [
            json.dumps({'a': 1, 'b': '2', 'c': None}),
            json.dumps({'d': 3, 'e': '4', 'f': None}),
            ]))
        self.touch(('record_1.meta', json.dumps({'type': 'messages', 'num': 1})))
        self.touch(('record_2', 'blob'))
        self.touch(('record_2.meta', json.dumps({'type': 'blob', 'num': 2})))
        self.touch(('record_3', json.dumps({'probe': None})))
        self.touch(('record_3.meta', json.dumps({'num': 3})))

        tarball = tarfile.open('tarball', 'w:gz')
        tarball.add('header')
        tarball.add('record_1')
        tarball.add('record_1.meta')
        tarball.add('record_2')
        tarball.add('record_2.meta')
        tarball.add('record_3')
        tarball.add('record_3.meta')
        tarball.close()

        packet = InPacket('tarball')

        records = []
        for i in packet:
            if i['type'] == 'blob':
                i['blob'] = i['blob'].read()
            records.append(i)

        self.assertEqual([
            {'type': 'messages', 'num': 1, 'a': 1, 'b': '2',  'c': None},
            {'type': 'messages', 'num': 1, 'd': 3, 'e': '4',  'f': None},
            {'type': 'blob', 'num': 2, 'blob': 'blob'},
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
        self.assertEqual('ok', packet['probe'])

    def test_OutPacket_Header(self):
        with OutPacket('probe', root='.', a=1) as out_packet:
            out_packet['b'] = '2'
            out_packet['c'] = None
        with InPacket(out_packet.path) as in_packet:
            self.assertEqual(1, in_packet['a'])
            self.assertEqual('2', in_packet['b'])
            self.assertEqual(None, in_packet['c'])

        with OutPacket('probe', a=1) as out_packet:
            out_packet['b'] = '2'
            out_packet['c'] = None
            stream, length = out_packet.pop_content()
            assert length > 0
            with InPacket(stream=stream) as in_packet:
                self.assertEqual(1, in_packet['a'])
                self.assertEqual('2', in_packet['b'])
                self.assertEqual(None, in_packet['c'])

    def test_OutPacket_Content(self):
        packet = OutPacket('probe', root='.')
        packet.push_messages(num=1, items=[
            {'a': 1, 'b': '2', 'c': None},
            {'d': 3, 'e': '4', 'f': None},
            ])
        self.touch(('blob', 'blob'))
        packet.push_blob(file('blob'), num=2)
        packet.push_messages(num=3, items=[
            {'g': 5, 'h': '6', 'i': None},
            ])
        packet.close()

        records = []
        for i in InPacket(packet.path):
            if i['type'] == 'blob':
                i['blob'] = i['blob'].read()
            records.append(i)

        self.assertEqual([
            {'type': 'messages', 'num': 1, 'a': 1, 'b': '2',  'c': None},
            {'type': 'messages', 'num': 1, 'd': 3, 'e': '4',  'f': None},
            {'type': 'blob', 'num': 2, 'blob': 'blob'},
            {'type': 'messages', 'num': 3, 'g': 5, 'h': '6',  'i': None},
            ], records)

    def test_OutPacket_DiskFullOnPushBlobs(self):
        self.touch(('blob', '0' * 100))

        packet = OutPacket('probe', root='.', limit=100)
        self.assertRaises(DiskFull, packet.push_blob, file('blob'))
        packet.close()
        self.assertEqual(
                [],
                [i['blob'].read() for i in InPacket(packet.path)])

        packet = OutPacket('probe', root='.', limit=200)
        packet.push_blob(file('blob'))
        self.assertRaises(DiskFull, packet.push_blob, file('blob'))
        packet.close()
        self.assertEqual(
                ['0' * 100],
                [i['blob'].read() for i in InPacket(packet.path)])

    def test_OutPacket_DiskFullOnPushMessages(self):
        packet = OutPacket('probe', root='.', limit=100)
        self.assertRaises(DiskFull, packet.push_messages, ['1' * 100])
        packet.close()
        self.assertEqual(
                [],
                [i for i in InPacket(packet.path)])

        packet = OutPacket('probe', root='.', limit=200)
        packet.push_messages([{'probe': '1' * 100}])
        self.assertRaises(DiskFull, packet.push_messages, ['1' * 100])
        packet.close()
        self.assertEqual(
                [{'type': 'messages', 'probe': '1' * 100}],
                [i for i in InPacket(packet.path)])

        packet = OutPacket('probe', root='.', limit=300)
        packet.push_messages([
            {'probe': '1' * 100},
            {'probe': '2' * 100},
            ])
        self.assertRaises(DiskFull, packet.push_messages, ['3' * 100])
        packet.close()
        self.assertEqual(
                [
                    {'type': 'messages', 'probe': '1' * 100},
                    {'type': 'messages', 'probe': '2' * 100},
                    ],
                [i for i in InPacket(packet.path)])


if __name__ == '__main__':
    tests.main()
