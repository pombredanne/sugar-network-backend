#!/usr/bin/env python
# sugar-lint: disable

import gzip
import json
from os.path import exists

from __init__ import tests

from active_document import env
from active_document.folder import NodeFolder, _InPacket


class FolderTest(tests.Test):

    def test_id(self):
        folder = NodeFolder([])
        assert exists('id')
        self.assertNotEqual('', file('id').read().strip())

        self.touch(('id', 'foo'))
        folder = NodeFolder([])
        self.assertEqual('foo', file('id').read())

    def test_InPacket_WrongFile(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.close()
        packet = _InPacket('test.gz')
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'bar'}))
        bundle.close()
        packet = _InPacket('test.gz')
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()
        packet = _InPacket('test.gz')
        assert packet.opened
        self.assertEqual(None, packet.sender)
        self.assertEqual(None, packet.receiver)

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet', 'sender': 'me', 'receiver': 'you'}))
        bundle.close()
        packet = _InPacket('test.gz')
        assert packet.opened
        self.assertEqual('me', packet.sender)
        self.assertEqual('you', packet.receiver)

    def test_InPacket_syns(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [],
                [i for i in packet.syns])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertRaises(KeyError, packet.syns.next)

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'document': 'foo', 'syn': 'bar'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [('foo', 'bar')],
                [i for i in packet.syns])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'document': 'foo', 'syn': 'bar-1'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'document': 'foo', 'syn': 'bar-2'}) + '\n')
        bundle.write(json.dumps({'type': 'stop'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'document': 'fake', 'syn': 'bar-3'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [('foo', 'bar-1'), ('foo', 'bar-2')],
                [i for i in packet.syns])
        assert packet.opened

    def test_InPacket_acks(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [],
                [i for i in packet.acks])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'ack'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertRaises(KeyError, packet.acks.next)

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'ack', 'document': 'foo', 'ack': 'bar'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [('foo', 'bar')],
                [i for i in packet.acks])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'ack', 'document': 'foo', 'ack': 'bar-1'}) + '\n')
        bundle.write(json.dumps({'type': 'ack', 'document': 'foo', 'ack': 'bar-2'}) + '\n')
        bundle.write(json.dumps({'type': 'stop'}) + '\n')
        bundle.write(json.dumps({'type': 'ack', 'document': 'fake', 'ack': 'bar-3'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [('foo', 'bar-1'), ('foo', 'bar-2')],
                [i for i in packet.acks])
        assert packet.opened

    def test_InPacket_dumps(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertEqual(
                [],
                [i for i in packet.dumps])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'dump'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        self.assertRaises(KeyError, packet.dumps.next)

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'dump', 'document': 'foo', 'region': 'bar'}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        dumps = packet.dumps
        document, region, rows = dumps.next()
        self.assertEqual('foo', document)
        self.assertEqual('bar', region)
        self.assertEqual(
                [],
                [i for i in rows])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'dump', 'document': 'foo', 'region': 1}) + '\n')
        bundle.write(json.dumps({'type': 'row', 'row': 1}) + '\n')
        bundle.write(json.dumps({'type': 'row', 'row': 2}) + '\n')
        bundle.write(json.dumps({'type': 'dump', 'document': 'bar', 'region': 2}) + '\n')
        bundle.write(json.dumps({'type': 'row', 'row': 3}) + '\n')
        bundle.write(json.dumps({'type': 'stop'}) + '\n')
        bundle.write(json.dumps({'type': 'dump', 'document': 'fake', 'region': 3}) + '\n')
        bundle.write(json.dumps({'type': 'row', 'row': 4}) + '\n')
        bundle.close()

        packet = _InPacket('test.gz')
        dumps = packet.dumps
        document, region, rows = dumps.next()
        self.assertEqual('foo', document)
        self.assertEqual(1, region)
        self.assertEqual(
                [1, 2],
                [i for i in rows])
        document, region, rows = dumps.next()
        self.assertEqual('bar', document)
        self.assertEqual(2, region)
        self.assertEqual(
                [3],
                [i for i in rows])
        self.assertRaises(StopIteration, dumps.next)
        assert packet.opened


if __name__ == '__main__':
    tests.main()
