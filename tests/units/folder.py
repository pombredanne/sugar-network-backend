#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import json
from os.path import exists

from __init__ import tests

from active_document import env, folder



class FolderTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.override(os, 'statvfs', lambda *args: statvfs())

    def test_id(self):
        node_folder = folder.NodeFolder([])
        assert exists('id')
        self.assertNotEqual('', file('id').read().strip())

        self.touch(('id', 'foo'))
        node_folder = folder.NodeFolder([])
        self.assertEqual('foo', file('id').read())

    def test_InPacket_WrongFile(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.close()
        packet = folder._InPacket('test.gz')
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'bar'}))
        bundle.close()
        packet = folder._InPacket('test.gz')
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()
        packet = folder._InPacket('test.gz')
        assert packet.opened
        self.assertEqual(None, packet.sender)
        self.assertEqual(None, packet.receiver)

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet', 'sender': 'me', 'receiver': 'you'}))
        bundle.close()
        packet = folder._InPacket('test.gz')
        assert packet.opened
        self.assertEqual('me', packet.sender)
        self.assertEqual('you', packet.receiver)

    def test_InPacket(self):
        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}))
        bundle.close()

        packet = folder._InPacket('test.gz')
        self.assertEqual(
                [],
                [i for i in packet.read_rows(type='syn')])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn'}) + '\n')
        bundle.close()

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'foo': 'bar'}) + '\n')
        bundle.close()

        packet = folder._InPacket('test.gz')
        self.assertEqual(
                [{'type': 'syn', 'foo': 'bar'}],
                [i for i in packet.read_rows(type='syn')])
        assert not packet.opened

        bundle = gzip.GzipFile('test.gz', 'w')
        bundle.write(json.dumps({'subject': 'Sugar Network Packet'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'foo': 1}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'foo': 2}) + '\n')
        bundle.write(json.dumps({'type': 'stop'}) + '\n')
        bundle.write(json.dumps({'type': 'syn', 'foo': 3}) + '\n')
        bundle.close()

        packet = folder._InPacket('test.gz')
        self.assertEqual(
                [{'type': 'syn', 'foo': 1}, {'type': 'syn', 'foo': 2}],
                [i for i in packet.read_rows(type='syn')])
        assert packet.opened
        self.assertEqual(
                [{'type': 'stop'}],
                [i for i in packet.read_rows(type='stop')])
        assert packet.opened
        self.assertEqual(
                [{'type': 'syn', 'foo': 3}],
                [i for i in packet.read_rows(type='syn')])
        assert not packet.opened

    def test_OutPacket(self):
        out_packet = folder._OutPacket('test.gz', sender='me')
        out_packet.close()
        assert not exists('test.gz')

        out_packet = folder._OutPacket('test.gz', sender='me')
        out_packet.write_row(foo='bar')
        out_packet.write_row(bar='foo')
        out_packet.close()
        assert exists('test.gz')
        in_packet = folder._InPacket('test.gz')
        self.assertEqual('me', in_packet.sender)
        self.assertEqual(
                [{'foo': 'bar'}, {'bar': 'foo'}],
                [i for i in in_packet.read_rows()])

    def test_OutPacket_DiskFull(self):
        statvfs.f_bfree = folder._RESERVED_SIZE * 2 - 1
        out_packet = folder._OutPacket('test.gz', sender='me')
        self.assertRaises(IOError, out_packet.write_row, foo='bar')
        out_packet.close()
        assert not exists('test.gz')

        statvfs.f_bfree = folder._RESERVED_SIZE * 2
        out_packet = folder._OutPacket('test.gz', sender='me')
        out_packet.write_row(foo='bar')
        out_packet.close()
        in_packet = folder._InPacket('test.gz')
        self.assertEqual('me', in_packet.sender)
        self.assertEqual(
                [{'foo': 'bar'}],
                [i for i in in_packet.read_rows()])

    def test_OutPacket_SwitchVolumes(self):
        switches = []

        def next_volume_cb(path):
            switches.append(path)
            if len(switches) == 3:
                statvfs.f_bfree += 1
            return True

        statvfs.f_bfree = folder._RESERVED_SIZE * 2 - 1
        out_packet = folder._OutPacket('test.gz', sender='me',
                next_volume_cb=next_volume_cb)
        out_packet.write_row(foo='bar')
        out_packet.close()

        self.assertEqual(
                [tests.tmpdir] * 3,
                switches)
        self.assertEqual(
                [{'foo': 'bar'}],
                [i for i in folder._InPacket('test.gz').read_rows()])

    def test_OutPacket_WriteToSeveralVolumes(self):
        switches = []

        def next_volume_cb(path):
            switches.append(path)
            os.rename('test.gz', 'test.gz.%s' % len(switches))
            statvfs.f_bfree = folder._RESERVED_SIZE * 2
            return True

        statvfs.f_bfree = folder._RESERVED_SIZE * 2
        out_packet = folder._OutPacket('test.gz', sender='me',
                next_volume_cb=next_volume_cb)
        out_packet.write_row(write=1, data='*' * folder._RESERVED_SIZE)
        statvfs.f_bfree = folder._RESERVED_SIZE
        out_packet.write_row(write=2, data='*' * folder._RESERVED_SIZE)
        statvfs.f_bfree = folder._RESERVED_SIZE
        out_packet.write_row(write=3, data='*' * folder._RESERVED_SIZE)
        out_packet.close()

        self.assertEqual(
                [tests.tmpdir] * 2,
                switches)

        in_packet = folder._InPacket('test.gz.1')
        self.assertEqual('me', in_packet.sender)
        self.assertEqual(
                [1],
                [i['write'] for i in in_packet.read_rows()])

        in_packet = folder._InPacket('test.gz.2')
        self.assertEqual('me', in_packet.sender)
        self.assertEqual(
                [2],
                [i['write'] for i in in_packet.read_rows()])

        in_packet = folder._InPacket('test.gz')
        self.assertEqual('me', in_packet.sender)
        self.assertEqual(
                [3],
                [i['write'] for i in in_packet.read_rows()])


class statvfs(object):

    f_bfree = folder._RESERVED_SIZE * 10
    f_frsize = 1


if __name__ == '__main__':
    tests.main()
