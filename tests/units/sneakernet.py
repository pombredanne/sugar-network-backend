#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import json
from glob import glob
from os.path import exists

from __init__ import tests

from sugar_network.local import sneakernet


class SneakernetTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.override(os, 'statvfs', lambda *args: statvfs())

    def test_InPacket_WrongFile(self):
        self.packet('test.gz', [])
        packet = sneakernet._InPacket('test.gz')
        assert not packet.opened

        self.packet('test.gz', [{'subject': 'bar'}])
        packet = sneakernet._InPacket('test.gz')
        assert not packet.opened

        self.packet('test.gz', [{'subject': 'Sugar Network Packet'}])
        packet = sneakernet._InPacket('test.gz')
        assert packet.opened
        self.assertEqual(None, packet.header.get('sender'))
        self.assertEqual(None, packet.header.get('receiver'))

        self.packet('test.gz', [{'subject': 'Sugar Network Packet', 'sender': 'me', 'receiver': 'you'}])
        packet = sneakernet._InPacket('test.gz')
        assert packet.opened
        self.assertEqual('me', packet.header.get('sender'))
        self.assertEqual('you', packet.header.get('receiver'))

    def test_InPacket(self):
        self.packet('test.gz', [
            {'subject': 'Sugar Network Packet'},
            ])
        packet = sneakernet._InPacket('test.gz')
        self.assertEqual(
                [],
                [i for i in packet.read_rows(type='probe')])
        assert not packet.opened

        self.packet('test.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'probe', 'foo': 'bar'},
            ])
        packet = sneakernet._InPacket('test.gz')
        self.assertEqual(
                [{'type': 'probe', 'foo': 'bar'}],
                [i for i in packet.read_rows(type='probe')])
        assert not packet.opened

        self.packet('test.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'probe', 'foo': 1},
            {'type': 'probe', 'foo': 2},
            {'type': 'stop'},
            {'type': 'probe', 'foo': 3}
            ])
        packet = sneakernet._InPacket('test.gz')
        self.assertEqual(
                [{'type': 'probe', 'foo': 1}, {'type': 'probe', 'foo': 2}],
                [i for i in packet.read_rows(type='probe')])
        assert packet.opened
        self.assertEqual(
                [{'type': 'stop'}],
                [i for i in packet.read_rows(type='stop')])
        assert packet.opened
        self.assertEqual(
                [{'type': 'probe', 'foo': 3}],
                [i for i in packet.read_rows(type='probe')])
        assert not packet.opened

    def test_OutPacket(self):
        out_packet = sneakernet._OutPacket('.', sender='me')
        out_packet.close()
        assert out_packet.path is None

        out_packet = sneakernet._OutPacket('.', sender='me')
        out_packet.write_row(foo='bar')
        out_packet.write_row(bar='foo')
        out_packet.close()
        assert exists(out_packet.path)
        in_packet = sneakernet._InPacket(out_packet.path)
        self.assertEqual('me', in_packet.header.get('sender'))
        self.assertEqual(
                [{'foo': 'bar'}, {'bar': 'foo'}],
                [i for i in in_packet.read_rows()])

    def test_OutPacket_DiskFull(self):
        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2 - 1
        out_packet = sneakernet._OutPacket('.', sender='me')
        self.assertRaises(IOError, out_packet.write_row, foo='bar')
        out_packet.close()
        assert out_packet.path is None

        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2
        out_packet = sneakernet._OutPacket('.', sender='me')
        out_packet.write_row(foo='bar')
        out_packet.close()
        in_packet = sneakernet._InPacket(out_packet.path)
        self.assertEqual('me', in_packet.header.get('sender'))
        self.assertEqual(
                [{'foo': 'bar'}],
                [i for i in in_packet.read_rows()])

    def test_OutPacket_SwitchVolumes(self):
        switches = []

        def next_volume_cb(path, *args):
            switches.append(path)
            if len(switches) == 3:
                statvfs.f_bfree += 1
            return True

        sneakernet.next_volume_cb = next_volume_cb
        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2 - 1
        out_packet = sneakernet._OutPacket('.', sender='me')
        out_packet.write_row(foo='bar')
        out_packet.close()

        self.assertEqual(
                [tests.tmpdir] * 3,
                switches)
        self.assertEqual(
                [{'foo': 'bar'}],
                [i for i in sneakernet._InPacket(out_packet.path).read_rows()])

    def test_OutPacket_WriteToSeveralVolumes(self):
        switches = []
        out_packet = sneakernet._OutPacket('.', sender='me')

        def next_volume_cb(path, *args):
            switches.append(path)
            os.rename(out_packet.path, '%s.gz' % len(switches))
            statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2
            if len(switches) == 3:
                raise '!!'
            return True

        sneakernet.next_volume_cb = next_volume_cb
        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2
        out_packet.write_row(write=1, data='*' * sneakernet._RESERVED_SIZE)
        statvfs.f_bfree = sneakernet._RESERVED_SIZE
        out_packet.write_row(write=2, data='*' * sneakernet._RESERVED_SIZE)
        statvfs.f_bfree = sneakernet._RESERVED_SIZE
        out_packet.write_row(write=3, data='*' * sneakernet._RESERVED_SIZE)
        out_packet.close()

        self.assertEqual(
                [tests.tmpdir] * 2,
                switches)

        in_packet_1 = sneakernet._InPacket('1.gz')
        in_packet_2 = sneakernet._InPacket('2.gz')
        in_packet_3 = sneakernet._InPacket(out_packet.path)

        assert in_packet_1.header['guid']
        assert in_packet_1.header['guid'] != in_packet_2.header['guid']
        assert in_packet_1.header['guid'] != in_packet_3.header['guid']
        self.assertEqual('me', in_packet_1.header.get('sender'))
        self.assertEqual(
                [(1, None), (None, in_packet_2.header['guid'])],
                [(i.get('write'), i.get('next')) for i in in_packet_1.read_rows()])
        assert 'prev' not in in_packet_1.header

        assert in_packet_2.header['guid']
        assert in_packet_2.header['guid'] != in_packet_1.header['guid']
        assert in_packet_2.header['guid'] != in_packet_3.header['guid']
        self.assertEqual('me', in_packet_2.header.get('sender'))
        self.assertEqual(
                [(2, None), (None, in_packet_3.header['guid'])],
                [(i.get('write'), i.get('next')) for i in in_packet_2.read_rows()])
        self.assertEqual(in_packet_1.header['guid'], in_packet_2.header.get('prev'))

        assert in_packet_3.header['guid']
        assert in_packet_3.header['guid'] != in_packet_1.header['guid']
        assert in_packet_3.header['guid'] != in_packet_2.header['guid']
        self.assertEqual('me', in_packet_3.header.get('sender'))
        self.assertEqual(
                [(3, None)],
                [(i.get('write'), i.get('next')) for i in in_packet_3.read_rows()])
        self.assertEqual(in_packet_2.header['guid'], in_packet_3.header.get('prev'))

    def test_node_import_SupportedTypes(self):
        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'foo'},
            {'type': 'diff'},
            {'type': 'syn'},
            {'type': 'ack'},
            {'type': 'request'},
            {'type': 'foo'},
            ])
        rows = []
        sneakernet.sync_node('node', '.', lambda h, x: rows.append(x), [])
        self.assertEqual(
                sorted([{'type': 'diff'}]),
                sorted(rows))
        assert exists('test.packet.gz')

        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'master'},
            {'type': 'diff'},
            {'type': 'syn'},
            {'type': 'ack'},
            {'type': 'request'},
            {'type': 'foo'},
            ])
        rows = []
        sneakernet.sync_node('node', '.', lambda h, x: rows.append(x), [])
        self.assertEqual(
                sorted([{'type': 'diff'}, {'type': 'syn'}]),
                sorted(rows))
        assert exists('test.packet.gz')

        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'node'},
            {'type': 'diff'},
            {'type': 'syn'},
            {'type': 'ack'},
            {'type': 'request'},
            {'type': 'foo'},
            ])
        rows = []
        sneakernet.sync_node('node', '.', lambda h, x: rows.append(x), [])
        self.assertEqual(
                sorted([]),
                sorted(rows))
        assert not exists('test.packet.gz')

        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'master', 'to': 'node'},
            {'type': 'ack'},
            {'type': 'diff'},
            {'type': 'syn'},
            {'type': 'request'},
            {'type': 'foo'},
            ])
        rows = []
        sneakernet.sync_node('node', '.', lambda h, x: rows.append(x), [])
        self.assertEqual(
                sorted([{'type': 'ack'}]),
                sorted(rows))
        assert not exists('test.packet.gz')

    def test_master_import_SupportedTypes(self):
        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'diff'},
            {'type': 'syn'},
            {'type': 'request'},
            {'type': 'foo'},
            ])

        rows = []
        sneakernet.sync_master('.', lambda h, x: rows.append(x), [])

        self.assertEqual(
                sorted([{'type': 'diff'}, {'type': 'syn'}, {'type': 'request'}]),
                sorted(rows))

    def test_master_import_SkipMaster(self):
        self.packet('test.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'master'},
            {'type': 'diff'},
            ])

        rows = []
        sneakernet.sync_master('.', lambda h, x: rows.append(x), [])

        self.assertEqual([], rows)

    def test_master_export_CleanupImported(self):
        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'master'},
            {'type': 'diff'},
            ])
        self.packet('2.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'master'},
            {'type': 'diff'},
            ])

        packet = sneakernet.sync_master('.', lambda h, x: None, [('node', {})])

        assert not exists('1.packet.gz')
        assert not exists('2.packet.gz')
        assert exists(packet)

    def test_node_export_CleanupExisting(self):
        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'node-1'},
            {'type': 'diff'},
            ])
        self.packet('2.packet.gz', [
            {'subject': 'Sugar Network Packet', 'sender': 'node-2'},
            {'type': 'diff'},
            ])

        packet_1 = sneakernet.sync_node('node-1', '.', lambda h, row: None, [('master', {})])

        assert not exists('1.packet.gz')
        assert exists('2.packet.gz')
        assert exists(packet_1)

    def test_sync_node_Volumes(self):
        volumes = []
        def next_volume_cb(*args):
            if not volumes:
                volumes.append(True)
                return True
        sneakernet.next_volume_cb = next_volume_cb

        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'diff', 'seqno': 1},
            {'type': 'part', 'next': '2'},
            ])
        imported = []
        sneakernet.sync_node('node-1', '.', lambda h, x: imported.append(x['seqno']), [])
        self.assertEqual([1], imported)

        def next_volume_cb(*args):
            if not exists('2.packet.gz'):
                self.packet('2.packet.gz', [
                    {'subject': 'Sugar Network Packet', 'prev': '3'},
                    {'type': 'diff', 'seqno': 2},
                    ])
                return True
            if not exists('3.packet.gz'):
                self.packet('3.packet.gz', [
                    {'subject': 'Sugar Network Packet'},
                    {'type': 'diff', 'seqno': 3},
                    {'type': 'part', 'next': '1'},
                    ])
                return True
        sneakernet.next_volume_cb = next_volume_cb

        imported = []
        sneakernet.sync_node('node-1', '.', lambda h, x: imported.append(x['seqno']), [])
        self.assertEqual([1, 2, 3], imported)

    def test_sync_master_Volumes(self):
        volumes = []
        def next_volume_cb(*args):
            if not volumes:
                volumes.append(True)
                return True
        sneakernet.next_volume_cb = next_volume_cb

        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'diff', 'seqno': 1},
            {'type': 'part', 'next': '2'},
            ])
        imported = []
        sneakernet.sync_master('.', lambda h, x: imported.append(x['seqno']), [])
        self.assertEqual([1], imported)

        volumes = []
        def next_volume_cb(*args):
            if len(volumes) == 0:
                self.packet('2.packet.gz', [
                    {'subject': 'Sugar Network Packet', 'prev': '3'},
                    {'type': 'diff', 'seqno': 2},
                    ])
                volumes.append(True)
                return True
            if len(volumes) == 1:
                self.packet('3.packet.gz', [
                    {'subject': 'Sugar Network Packet'},
                    {'type': 'diff', 'seqno': 3},
                    {'type': 'part', 'next': '1'},
                    ])
                volumes.append(True)
                return True
        sneakernet.next_volume_cb = next_volume_cb

        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet'},
            {'type': 'diff', 'seqno': 1},
            {'type': 'part', 'next': '2'},
            ])
        imported = []
        sneakernet.sync_master('.', lambda h, x: imported.append(x['seqno']), [])
        self.assertEqual([1, 2, 3], imported)

    def test_SYNOnlyForImportedPackets(self):
        self.packet('1.packet.gz', [
            {'subject': 'Sugar Network Packet', 'guid': '1', 'sender': 'master'},
            {'type': 'syn', 'packets': ['1'], 'mark': '1'},
            {'type': 'syn', 'packets': ['1', 'foo'], 'mark': '2'},
            {'type': 'part', 'next': '2'},
            ])
        self.packet('2.packet.gz', [
            {'subject': 'Sugar Network Packet', 'guid': '2', 'prev': '1'},
            {'type': 'syn', 'packets': ['1', '2'], 'mark': '3'},
            {'type': 'syn', 'packets': ['foo', '2'], 'mark': '4'},
            {'type': 'syn', 'packets': ['1'], 'mark': '5'},
            ])

        node_syns = []
        sneakernet.sync_node('node', '.', lambda h, x: node_syns.append(x['mark']), [])
        master_syns = []
        sneakernet.sync_master('.', lambda h, x: master_syns.append(x['mark']), [])

        self.assertEqual(sorted(['1']), sorted(node_syns))
        self.assertEqual(sorted(['3', '5']), sorted(master_syns))

    def test_SYNOnlyForExportedPackets(self):

        def next_volume_cb(path, *args):
            statvfs.f_bfree += 1
            return True
        sneakernet.next_volume_cb = next_volume_cb

        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2
        sneakernet.sync_master('.', lambda h, x: None, [
            ('node', {'type': 'syn', 'probe': 1, 'ballast': '*' * sneakernet._RESERVED_SIZE}),
            ('node', {'type': 'syn', 'probe': 2}),
            ])

        statvfs.f_bfree = sneakernet._RESERVED_SIZE * 2
        sneakernet.sync_node('node2', '.', lambda h, x: None, [
            ('node', {'type': 'syn', 'probe': 3, 'ballast': '*' * sneakernet._RESERVED_SIZE}),
            ('node', {'type': 'syn', 'probe': 4}),
            ])

        packets = []
        for path in glob('*.packet.gz'):
            with sneakernet._InPacket(path) as packet:
                for row in packet.read_rows(type='syn'):
                    pass
                for row in packet.syns:
                    packets.append((row['probe'], len(row.get('ballast', []))))
        self.assertEqual(
                sorted([
                    (1, sneakernet._RESERVED_SIZE),
                    (2, 0),
                    (3, sneakernet._RESERVED_SIZE),
                    (4, 0),
                    ]),
                sorted(packets))

    def packet(self, filename, data):
        bundle = gzip.GzipFile(filename, 'w')
        for i in data:
            bundle.write(json.dumps(i) + '\n')
        bundle.close()


class statvfs(object):

    f_bfree = sneakernet._RESERVED_SIZE * 10
    f_frsize = 1


if __name__ == '__main__':
    tests.main()
