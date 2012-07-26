#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from glob import glob
from os.path import exists, isfile, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit.files_sync import Seeder
from sugar_network.toolkit.sneakernet import OutBufferPacket, InPacket, DiskFull


class FilesSyncTest(tests.Test):

    def test_Seeder_pull_Populate(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        os.utime('files', (1, 1))

        packet = OutBufferPacket()
        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[1, None]], in_seq)
        self.assertEqual(0, seqno.value)
        self.assertEqual(True, packet.empty)
        assert not exists('index')

        self.touch('files/1')
        self.touch('files/2/3')
        self.touch('files/4/5/6')
        utime('files', 1)
        os.utime('files', (1, 1))

        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[1, None]], in_seq)
        self.assertEqual(0, seqno.value)
        self.assertEqual(True, packet.empty)
        assert not exists('index')

        utime('files', 2)
        os.utime('files', (2, 2))

        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[4, None]], in_seq)
        self.assertEqual(3, seqno.value)
        self.assertEqual(False, packet.empty)
        assert exists('index')
        self.assertEqual(
                [[
                    [1, '1', os.stat('files/1').st_mtime],
                    [2, '2/3', os.stat('files/2/3').st_mtime],
                    [3, '4/5/6', os.stat('files/4/5/6').st_mtime],
                    ],
                    os.stat('files').st_mtime],
                json.load(file('index')))
        in_packet = InPacket(stream=packet.pop())
        self.assertEqual([[1, 3]], in_packet.header['sequence'])
        self.assertEqual([], in_packet.header['deleted'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/1',
                    'files/2/3',
                    'files/4/5/6',
                    ]),
                sorted(in_packet._tarball.getnames()))

        packet = OutBufferPacket()
        in_seq = Sequence([[4, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[4, None]], in_seq)
        self.assertEqual(3, seqno.value)
        self.assertEqual(True, packet.empty)

    def test_Seeder_pull_NotFull(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        self.touch('files/4')
        self.touch('files/5')
        utime('files', 1)

        out_packet = OutBufferPacket()
        in_seq = Sequence([[2, 2], [4, 10], [20, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[6, 10], [20,None]], in_seq)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[2, 2], [4, 5]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/2',
                    'files/4',
                    'files/5',
                    ]),
                sorted(in_packet._tarball.getnames()))

    def test_Seeder_pull_DiskFull(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '*' * 1000))
        self.touch(('files/2', '*' * 1000))
        self.touch(('files/3', '*' * 1000))
        utime('files', 1)

        out_packet = OutBufferPacket(limit=2750)
        in_seq = Sequence([[1, None]])
        try:
            seeder.pull(in_seq, out_packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([[3, None]], in_seq)

        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[1, 2]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/1',
                    'files/2',
                    ]),
                sorted(in_packet._tarball.getnames()))

    def test_Seeder_pull_UpdateFiles(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[1, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files/2', (2, 2))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[4, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files', (3, 3))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[4, None]]), out_packet)
        self.assertEqual(4, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[4, 4]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/2',
                    ]),
                sorted(in_packet._tarball.getnames()))

        os.utime('files/1', (4, 4))
        os.utime('files/3', (4, 4))
        os.utime('files', (4, 4))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[5, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[5, 6]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/1',
                    'files/3',
                    ]),
                sorted(in_packet._tarball.getnames()))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[1, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[1, 6]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/1',
                    'files/2',
                    'files/3',
                    ]),
                sorted(in_packet._tarball.getnames()))

    def test_Seeder_pull_CreateFiles(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[1, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        self.touch('files/4')
        os.utime('files/4', (2, 2))
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[4, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files/4', (2, 2))
        os.utime('files', (2, 2))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[4, None]]), out_packet)
        self.assertEqual(4, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[4, 4]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/4',
                    ]),
                sorted(in_packet._tarball.getnames()))

        self.touch('files/5')
        os.utime('files/5', (3, 3))
        self.touch('files/6')
        os.utime('files/6', (3, 3))
        os.utime('files', (3, 3))

        out_packet = OutBufferPacket()
        seeder.pull(Sequence([[5, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[5, 6]], in_packet.header['sequence'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/5',
                    'files/6',
                    ]),
                sorted(in_packet._tarball.getnames()))

    def test_Seeder_pull_DeleteFiles(self):
        seqno = ad.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[4, None]], in_seq)
        self.assertEqual(3, seqno.value)

        os.unlink('files/2')
        os.utime('files', (2, 2))

        out_packet = OutBufferPacket()
        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[5, None]], in_seq)
        self.assertEqual(4, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[1, 4]], in_packet.header['sequence'])
        self.assertEqual(['2'], in_packet.header['deleted'])
        self.assertEqual(
                sorted([
                    'header',
                    'files/1',
                    'files/3',
                    ]),
                sorted(in_packet._tarball.getnames()))

        os.unlink('files/1')
        os.unlink('files/3')
        os.utime('files', (3, 3))

        out_packet = OutBufferPacket()
        in_seq = Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[7, None]], in_seq)
        self.assertEqual(6, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[1, 6]], in_packet.header['sequence'])
        self.assertEqual(['2', '1', '3'], in_packet.header['deleted'])
        self.assertEqual(
                sorted([
                    'header',
                    ]),
                sorted(in_packet._tarball.getnames()))

        out_packet = OutBufferPacket()
        in_seq = Sequence([[4, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[7, None]], in_seq)
        self.assertEqual(6, seqno.value)
        in_packet = InPacket(stream=out_packet.pop())
        self.assertEqual([[4, 6]], in_packet.header['sequence'])
        self.assertEqual(['2', '1', '3'], in_packet.header['deleted'])
        self.assertEqual(
                sorted([
                    'header',
                    ]),
                sorted(in_packet._tarball.getnames()))


def utime(path, ts):
    if isfile(path):
        os.utime(path, (ts, ts))
    else:
        for root, __, files in os.walk(path):
            for i in files:
                os.utime(join(root, i), (ts, ts))


if __name__ == '__main__':
    tests.main()
