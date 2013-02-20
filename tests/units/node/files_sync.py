#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from glob import glob
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.toolkit import util
from sugar_network.toolkit.files_sync import Seeder, Leecher
from sugar_network.toolkit.sneakernet import OutBufferPacket, InPacket, DiskFull, OutFilePacket


CHUNK = 100000


class FilesSyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(db, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Seeder_pull_Populate(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        os.utime('files', (1, 1))

        assert not seeder.pending(util.Sequence([[1, None]]))
        packet = OutBufferPacket()
        in_seq = util.Sequence([[1, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[1, None]], in_seq)
        self.assertEqual(0, seqno.value)
        self.assertEqual(True, packet.empty)
        assert not exists('index')

        self.touch(('files/1', '1'))
        self.touch(('files/2/3', '3'))
        self.touch(('files/4/5/6', '6'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        assert not seeder.pending(util.Sequence([[1, None]]))
        in_seq = util.Sequence([[1, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[1, None]], in_seq)
        self.assertEqual(0, seqno.value)
        self.assertEqual(True, packet.empty)
        assert not exists('index')

        self.utime('files', 2)
        os.utime('files', (2, 2))

        assert seeder.pending(util.Sequence([[1, None]]))
        in_seq = util.Sequence([[1, None]])
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
        self.assertEqual(
                sorted([
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '1', 'content_type': 'blob', 'path': '1'},
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '3', 'content_type': 'blob', 'path': '2/3'},
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '6', 'content_type': 'blob', 'path': '4/5/6'},
                    {'filename': '1.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 3]]},
                    ]),
                read_records(packet))

        assert not seeder.pending(util.Sequence([[4, None]]))
        packet = OutBufferPacket()
        in_seq = util.Sequence([[4, None]])
        seeder.pull(in_seq, packet)
        self.assertEqual([[4, None]], in_seq)
        self.assertEqual(3, seqno.value)
        self.assertEqual(True, packet.empty)

    def test_Seeder_pull_NotFull(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.touch(('files/4', '4'))
        self.touch(('files/5', '5'))
        self.utime('files', 1)

        out_packet = OutBufferPacket()
        in_seq = util.Sequence([[2, 2], [4, 10], [20, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[6, 10], [20,None]], in_seq)
        self.assertEqual(
                sorted([
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '2', 'content_type': 'blob', 'path': '2'},
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '4', 'content_type': 'blob', 'path': '4'},
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '5', 'content_type': 'blob', 'path': '5'},
                    {'filename': '1.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[2, 2], [4, 5]]},
                    ]),
                read_records(out_packet))

    def test_Seeder_pull_DiskFull(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '*' * CHUNK))
        self.touch(('files/2', '*' * CHUNK))
        self.touch(('files/3', '*' * CHUNK))
        self.utime('files', 1)

        out_packet = OutBufferPacket(limit=CHUNK * 2.5)
        in_seq = util.Sequence([[1, None]])
        try:
            seeder.pull(in_seq, out_packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([[3, None]], in_seq)
        self.assertEqual(
                sorted([
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '*' * CHUNK, 'content_type': 'blob', 'path': '1'},
                    {'filename': '1.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '*' * CHUNK, 'content_type': 'blob', 'path': '2'},
                    {'filename': '1.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 2]]},
                    ]),
                read_records(out_packet))

    def test_Seeder_pull_UpdateFiles(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[1, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files/2', (2, 2))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[4, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files', (3, 3))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[4, None]]), out_packet)
        self.assertEqual(4, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '3.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '2', 'content_type': 'blob', 'path': '2'},
                    {'filename': '3.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[4, 4]]},
                    ]),
                read_records(out_packet))

        os.utime('files/1', (4, 4))
        os.utime('files/3', (4, 4))
        os.utime('files', (4, 4))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[5, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '4.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '1', 'content_type': 'blob', 'path': '1'},
                    {'filename': '4.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '3', 'content_type': 'blob', 'path': '3'},
                    {'filename': '4.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[5, 6]]},
                    ]),
                read_records(out_packet))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[1, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '5.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '1', 'content_type': 'blob', 'path': '1'},
                    {'filename': '5.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '2', 'content_type': 'blob', 'path': '2'},
                    {'filename': '5.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '3', 'content_type': 'blob', 'path': '3'},
                    {'filename': '5.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 6]]},
                    ]),
                read_records(out_packet))

    def test_Seeder_pull_CreateFiles(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[1, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        self.touch(('files/4', '4'))
        os.utime('files/4', (2, 2))
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[4, None]]), out_packet)
        self.assertEqual(3, seqno.value)

        os.utime('files/4', (2, 2))
        os.utime('files', (2, 2))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[4, None]]), out_packet)
        self.assertEqual(4, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '3.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '4', 'content_type': 'blob', 'path': '4'},
                    {'filename': '3.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[4, 4]]},
                    ]),
                read_records(out_packet))

        self.touch(('files/5', '5'))
        os.utime('files/5', (3, 3))
        self.touch(('files/6', '6'))
        os.utime('files/6', (3, 3))
        os.utime('files', (3, 3))

        out_packet = OutBufferPacket()
        seeder.pull(util.Sequence([[5, None]]), out_packet)
        self.assertEqual(6, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '4.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '5', 'content_type': 'blob', 'path': '5'},
                    {'filename': '4.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '6', 'content_type': 'blob', 'path': '6'},
                    {'filename': '4.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[5, 6]]},
                    ]),
                read_records(out_packet))

    def test_Seeder_pull_DeleteFiles(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        out_packet = OutBufferPacket()
        in_seq = util.Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[4, None]], in_seq)
        self.assertEqual(3, seqno.value)

        os.unlink('files/2')
        os.utime('files', (2, 2))

        assert seeder.pending(util.Sequence([[4, None]]))
        out_packet = OutBufferPacket()
        in_seq = util.Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[2, 2], [5, None]], in_seq)
        self.assertEqual(4, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '2.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '1', 'content_type': 'blob', 'path': '1'},
                    {'filename': '2.packet', 'cmd': 'files_push', 'directory': 'files', 'blob': '3', 'content_type': 'blob', 'path': '3'},
                    {'filename': '2.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '2'},
                    {'filename': '2.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 4]]},
                    ]),
                read_records(out_packet))

        os.unlink('files/1')
        os.unlink('files/3')
        os.utime('files', (3, 3))

        assert seeder.pending(util.Sequence([[5, None]]))
        out_packet = OutBufferPacket()
        in_seq = util.Sequence([[1, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[1, 3], [7, None]], in_seq)
        self.assertEqual(6, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '3.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '1'},
                    {'filename': '3.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '2'},
                    {'filename': '3.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '3'},
                    {'filename': '3.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[1, 6]]},
                    ]),
                read_records(out_packet))

        out_packet = OutBufferPacket()
        in_seq = util.Sequence([[4, None]])
        seeder.pull(in_seq, out_packet)
        self.assertEqual([[7, None]], in_seq)
        self.assertEqual(6, seqno.value)
        self.assertEqual(
                sorted([
                    {'filename': '4.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '1'},
                    {'filename': '4.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '2'},
                    {'filename': '4.packet', 'cmd': 'files_delete', 'directory': 'files', 'path': '3'},
                    {'filename': '4.packet', 'cmd': 'files_commit', 'directory': 'files', 'sequence': [[4, 6]]},
                    ]),
                read_records(out_packet))

    def test_Leecher_push(self):
        seqno = util.Seqno('seqno')
        seeder = Seeder('src/files', 'src/index', seqno)
        leecher = Leecher('dst/files', 'dst/sequence')

        self.touch(('src/files/1', '1'))
        self.touch(('src/files/2/3', '3'))
        self.touch(('src/files/4/5/6', '6'))
        self.utime('src/files', 1)
        os.utime('src/files', (1, 1))

        with OutFilePacket('.') as packet:
            seeder.pull(util.Sequence([[1, None]]), packet)
            self.assertEqual(3, seqno.value)
        for i in InPacket(packet.path):
            leecher.push(i)

        self.assertEqual(
                '[[4, null]]',
                file('dst/sequence').read())
        self.assertEqual(
                '1',
                file('dst/files/1').read())
        self.assertEqual(
                '3',
                file('dst/files/2/3').read())
        self.assertEqual(
                '6',
                file('dst/files/4/5/6').read())

        os.unlink('src/files/2/3')
        os.utime('src/files', (2, 2))

        with OutFilePacket('.') as packet:
            seeder.pull(util.Sequence([[4, None]]), packet)
            self.assertEqual(4, seqno.value)
        for i in InPacket(packet.path):
            leecher.push(i)

        self.assertEqual(
                '[[5, null]]',
                file('dst/sequence').read())
        assert exists('dst/files/1')
        assert not exists('dst/files/2/3')
        assert exists('dst/files/4/5/6')

        os.unlink('src/files/1')
        self.touch(('src/files/2/3', 'new_3'))
        os.unlink('src/files/4/5/6')
        self.utime('src/files', 3)
        os.utime('src/files', (3, 3))

        with OutFilePacket('.') as packet:
            seeder.pull(util.Sequence([[5, None]]), packet)
            self.assertEqual(7, seqno.value)
        for i in InPacket(packet.path):
            leecher.push(i)

        self.assertEqual(
                '[[8, null]]',
                file('dst/sequence').read())
        assert not exists('dst/files/1')
        assert exists('dst/files/2/3')
        assert not exists('dst/files/4/5/6')
        self.assertEqual(
                'new_3',
                file('dst/files/2/3').read())


def read_records(in_packet):
    records = []
    for i in InPacket(stream=in_packet.pop()):
        if i.get('content_type') == 'blob':
            i['blob'] = i['blob'].read()
        records.append(i)
    return sorted(records)


if __name__ == '__main__':
    tests.main()
