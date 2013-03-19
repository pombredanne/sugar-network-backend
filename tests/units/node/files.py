#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from glob import glob
from os.path import exists
from cStringIO import StringIO

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.toolkit import util
from sugar_network.node import files


class FilesTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(toolkit, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Index_Populate(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        os.utime('files', (1, 1))
        assert seeder.sync()

        assert not seeder.sync()
        in_seq = util.Sequence([[1, None]])
        self.assertEqual(
                [{'op': 'commit', 'sequence': []}],
                [i for i in seeder.diff(in_seq)])
        self.assertEqual(0, seqno.value)
        assert not exists('index')

        self.touch(('files/1', '1'))
        self.touch(('files/2/3', '3'))
        self.touch(('files/4/5/6', '6'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        assert not seeder.sync()
        in_seq = util.Sequence([[1, None]])
        self.assertEqual(
                [{'op': 'commit', 'sequence': []}],
                [i for i in seeder.diff(in_seq)])
        self.assertEqual(0, seqno.value)
        assert not exists('index')

        self.utime('files', 2)
        os.utime('files', (2, 2))

        assert seeder.sync()
        in_seq = util.Sequence([[1, None]])
        self.assertEqual(sorted([
            {'op': 'commit', 'sequence': [[1, 3]]},
            {'op': 'update', 'blob': 'files/1', 'path': '1'},
            {'op': 'update', 'blob': 'files/2/3', 'path': '2/3'},
            {'op': 'update', 'blob': 'files/4/5/6', 'path': '4/5/6'},
            ]),
            sorted([i for i in seeder.diff(in_seq)]))
        self.assertEqual(3, seqno.value)
        assert exists('index')
        self.assertEqual(
                [[
                    [1, '1', os.stat('files/1').st_mtime],
                    [2, '2/3', os.stat('files/2/3').st_mtime],
                    [3, '4/5/6', os.stat('files/4/5/6').st_mtime],
                    ],
                    os.stat('files').st_mtime],
                json.load(file('index')))

        assert not seeder.sync()
        in_seq = util.Sequence([[4, None]])
        self.assertEqual(
                [{'op': 'commit', 'sequence': []}],
                [i for i in seeder.diff(in_seq)])
        self.assertEqual(3, seqno.value)

    def test_Index_SelectiveDiff(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.touch(('files/4', '4'))
        self.touch(('files/5', '5'))
        self.utime('files', 1)

        in_seq = util.Sequence([[2, 2], [4, 10], [20, None]])
        self.assertEqual(sorted([
            {'op': 'commit', 'sequence': [[2, 5]]},
            {'op': 'update', 'blob': 'files/2', 'path': '2'},
            {'op': 'update', 'blob': 'files/4', 'path': '4'},
            {'op': 'update', 'blob': 'files/5', 'path': '5'},
            ]),
            sorted([i for i in seeder.diff(in_seq)]))

    def test_Index_PartialDiff(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        self.utime('files', 1)

        in_seq = util.Sequence([[1, None]])
        diff = seeder.diff(in_seq)
        self.assertEqual({'op': 'update', 'blob': 'files/1', 'path': '1'}, next(diff))
        self.assertEqual({'op': 'commit', 'sequence': []}, diff.throw(StopIteration))
        self.assertRaises(StopIteration, diff.next)

        diff = seeder.diff(in_seq)
        self.assertEqual({'op': 'update', 'blob': 'files/1', 'path': '1'}, next(diff))
        self.assertEqual({'op': 'update', 'blob': 'files/2', 'path': '2'}, next(diff))
        self.assertEqual({'op': 'commit', 'sequence': [[1, 1]]}, diff.throw(StopIteration))
        self.assertRaises(StopIteration, diff.next)

    def test_Index_diff_Stretch(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        self.utime('files', 1)

        in_seq = util.Sequence([[1, 1], [3, None]])
        diff = seeder.diff(in_seq)
        self.assertEqual({'op': 'update', 'blob': 'files/1', 'path': '1'}, next(diff))
        self.assertEqual({'op': 'update', 'blob': 'files/3', 'path': '3'}, next(diff))
        self.assertEqual({'op': 'commit', 'sequence': [[1, 3]]}, next(diff))
        self.assertRaises(StopIteration, diff.next)

    def test_Index_diff_DoNotStretchContinuesPacket(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch('files/1')
        self.touch('files/2')
        self.touch('files/3')
        self.utime('files', 1)

        in_seq = util.Sequence([[1, 1], [3, None]])
        diff = seeder.diff(in_seq, util.Sequence([[1, 1]]))
        self.assertEqual({'op': 'update', 'blob': 'files/1', 'path': '1'}, next(diff))
        self.assertEqual({'op': 'update', 'blob': 'files/3', 'path': '3'}, next(diff))
        self.assertEqual({'op': 'commit', 'sequence': [[1, 1], [3, 3]]}, next(diff))
        self.assertRaises(StopIteration, diff.next)

    def test_Index_DiffUpdatedFiles(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        for __ in seeder.diff(util.Sequence([[1, None]])):
            pass
        self.assertEqual(3, seqno.value)

        os.utime('files/2', (2, 2))

        self.assertEqual(
                [{'op': 'commit', 'sequence': []}],
                [i for i in seeder.diff(util.Sequence([[4, None]]))])
        self.assertEqual(3, seqno.value)

        os.utime('files', (3, 3))

        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/2', 'path': '2'},
            {'op': 'commit', 'sequence': [[4, 4]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[4, None]]))]))
        self.assertEqual(4, seqno.value)

        os.utime('files/1', (4, 4))
        os.utime('files/3', (4, 4))
        os.utime('files', (4, 4))

        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/1', 'path': '1'},
            {'op': 'update', 'blob': 'files/3', 'path': '3'},
            {'op': 'commit', 'sequence': [[5, 6]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[5, None]]))]))
        self.assertEqual(6, seqno.value)

        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/1', 'path': '1'},
            {'op': 'update', 'blob': 'files/2', 'path': '2'},
            {'op': 'update', 'blob': 'files/3', 'path': '3'},
            {'op': 'commit', 'sequence': [[1, 6]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[1, None]]))]))
        self.assertEqual(6, seqno.value)

    def test_Index_DiffCreatedFiles(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        for __ in seeder.diff(util.Sequence([[1, None]])):
            pass
        self.assertEqual(3, seqno.value)

        self.touch(('files/4', '4'))
        os.utime('files/4', (2, 2))
        os.utime('files', (1, 1))

        self.assertEqual(
                [{'op': 'commit', 'sequence': []}],
                [i for i in seeder.diff(util.Sequence([[4, None]]))])
        self.assertEqual(3, seqno.value)

        os.utime('files/4', (2, 2))
        os.utime('files', (2, 2))

        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/4', 'path': '4'},
            {'op': 'commit', 'sequence': [[4, 4]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[4, None]]))]))
        self.assertEqual(4, seqno.value)

        self.touch(('files/5', '5'))
        os.utime('files/5', (3, 3))
        self.touch(('files/6', '6'))
        os.utime('files/6', (3, 3))
        os.utime('files', (3, 3))

        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/5', 'path': '5'},
            {'op': 'update', 'blob': 'files/6', 'path': '6'},
            {'op': 'commit', 'sequence': [[5, 6]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[5, None]]))]))
        self.assertEqual(6, seqno.value)

    def test_Index_DiffDeletedFiles(self):
        seqno = util.Seqno('seqno')
        seeder = files.Index('files', 'index', seqno)

        self.touch(('files/1', '1'))
        self.touch(('files/2', '2'))
        self.touch(('files/3', '3'))
        self.utime('files', 1)
        os.utime('files', (1, 1))

        for __ in seeder.diff(util.Sequence([[1, None]])):
            pass
        self.assertEqual(3, seqno.value)

        os.unlink('files/2')
        os.utime('files', (2, 2))

        assert seeder.sync()
        self.assertEqual(sorted([
            {'op': 'update', 'blob': 'files/1', 'path': '1'},
            {'op': 'update', 'blob': 'files/3', 'path': '3'},
            {'op': 'delete', 'path': '2'},
            {'op': 'commit', 'sequence': [[1, 4]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[1, None]]))]))
        self.assertEqual(4, seqno.value)

        os.unlink('files/1')
        os.unlink('files/3')
        os.utime('files', (3, 3))

        assert seeder.sync()
        self.assertEqual(sorted([
            {'op': 'delete', 'path': '1'},
            {'op': 'delete', 'path': '2'},
            {'op': 'delete', 'path': '3'},
            {'op': 'commit', 'sequence': [[1, 6]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[1, None]]))]))
        self.assertEqual(6, seqno.value)

        assert not seeder.sync()
        self.assertEqual(sorted([
            {'op': 'delete', 'path': '1'},
            {'op': 'delete', 'path': '2'},
            {'op': 'delete', 'path': '3'},
            {'op': 'commit', 'sequence': [[1, 6]]},
            ]),
            sorted([i for i in seeder.diff(util.Sequence([[1, None]]))]))
        self.assertEqual(6, seqno.value)

    def test_merge_Updated(self):
        self.assertEqual('commit-sequence', files.merge('dst', [
            {'op': 'update', 'path': '1', 'blob': StringIO('1')},
            {'op': 'update', 'path': '2/2', 'blob': StringIO('22')},
            {'op': 'update', 'path': '3/3/3', 'blob': StringIO('333')},
            {'op': 'commit', 'sequence': 'commit-sequence'},
            ]))
        self.assertEqual('1', file('dst/1').read())
        self.assertEqual('22', file('dst/2/2').read())
        self.assertEqual('333', file('dst/3/3/3').read())

    def test_merge_Deleted(self):
        self.touch('dst/1')
        self.touch('dst/2/2')

        self.assertEqual('commit-sequence', files.merge('dst', [
            {'op': 'delete', 'path': '1'},
            {'op': 'delete', 'path': '2/2'},
            {'op': 'delete', 'path': '3/3/3'},
            {'op': 'commit', 'sequence': 'commit-sequence'},
            ]))
        assert not exists('dst/1')
        assert not exists('dst/2/2')
        assert not exists('dst/3/3/3')


if __name__ == '__main__':
    tests.main()
