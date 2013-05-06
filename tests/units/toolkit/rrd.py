#!/usr/bin/env python
# sugar-lint: disable

import os
import time

import rrdtool

from __init__ import tests

from sugar_network.toolkit import rrd


class RrdTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        rrd._FETCH_PAGE = 100
        rrd._rrdtool = rrdtool

    def test_Db(self):
        ts = int(time.time()) + 100

        rrdtool.create('test.rrd',
                '--start', str(ts),
                '-s', '1',
                'DS:f1:GAUGE:1:2:3',
                'DS:f2:COUNTER:4:5:6',
                'RRA:AVERAGE:0.1:7:8',
                'RRA:LAST:0.2:9:10',
                )

        db = rrd._Db('test.rrd')
        self.assertEqual(1, db.step)
        self.assertEqual(ts, db.last)
        self.assertEqual(2, len(db.fields))
        self.assertEqual(2, len(db.rras))

        self.assertEqual('f1', db.fields[0]['name'])
        self.assertEqual('GAUGE', db.fields[0]['type'])
        self.assertEqual(1, db.fields[0]['minimal_heartbeat'])
        self.assertEqual(2, db.fields[0]['min'])
        self.assertEqual(3, db.fields[0]['max'])

        self.assertEqual('f2', db.fields[1]['name'])
        self.assertEqual('COUNTER', db.fields[1]['type'])
        self.assertEqual(4, db.fields[1]['minimal_heartbeat'])
        self.assertEqual(5, db.fields[1]['min'])
        self.assertEqual(6, db.fields[1]['max'])

        self.assertEqual('RRA:AVERAGE:0.1:7:8', db.rras[0])
        self.assertEqual('RRA:LAST:0.2:9:10', db.rras[1])

    def test_DbSet_load(self):
        rrdtool.create('1.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')
        rrdtool.create('2.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')
        rrdtool.create('3.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')

        dbset = rrd._DbSet('.', None, None, None)
        dbset.load('1.rrd', 1)
        self.assertEqual(
                ['./1.rrd'],
                [i.path for i in dbset._revisions])
        dbset.load('2.rrd' ,2)
        self.assertEqual(
                ['./1.rrd', './2.rrd'],
                [i.path for i in dbset._revisions])
        dbset.load('3.rrd', 3)
        self.assertEqual(
                ['./1.rrd', './2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])

        dbset = rrd._DbSet('.', None, None, None)
        dbset.load('3.rrd', 3)
        self.assertEqual(
                ['./3.rrd'],
                [i.path for i in dbset._revisions])
        dbset.load('2.rrd', 2)
        self.assertEqual(
                ['./2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])
        dbset.load('1.rrd', 1)
        self.assertEqual(
                ['./1.rrd', './2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])

    def test_DbSet_put_ToNewDbAndSkipOlds(self):
        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time())
        dbset.put({'f1': 1, 'f2': 1}, ts)
        __, (f1, f2), values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 3))
        self.assertEqual('f1', f1)
        self.assertEqual('f2', f2)
        assert (1, 1) in values

        dbset.put({'f1': 2, 'f2': 2}, ts)
        ts = int(time.time())
        __, (f1, f2), values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 3))
        assert (2, 2) not in values

        dbset.put({'f1': 3, 'f2': 3}, ts + 1)
        ts = int(time.time())
        __, (f1, f2), values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 3))
        assert (3, 3) in values

    def test_DbSet_put_WithChangedLayout(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0).path)

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        dbset.put({'f1': 2, 'f2': 2}, ts)
        assert dbset._get_db(0) is None

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        self.assertEqual('./test-1.rrd', dbset._get_db(0).path)

        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 10))
        assert (1,) in values
        assert (2, 2) not in values

        __, __, values = rrdtool.fetch('test-1.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 10))
        assert (1,) not in values
        assert (2, 2) in values

    def test_DbSet_put_WithChangedRRA(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0).path)

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.1:1:10'])
        dbset.load('test.rrd', 0)
        dbset.put({'f1': 1}, ts + 1)
        self.assertEqual('./test-1.rrd', dbset._get_db(0).path)

    def test_DbSet_put_WithChangedStep(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0).path)

        dbset = rrd._DbSet('.', 'test', 2, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        dbset.put({'f1': 1}, ts + 2)
        self.assertEqual('./test-1.rrd', dbset._get_db(0).path)

    def test_DbSet_get_OneDb(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        dbset.put({'f1': 3, 'f2': 3}, ts + 2)

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0})],
                [(t, i) for t, i in dbset.get(ts, ts)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 1, {'f1': 2.0, 'f2': 2.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 1)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 1, {'f1': 2.0, 'f2': 2.0}), (ts + 2, {'f1': 3.0, 'f2': 3.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 2)])

    def test_DbSet_get_OneDbLongSteps(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 3, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        dbset.put({'f1': 2, 'f2': 2}, ts + 3)
        dbset.put({'f1': 3, 'f2': 3}, ts + 6)

        ts = ts / 3 * 3

        dbset = rrd._DbSet('.', 'test', 3, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0})],
                [(t, i) for t, i in dbset.get(ts, ts)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 3, {'f1': 2.0, 'f2': 2.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 3)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 3, {'f1': 2.0, 'f2': 2.0}), (ts + 6, {'f1': 3.0, 'f2': 3.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 6)])

    def test_DbSet_get_MultipeDbs(self):
        ts = int(time.time())

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        ts = dbset._get_db(0).last

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.6:1:10'])
        dbset.load('test.rrd', 0)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        dbset.put({'f1': 3, 'f2': 3}, ts + 2)

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.7:1:10'])
        dbset.load('test.rrd', 0)
        dbset.load('test-1.rrd', 1)
        dbset.put({'f1': 4, 'f2': 4}, ts + 3)
        dbset.put({'f1': 5, 'f2': 5}, ts + 4)
        dbset.put({'f1': 6, 'f2': 6}, ts + 5)

        dbset = rrd._DbSet('.', 'test', 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.load('test.rrd', 0)
        dbset.load('test-1.rrd', 1)
        dbset.load('test-2.rrd', 2)
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts)])
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    (ts + 1, {'f1': 2.0, 'f2': 2.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts + 1)])
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    (ts + 1, {'f1': 2.0, 'f2': 2.0}),
                    (ts + 2, {'f1': 3.0, 'f2': 3.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts + 2)])
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    (ts + 1, {'f1': 2.0, 'f2': 2.0}),
                    (ts + 2, {'f1': 3.0, 'f2': 3.0}),
                    (ts + 3, {'f1': 4.0, 'f2': 4.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts + 3)])
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    (ts + 1, {'f1': 2.0, 'f2': 2.0}),
                    (ts + 2, {'f1': 3.0, 'f2': 3.0}),
                    (ts + 3, {'f1': 4.0, 'f2': 4.0}),
                    (ts + 4, {'f1': 5.0, 'f2': 5.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts + 4)])
        self.assertEqual(
                [
                    (ts, {'f1': 1.0, 'f2': 1.0}),
                    (ts + 1, {'f1': 2.0, 'f2': 2.0}),
                    (ts + 2, {'f1': 3.0, 'f2': 3.0}),
                    (ts + 3, {'f1': 4.0, 'f2': 4.0}),
                    (ts + 4, {'f1': 5.0, 'f2': 5.0}),
                    (ts + 5, {'f1': 6.0, 'f2': 6.0}),
                    ],
                [(t, i) for t, i in dbset.get(ts, ts + 5)])

    def test_NoTimestampDupes(self):
        start_ts = int(time.time())
        end_ts = start_ts + 86400 * 3

        dbset = rrd._DbSet('.', 'test', 300, [
            'RRA:AVERAGE:0.5:1:288',
            'RRA:AVERAGE:0.5:3:672',
            'RRA:AVERAGE:0.5:12:744',
            'RRA:AVERAGE:0.5:144:732',
            ])
        for i in xrange((end_ts - start_ts) / 300):
            dbset.put({'f': i}, start_ts + i * 300)

        prev_ts = 0
        prev_value = 0
        for ts, value in dbset.get(start_ts, end_ts, 86400):
            value = value['f']
            assert ts > prev_ts
            assert value > prev_value
            prev_ts = ts
            prev_value = value


if __name__ == '__main__':
    tests.main()
