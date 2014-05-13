#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json

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
        self.assertEqual([
            'DS:f1:GAUGE:1:U:U',
            'DS:f2:COUNTER:4:U:U'
            ],
            db.fields)
        self.assertEqual([
            'RRA:AVERAGE:0.1:7:8',
            'RRA:LAST:0.2:9:10',
            ],
            db.rras)

    def test_load(self):
        rrdtool.create('1.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')
        rrdtool.create('2.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')
        rrdtool.create('3.rrd', 'DS:f:GAUGE:1:2:3', 'RRA:AVERAGE:0.1:7:8')

        dbset = rrd.Rrd('.', None, None, None, None)
        dbset._load('1.rrd', 1)
        self.assertEqual(
                ['./1.rrd'],
                [i.path for i in dbset._revisions])
        dbset._load('2.rrd' ,2)
        self.assertEqual(
                ['./1.rrd', './2.rrd'],
                [i.path for i in dbset._revisions])
        dbset._load('3.rrd', 3)
        self.assertEqual(
                ['./1.rrd', './2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])

        dbset = rrd.Rrd('.', None, None, None, None)
        dbset._load('3.rrd', 3)
        self.assertEqual(
                ['./3.rrd'],
                [i.path for i in dbset._revisions])
        dbset._load('2.rrd', 2)
        self.assertEqual(
                ['./2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])
        dbset._load('1.rrd', 1)
        self.assertEqual(
                ['./1.rrd', './2.rrd', './3.rrd'],
                [i.path for i in dbset._revisions])

    def test_put_WithChangedLayout(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0, {}).path)

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset._load('test.rrd', 0)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        self.assertEqual('./test-1.rrd', dbset._get_db(0, {}).path)

        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 10))
        assert (1,) in values
        assert (2, 2) not in values

        __, __, values = rrdtool.fetch('test-1.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 10))
        assert (1,) not in values
        assert (2, 2) in values

    def test_put_WithChangedRRA(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0, {}).path)

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.1:1:10'])
        dbset._load('test.rrd', 0)
        dbset.put({'f1': 1}, ts + 1)
        self.assertEqual('./test-1.rrd', dbset._get_db(0, {}).path)

    def test_put_WithChangedStep(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1}, ts)
        self.assertEqual('./test.rrd', dbset._get_db(0, {}).path)

        dbset = rrd.Rrd('.', 'test', None, 2, ['RRA:AVERAGE:0.5:1:10'])
        dbset._load('test.rrd', 0)
        dbset.put({'f1': 1}, ts + 2)
        self.assertEqual('./test-1.rrd', dbset._get_db(0, {}).path)

    def test_get_OneDb(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        dbset.put({'f1': 3, 'f2': 3}, ts + 2)

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset._load('test.rrd', 0)
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0})],
                [(t, i) for t, i in dbset.get(ts, ts)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 1, {'f1': 2.0, 'f2': 2.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 1)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 1, {'f1': 2.0, 'f2': 2.0}), (ts + 2, {'f1': 3.0, 'f2': 3.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 2)])

    def test_get_OneDbLongSteps(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        dbset.put({'f1': 2, 'f2': 2}, ts + 3)
        dbset.put({'f1': 3, 'f2': 3}, ts + 6)

        ts = ts / 3 * 3

        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])
        dbset._load('test.rrd', 0)
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0})],
                [(t, i) for t, i in dbset.get(ts, ts)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 3, {'f1': 2.0, 'f2': 2.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 3)])
        self.assertEqual(
                [(ts, {'f1': 1.0, 'f2': 1.0}), (ts + 3, {'f1': 2.0, 'f2': 2.0}), (ts + 6, {'f1': 3.0, 'f2': 3.0})],
                [(t, i) for t, i in dbset.get(ts, ts + 6)])

    def test_get_MultipeDbs(self):
        ts = int(time.time())

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset.put({'f1': 1, 'f2': 1}, ts)
        ts = dbset._get_db(0, {}).last

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.6:1:10'])
        dbset._load('test.rrd', 0)
        dbset.put({'f1': 2, 'f2': 2}, ts + 1)
        dbset.put({'f1': 3, 'f2': 3}, ts + 2)

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.7:1:10'])
        dbset._load('test.rrd', 0)
        dbset._load('test-1.rrd', 1)
        dbset.put({'f1': 4, 'f2': 4}, ts + 3)
        dbset.put({'f1': 5, 'f2': 5}, ts + 4)
        dbset.put({'f1': 6, 'f2': 6}, ts + 5)

        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])
        dbset._load('test.rrd', 0)
        dbset._load('test-1.rrd', 1)
        dbset._load('test-2.rrd', 2)
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

        dbset = rrd.Rrd('.', 'test', None, 300, [
            'RRA:AVERAGE:0.5:1:288',
            'RRA:AVERAGE:0.5:3:672',
            'RRA:AVERAGE:0.5:12:744',
            'RRA:AVERAGE:0.5:144:732',
            ])
        for i in xrange((end_ts - start_ts) / 300):
            dbset.put({'f': i}, start_ts + i * 300)

        prev_ts = -1
        prev_value = -1
        for ts, value in dbset.get(start_ts, end_ts, 86400):
            value = value['f']
            assert ts > prev_ts
            assert value > prev_value
            prev_ts = ts
            prev_value = value

    def test_PendingPuts(self):
        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time()) / 3 * 3
        dbset.put({'f': 1}, ts)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset.put({'f': 2}, ts + 1)
        self.assertEqual({
            'first': ts,
            'pending': {'f': 2},
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset.put({'f': 3}, ts + 2)
        self.assertEqual({
            'first': ts,
            'pending': {'f': 3},
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset.put({'f': 4}, ts + 3)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (4.0,),
            ],
            values)

    def test_SavePendingPuts(self):
        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time()) / 3 * 3
        dbset.put({'f': 1}, ts)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 5))
        self.assertEqual([
            (1.0,), (None,), (None,),
            ],
            values)

        dbset.put({'f': 2}, ts + 1)
        self.assertEqual({
            'first': ts,
            'pending': {'f': 2},
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 5))
        self.assertEqual([
            (1.0,), (None,), (None, ),
            ],
            values)

        dbset.put({'f': 3}, ts + 6)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 5))
        self.assertEqual([
            (1.0,), (2.0,), (3.0,),
            ],
            values)

    def test_PendingPutsAfterOnStartup(self):
        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time()) / 3 * 3
        dbset.put({'f': 1}, ts)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        dbset.put({'f': 2}, ts + 1)
        self.assertEqual({
            'first': ts,
            'pending': {'f': 2},
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        dbset.put({'f': 3}, ts + 2)
        self.assertEqual({
            'first': ts,
            'pending': {'f': 3},
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)

        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        dbset.put({'f': 4}, ts + 3)
        self.assertEqual({
            'first': ts,
            },
            json.load(file('test.meta')))
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (4.0,),
            ],
            values)

    def test_GetPendingValues(self):
        dbset = rrd.Rrd('.', 'test', None, 3, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time()) / 3 * 3 - 3
        dbset.put({'f': 1}, ts)
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)
        self.assertEqual({'f': 1.0}, dbset.values())
        self.assertEqual({'f': 1.0}, dbset.values(ts))

        dbset.put({'f': 2}, ts + 1)
        __, __, values = rrdtool.fetch('test.rrd', 'AVERAGE', '-s', str(ts - 1), '-e', str(ts + 2))
        self.assertEqual([
            (1.0,), (None,),
            ],
            values)
        self.assertEqual({'f': 1.0}, dbset.values())
        self.assertEqual({'f': 2.0}, dbset.values(ts))
        self.assertEqual({'f': 2.0}, dbset.values(ts + 1))
        self.assertEqual({'f': 2.0}, dbset.values(ts + 2))
        self.assertEqual({'f': 2.0}, dbset.values(ts + 3))
        self.assertEqual({'f': 0.0}, dbset.values(ts + 4))

    def test_GetValues(self):
        dbset = rrd.Rrd('.', 'test', None, 1, ['RRA:AVERAGE:0.5:1:10'])

        ts = int(time.time())
        dbset.put({'f': 1}, ts + 0)
        dbset.put({'f': 2}, ts + 1)
        dbset.put({'f': 3}, ts + 2)

        self.assertEqual({'f': 3.0}, dbset.values())
        self.assertEqual({'f': 1.0}, dbset.values(ts + 0))
        self.assertEqual({'f': 2.0}, dbset.values(ts + 1))
        self.assertEqual({'f': 3.0}, dbset.values(ts + 2))
        self.assertEqual({'f': 0.0}, dbset.values(ts + 3))


if __name__ == '__main__':
    tests.main()
