#!/usr/bin/env python
# sugar-lint: disable

import json
import time

from __init__ import tests

from sugar_network.toolkit.rrd import Rrd
from sugar_network.node.stats_user import stats_user_step, stats_user_rras, diff, merge, commit


class StatsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        stats_user_step.value = 1
        stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']

    def test_diff(self):
        ts = int(time.time())

        rrd = Rrd('stats/user/dir1/user1', stats_user_step.value, stats_user_rras.value)
        rrd['db1'].put({'a': 1}, ts)
        rrd['db1'].put({'a': 2}, ts + 1)

        rrd = Rrd('stats/user/dir1/user2', stats_user_step.value, stats_user_rras.value)
        rrd['db2'].put({'b': 3}, ts)

        rrd = Rrd('stats/user/dir2/user3', stats_user_step.value, stats_user_rras.value)
        rrd['db3'].put({'c': 4}, ts)
        rrd['db4'].put({'d': 5}, ts)

        self.assertEqual([
            {'db': 'db3', 'user': 'user3'},
                {'timestamp': ts, 'values': {'c': 4.0}},
            {'db': 'db4', 'user': 'user3'},
                {'timestamp': ts, 'values': {'d': 5.0}},
            {'db': 'db2', 'user': 'user2'},
                {'timestamp': ts, 'values': {'b': 3.0}},
            {'db': 'db1', 'user': 'user1'},
                {'timestamp': ts, 'values': {'a': 1.0}},
                {'timestamp': ts + 1, 'values': {'a': 2.0}},
            {'commit': {
                'user1': {
                    'db1': [[1, ts + 1]],
                    },
                'user2': {
                    'db2': [[1, ts]],
                    },
                'user3': {
                    'db3': [[1, ts]],
                    'db4': [[1, ts]],
                    },
                }},
            ],
            [i for i in diff()])

    def test_merge(self):
        ts = int(time.time())

        self.assertEqual(
                'info',
                merge([
                    {'db': 'db3', 'user': 'user3'},
                        {'timestamp': ts, 'values': {'c': 4.0}},
                    {'db': 'db4', 'user': 'user3'},
                        {'timestamp': ts, 'values': {'d': 5.0}},
                    {'db': 'db2', 'user': 'user2'},
                        {'timestamp': ts, 'values': {'b': 3.0}},
                    {'db': 'db1', 'user': 'user1'},
                        {'timestamp': ts, 'values': {'a': 1.0}},
                        {'timestamp': ts + 1, 'values': {'a': 2.0}},
                    {'commit': 'info'},
                    ]))

        self.assertEqual([
            [('db1', ts, {'a': 1.0}), ('db1', ts + 1, {'a': 2.0})],
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in Rrd('stats/user/us/user1', 1)])

        self.assertEqual([
            [('db2', ts, {'b': 3.0})],
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in Rrd('stats/user/us/user2', 1)])

        self.assertEqual([
            [('db3', ts, {'c': 4.0})],
            [('db4', ts, {'d': 5.0})],
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in Rrd('stats/user/us/user3', 1)])

    def test_commit(self):
        ts = int(time.time())
        commit({
            'user1': {
                'db1': [[1, ts + 1]],
                },
            'user2': {
                'db2': [[1, ts]],
                },
            'user3': {
                'db3': [[1, ts]],
                'db4': [[1, ts]],
                },
            })

        self.assertEqual(
                [[ts + 2, None]],
                json.load(file('stats/user/us/user1/db1.push')))
        self.assertEqual(
                [[ts + 1, None]],
                json.load(file('stats/user/us/user2/db2.push')))
        self.assertEqual(
                [[ts + 1, None]],
                json.load(file('stats/user/us/user3/db3.push')))
        self.assertEqual(
                [[ts + 1, None]],
                json.load(file('stats/user/us/user3/db4.push')))


if __name__ == '__main__':
    tests.main()
