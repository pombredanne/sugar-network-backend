#!/usr/bin/env python
# sugar-lint: disable

import time

from __init__ import tests

from sugar_network import db, model
from sugar_network.node.stats_node import Sniffer, stats_node_step
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import Request


class StatsTest(tests.Test):

    def test_InitializeTotals(self):
        volume = db.Volume('local', model.RESOURCES)

        stats = Sniffer(volume, 'stats/node')
        self.assertEqual(0, stats._stats['user']['total'])
        self.assertEqual(0, stats._stats['context']['total'])
        self.assertEqual(0, stats._stats['post']['total'])

        volume['user'].create({'guid': 'user', 'name': 'user', 'pubkey': ''})
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['post'].create({'guid': 'post', 'context': 'context', 'title': '', 'message': '', 'type': 'update'})

        stats = Sniffer(volume, 'stats/node')
        self.assertEqual(1, stats._stats['user']['total'])
        self.assertEqual(1, stats._stats['context']['total'])
        self.assertEqual(1, stats._stats['post']['total'])

    def test_POSTs(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')

        request = Request(method='POST', path=['context'])
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(3, stats._stats['context']['total'])

    def test_DELETEs(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')

        request = Request(method='DELETE', path=['context'])
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(-3, stats._stats['context']['total'])

    def test_Posts(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['post'].create({'guid': 'topic', 'type': 'update', 'context': 'context', 'title': '', 'message': ''})

        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'context': 'context', 'vote': 1, 'type': 'review', 'title': '', 'message': ''}
        stats.log(request)
        self.assertEqual(1, stats._stats['post']['total'])

        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'context': 'context', 'vote': 2, 'type': 'review', 'title': '', 'message': ''}
        stats.log(request)
        self.assertEqual(2, stats._stats['post']['total'])

        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'topic': 'topic', 'vote': 3, 'type': 'feedback', 'title': '', 'message': ''}
        stats.log(request)
        self.assertEqual(3, stats._stats['post']['total'])

        stats.commit_objects()
        self.assertEqual([2, 3], volume['context'].get('context')['reviews'])
        self.assertEqual(2, volume['context'].get('context')['rating'])
        self.assertEqual([1, 3], volume['post'].get('topic')['reviews'])
        self.assertEqual(3, volume['post'].get('topic')['rating'])

    def test_ContextDownloaded(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['release'].create({'guid': 'release', 'context': 'context', 'license': 'GPLv3', 'version': '1', 'date': 0, 'stability': 'stable', 'notes': ''})

        request = Request(method='GET', path=['release', 'release', 'fake'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['context']['downloaded'])

        request = Request(method='GET', path=['release', 'release', 'data'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['context']['downloaded'])

    def test_ContextReleased(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})

        request = Request(method='POST', path=['release'])
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context']['released'])

    def test_ContextFailed(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})

        request = Request(method='POST', path=['report'])
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context']['failed'])

    def test_PostDownloaded(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume, 'stats/node')
        volume['post'].create({'guid': 'topic', 'type': 'object', 'context': 'context', 'title': '', 'message': ''})

        request = Request(method='GET', path=['post', 'topic', 'fake'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['post']['downloaded'])

        request = Request(method='GET', path=['post', 'topic', 'data'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['post']['downloaded'])

    def test_Commit(self):
        volume = db.Volume('local', model.RESOURCES)
        volume['user'].create({'guid': 'user', 'name': 'user', 'pubkey': ''})
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['post'].create({'guid': 'review', 'context': 'context', 'type': 'review', 'title': '', 'message': '', 'vote': 5})

        stats = Sniffer(volume, 'stats/node')
        request = Request(method='GET', path=['user', 'user'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['context', 'context'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['post', 'review'])
        request.principal = 'user'
        stats.log(request)

        self.assertEqual(1, stats._stats['user']['total'])
        self.assertEqual(1, stats._stats['context']['total'])
        self.assertEqual(1, stats._stats['post']['total'])

        ts = int(time.time())
        stats.commit(ts)
        stats.commit_objects()

        self.assertEqual(1, stats._stats['user']['total'])
        self.assertEqual(1, stats._stats['context']['total'])
        self.assertEqual(1, stats._stats['post']['total'])

        self.assertEqual([
            [('post', ts, {
                'downloaded': 0.0,
                'total': 1.0,
                })],
            [('user', ts, {
                'total': 1.0,
                })],
            [('context', ts, {
                'failed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                'released': 0.0,
                })],
            ],
            [[(j.name,) + i for i in j.get(j.last, j.last)] for j in Rrd('stats/node', 1)])

    def test_CommitContextStats(self):
        volume = db.Volume('local', model.RESOURCES)

        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['release'].create({'guid': 'release', 'context': 'context', 'license': 'GPLv3', 'version': '1', 'date': 0, 'stability': 'stable', 'notes': ''})

        self.assertEqual(0, volume['context'].get('context')['downloads'])
        self.assertEqual([0, 0], volume['context'].get('context')['reviews'])
        self.assertEqual(0, volume['context'].get('context')['rating'])

        stats = Sniffer(volume, 'stats/node')
        request = Request(method='GET', path=['release', 'release', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'context': 'context', 'vote': 5, 'type': 'review', 'title': '', 'message': ''}
        stats.log(request)

        stats.commit()
        stats.commit_objects()

        self.assertEqual(1, volume['context'].get('context')['downloads'])
        self.assertEqual([1, 5], volume['context'].get('context')['reviews'])
        self.assertEqual(5, volume['context'].get('context')['rating'])

        stats.commit()
        stats.commit_objects()

        self.assertEqual(1, volume['context'].get('context')['downloads'])
        self.assertEqual([1, 5], volume['context'].get('context')['reviews'])
        self.assertEqual(5, volume['context'].get('context')['rating'])

        stats = Sniffer(volume, 'stats/node')
        request = Request(method='GET', path=['release', 'release', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'context': 'context', 'vote': 1, 'type': 'review', 'title': '', 'message': ''}
        stats.log(request)
        stats.commit()
        stats.commit_objects()

        self.assertEqual(2, volume['context'].get('context')['downloads'])
        self.assertEqual([2, 6], volume['context'].get('context')['reviews'])
        self.assertEqual(3, volume['context'].get('context')['rating'])

    def test_CommitTopicStats(self):
        volume = db.Volume('local', model.RESOURCES)

        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['post'].create({'guid': 'topic', 'type': 'object', 'context': 'context', 'title': '', 'message': ''})

        self.assertEqual(0, volume['post'].get('topic')['downloads'])
        self.assertEqual([0, 0], volume['post'].get('topic')['reviews'])
        self.assertEqual(0, volume['post'].get('topic')['rating'])

        stats = Sniffer(volume, 'stats/node')
        request = Request(method='GET', path=['post', 'topic', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'topic': 'topic', 'vote': 5, 'type': 'feedback'}
        stats.log(request)
        stats.commit()
        stats.commit_objects()

        self.assertEqual(1, volume['post'].get('topic')['downloads'])
        self.assertEqual([1, 5], volume['post'].get('topic')['reviews'])
        self.assertEqual(5, volume['post'].get('topic')['rating'])

        stats.commit()
        stats.commit_objects()

        self.assertEqual(1, volume['post'].get('topic')['downloads'])
        self.assertEqual([1, 5], volume['post'].get('topic')['reviews'])
        self.assertEqual(5, volume['post'].get('topic')['rating'])

        request = Request(method='GET', path=['post', 'topic', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['post'])
        request.principal = 'user'
        request.content = {'topic': 'topic', 'vote': 1, 'type': 'feedback'}
        stats.log(request)
        stats.commit()
        stats.commit_objects()

        self.assertEqual(2, volume['post'].get('topic')['downloads'])
        self.assertEqual([2, 6], volume['post'].get('topic')['reviews'])
        self.assertEqual(3, volume['post'].get('topic')['rating'])

    def test_Suspend(self):
        stats_node_step.value = 5
        volume = db.Volume('local', model.RESOURCES)
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['release'].create({'guid': 'impl', 'context': 'context', 'license': 'GPLv3', 'version': '1', 'date': 0, 'stability': 'stable', 'notes': ''})

        ts = self.ts = 1000000000
        self.override(time, 'time', lambda: self.ts)

        stats = Sniffer(volume, 'stats')
        request = Request(method='POST', path=['context'])
        stats.log(request)
        request = Request(method='GET', path=['release', 'impl', 'data'], context='context')
        stats.log(request)
        stats.suspend()

        rdb = Rrd('stats', 1)['context']
        self.assertEqual([
            ],
            [i for i in rdb.get(ts, ts + 10)])

        stats = Sniffer(volume, 'stats')
        stats.suspend()

        rdb = Rrd('stats', 1)['context']
        self.assertEqual([
            ],
            [i for i in rdb.get(ts, ts + 10)])

        self.ts += 6
        stats = Sniffer(volume, 'stats')

        rdb = Rrd('stats', 1)['context']
        self.assertEqual([
            (ts + 0, {'failed': 0.0, 'downloaded': 0.0, 'total': 0.0, 'released': 0.0}),
            (ts + 5, {'failed': 0.0, 'downloaded': 1.0, 'total': 2.0, 'released': 0.0}),
            ],
            [i for i in rdb.get(ts, ts + 20)])

        request = Request(method='POST', path=['context'])
        stats.log(request)
        request = Request(method='GET', path=['release', 'impl', 'data'], context='context')
        stats.log(request)
        request = Request(method='GET', path=['release', 'impl', 'data'], context='context')
        stats.log(request)
        stats.suspend()

        stats = Sniffer(volume, 'stats')
        stats.suspend()

        rdb = Rrd('stats', 1)['context']
        self.assertEqual([
            (ts + 0, {'failed': 0.0, 'downloaded': 0.0, 'total': 0.0, 'released': 0.0}),
            (ts + 5, {'failed': 0.0, 'downloaded': 1.0, 'total': 2.0, 'released': 0.0}),
            ],
            [i for i in rdb.get(ts, ts + 10)])

        self.ts += 6
        stats = Sniffer(volume, 'stats')

        rdb = Rrd('stats', 1)['context']
        self.assertEqual([
            (ts + 0, {'failed': 0.0, 'downloaded': 0.0, 'total': 0.0, 'released': 0.0}),
            (ts + 5, {'failed': 0.0, 'downloaded': 1.0, 'total': 2.0, 'released': 0.0}),
            (ts + 10, {'failed': 0.0, 'downloaded': 3.0, 'total': 3.0, 'released': 0.0}),
            ],
            [i for i in rdb.get(ts, ts + 20)])


if __name__ == '__main__':
    tests.main()
