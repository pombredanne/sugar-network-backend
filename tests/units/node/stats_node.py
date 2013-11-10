#!/usr/bin/env python
# sugar-lint: disable

import time

from __init__ import tests

from sugar_network import db, model
from sugar_network.node.stats_node import stats_node_step, Sniffer
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import Request


class StatsTest(tests.Test):

    def test_InitializeTotals(self):
        volume = db.Volume('local', model.RESOURCES)

        stats = Sniffer(volume)
        self.assertEqual(0, stats._stats['user'].total)
        self.assertEqual(0, stats._stats['context'].total)
        self.assertEqual(0, stats._stats['review'].total)
        self.assertEqual(0, stats._stats['feedback'].total)
        self.assertEqual(0, stats._stats['feedback'].solutions)
        self.assertEqual(0, stats._stats['solution'].total)
        self.assertEqual(0, stats._stats['artifact'].total)

        volume['user'].create({'guid': 'user', 'name': 'user', 'color': '', 'pubkey': ''})
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['review'].create({'guid': 'review', 'context': 'context', 'title': '', 'content': '', 'rating': 5})
        volume['feedback'].create({'guid': 'feedback', 'context': 'context', 'type': 'idea', 'title': '', 'content': ''})
        volume['feedback'].create({'guid': 'feedback2', 'context': 'context', 'type': 'idea', 'title': '', 'content': '', 'solution': 'solution'})
        volume['solution'].create({'guid': 'solution', 'context': 'context', 'feedback': 'feedback', 'content': ''})
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        stats = Sniffer(volume)
        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(2, stats._stats['feedback'].total)
        self.assertEqual(1, stats._stats['feedback'].solutions)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(1, stats._stats['artifact'].total)

    def test_POSTs(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)

        request = Request(method='POST', path=['context'])
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(3, stats._stats['context'].total)

    def test_DELETEs(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)

        request = Request(method='DELETE', path=['context'])
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(-3, stats._stats['context'].total)

    def test_FeedbackSolutions(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['feedback'].create({'guid': 'guid', 'context': 'context', 'type': 'idea', 'title': '', 'content': ''})

        request = Request(method='PUT', path=['feedback', 'guid'])
        request.principal = 'user'
        request.content = {}
        stats.log(request)
        self.assertEqual(0, stats._stats['feedback'].solutions)

        request.content = {'solution': 'solution'}
        stats.log(request)
        self.assertEqual(1, stats._stats['feedback'].solutions)

        request.content = {'solution': None}
        stats.log(request)
        self.assertEqual(1, stats._stats['feedback'].solutions)

        volume['feedback'].update('guid', {'solution': 'exists'})
        request.content = {'solution': None}
        stats.log(request)
        self.assertEqual(0, stats._stats['feedback'].solutions)

    def test_Comments(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['solution'].create({'guid': 'solution', 'context': 'context', 'feedback': 'feedback', 'content': ''})
        volume['feedback'].create({'guid': 'feedback', 'context': 'context', 'type': 'idea', 'title': '', 'content': ''})
        volume['review'].create({'guid': 'review', 'context': 'context', 'title': '', 'content': '', 'rating': 5})

        request = Request(method='POST', path=['comment'])
        request.principal = 'user'
        request.content = {'solution': 'solution'}
        stats.log(request)
        self.assertEqual(1, stats._stats['solution'].commented)

        request = Request(method='POST', path=['comment'])
        request.principal = 'user'
        request.content = {'feedback': 'feedback'}
        stats.log(request)
        self.assertEqual(1, stats._stats['feedback'].commented)

        request = Request(method='POST', path=['comment'])
        request.principal = 'user'
        request.content = {'review': 'review'}
        stats.log(request)
        self.assertEqual(1, stats._stats['review'].commented)

    def test_Reviewes(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'context': 'context', 'rating': 0}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].reviewed)
        self.assertEqual(0, stats._stats['artifact'].reviewed)

        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'context': 'context', 'artifact': '', 'rating': 0}
        stats.log(request)
        self.assertEqual(2, stats._stats['context'].reviewed)
        self.assertEqual(0, stats._stats['artifact'].reviewed)

        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'artifact': 'artifact', 'rating': 0}
        stats.log(request)
        self.assertEqual(2, stats._stats['context'].reviewed)
        self.assertEqual(1, stats._stats['artifact'].reviewed)

    def test_ContextDownloaded(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['implementation'].create({'guid': 'implementation', 'context': 'context', 'license': 'GPLv3', 'version': '1', 'date': 0, 'stability': 'stable', 'notes': ''})

        request = Request(method='GET', path=['implementation', 'implementation', 'fake'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['context'].downloaded)

        request = Request(method='GET', path=['implementation', 'implementation', 'data'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].downloaded)

    def test_ContextReleased(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})

        request = Request(method='POST', path=['implementation'])
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].released)

    def test_ContextFailed(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})

        request = Request(method='POST', path=['report'])
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].failed)

    def test_ArtifactDownloaded(self):
        volume = db.Volume('local', model.RESOURCES)
        stats = Sniffer(volume)
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        request = Request(method='GET', path=['artifact', 'artifact', 'fake'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['artifact'].downloaded)

        request = Request(method='GET', path=['artifact', 'artifact', 'data'])
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['artifact'].downloaded)

    def test_Commit(self):
        stats_node_step.value = 1
        volume = db.Volume('local', model.RESOURCES)
        volume['user'].create({'guid': 'user', 'name': 'user', 'color': '', 'pubkey': ''})
        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['review'].create({'guid': 'review', 'context': 'context', 'title': '', 'content': '', 'rating': 5})
        volume['feedback'].create({'guid': 'feedback', 'context': 'context', 'type': 'idea', 'title': '', 'content': ''})
        volume['solution'].create({'guid': 'solution', 'context': 'context', 'feedback': 'feedback', 'content': ''})
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        stats = Sniffer(volume)
        request = Request(method='GET', path=['user', 'user'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['context', 'context'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['review', 'review'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['feedback', 'feedback'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['solution', 'solution'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', path=['artifact', 'artifact'])
        request.principal = 'user'
        stats.log(request)

        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(1, stats._stats['feedback'].total)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(1, stats._stats['artifact'].total)

        ts = int(time.time())
        stats.commit(ts)

        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(1, stats._stats['feedback'].total)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(1, stats._stats['artifact'].total)

        self.assertEqual([
            [('feedback', ts, {
                'solutions': 0.0,
                'total': 1.0,
                'commented': 0.0,
                })],
            [('review', ts, {
                'total': 1.0,
                'commented': 0.0,
                })],
            [('solution', ts, {
                'total': 1.0,
                'commented': 0.0,
                })],
            [('artifact', ts, {
                'reviewed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                })],
            [('user', ts, {
                'total': 1.0,
                })],
            [('context', ts, {
                'failed': 0.0,
                'reviewed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                'released': 0.0,
                })],
            ],
            [[(j.name,) + i for i in j.get(j.last, j.last)] for j in Rrd('stats/node', 1)])

    def test_CommitContextStats(self):
        stats_node_step.value = 1
        volume = db.Volume('local', model.RESOURCES)

        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['implementation'].create({'guid': 'implementation', 'context': 'context', 'license': 'GPLv3', 'version': '1', 'date': 0, 'stability': 'stable', 'notes': ''})
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        self.assertEqual(0, volume['context'].get('context')['downloads'])
        self.assertEqual([0, 0], volume['context'].get('context')['reviews'])
        self.assertEqual(0, volume['context'].get('context')['rating'])

        stats = Sniffer(volume)
        request = Request(method='GET', path=['implementation', 'implementation', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'context': 'context', 'rating': 5}
        stats.log(request)
        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'artifact': 'artifact', 'rating': 5}
        stats.log(request)
        stats.commit()

        self.assertEqual(1, volume['context'].get('context')['downloads'])
        self.assertEqual([1, 5], volume['context'].get('context')['reviews'])
        self.assertEqual(5, volume['context'].get('context')['rating'])

        stats.commit()

        self.assertEqual(1, volume['context'].get('context')['downloads'])
        self.assertEqual([1, 5], volume['context'].get('context')['reviews'])
        self.assertEqual(5, volume['context'].get('context')['rating'])

        stats = Sniffer(volume)
        request = Request(method='GET', path=['implementation', 'implementation', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'context': 'context', 'rating': 1}
        stats.log(request)
        stats.commit()

        self.assertEqual(2, volume['context'].get('context')['downloads'])
        self.assertEqual([2, 6], volume['context'].get('context')['reviews'])
        self.assertEqual(3, volume['context'].get('context')['rating'])

    def test_CommitArtifactStats(self):
        stats_node_step.value = 1
        volume = db.Volume('local', model.RESOURCES)

        volume['context'].create({'guid': 'context', 'type': 'activity', 'title': '', 'summary': '', 'description': ''})
        volume['artifact'].create({'guid': 'artifact', 'type': 'instance', 'context': 'context', 'title': '', 'description': ''})

        self.assertEqual(0, volume['artifact'].get('artifact')['downloads'])
        self.assertEqual([0, 0], volume['artifact'].get('artifact')['reviews'])
        self.assertEqual(0, volume['artifact'].get('artifact')['rating'])

        stats = Sniffer(volume)
        request = Request(method='GET', path=['artifact', 'artifact', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'artifact': 'artifact', 'rating': 5}
        stats.log(request)
        stats.commit()

        self.assertEqual(1, volume['artifact'].get('artifact')['downloads'])
        self.assertEqual([1, 5], volume['artifact'].get('artifact')['reviews'])
        self.assertEqual(5, volume['artifact'].get('artifact')['rating'])

        stats.commit()

        self.assertEqual(1, volume['artifact'].get('artifact')['downloads'])
        self.assertEqual([1, 5], volume['artifact'].get('artifact')['reviews'])
        self.assertEqual(5, volume['artifact'].get('artifact')['rating'])

        request = Request(method='GET', path=['artifact', 'artifact', 'data'])
        request.principal = 'user'
        stats.log(request)
        request = Request(method='POST', path=['review'])
        request.principal = 'user'
        request.content = {'artifact': 'artifact', 'rating': 1}
        stats.log(request)
        stats.commit()

        self.assertEqual(2, volume['artifact'].get('artifact')['downloads'])
        self.assertEqual([2, 6], volume['artifact'].get('artifact')['reviews'])
        self.assertEqual(3, volume['artifact'].get('artifact')['rating'])


if __name__ == '__main__':
    tests.main()
