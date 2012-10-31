#!/usr/bin/env python
# sugar-lint: disable

import time

from __init__ import tests

from sugar_network.toolkit.rrd import ReadOnlyRrd
from sugar_network.node.stats import stats_node_step, NodeStats
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.review import Review
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.solution import Solution
from sugar_network.resources.volume import Volume, Request


class StatsTest(tests.Test):

    def test_DoNotLogAnonymouses(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='GET', document='context', guid='guid')
        stats.log(request)
        self.assertEqual(0, stats._stats['context'].viewed)

        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].viewed)

    def test_DoNotLogCmds(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='GET', document='context', guid='guid', cmd='probe')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['context'].viewed)

        del request['cmd']
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].viewed)

    def test_InitializeTotals(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])

        stats = NodeStats(volume)
        self.assertEqual(0, stats._stats['user'].total)
        self.assertEqual(0, stats._stats['context'].total)
        self.assertEqual(0, stats._stats['review'].total)
        self.assertEqual(0, stats._stats['feedback'].total)
        self.assertEqual(0, stats._stats['feedback'].solutions)
        self.assertEqual(0, stats._stats['solution'].total)
        self.assertEqual(0, stats._stats['artifact'].total)

        volume['user'].create(guid='user', name='user', color='', pubkey='')
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')
        volume['review'].create(guid='review', context='context', title='', content='', rating=5)
        volume['feedback'].create(guid='feedback', context='context', type='idea', title='', content='')
        volume['feedback'].create(guid='feedback2', context='context', type='idea', title='', content='', solution='solution')
        volume['solution'].create(guid='solution', context='context', feedback='feedback', content='')
        volume['artifact'].create(guid='artifact', context='context', title='', description='')

        stats = NodeStats(volume)
        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(2, stats._stats['feedback'].total)
        self.assertEqual(1, stats._stats['feedback'].solutions)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(1, stats._stats['artifact'].total)

    def test_POSTs(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='POST', document='context')
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(3, stats._stats['context'].total)
        self.assertEqual(3, stats._stats['context'].created)
        self.assertEqual(0, stats._stats['context'].updated)
        self.assertEqual(0, stats._stats['context'].deleted)

    def test_PUTs(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='PUT', document='context', guid='guid')
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(0, stats._stats['context'].total)
        self.assertEqual(0, stats._stats['context'].created)
        self.assertEqual(3, stats._stats['context'].updated)
        self.assertEqual(0, stats._stats['context'].deleted)

    def test_DELETEs(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='DELETE', document='context')
        request.principal = 'user'
        stats.log(request)
        stats.log(request)
        stats.log(request)
        self.assertEqual(-3, stats._stats['context'].total)
        self.assertEqual(0, stats._stats['context'].created)
        self.assertEqual(0, stats._stats['context'].updated)
        self.assertEqual(3, stats._stats['context'].deleted)

    def test_GETs(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='GET', document='user')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['user'].viewed)

    def test_GETsDocument(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='GET', document='user', guid='user')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['user'].viewed)

    def test_FeedbackSolutions(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)

        request = Request(method='PUT', document='feedback', guid='guid')
        request.principal = 'user'
        request.content = {}
        stats.log(request)
        self.assertEqual(1, stats._stats['feedback'].updated)
        self.assertEqual(0, stats._stats['feedback'].rejected)
        self.assertEqual(0, stats._stats['feedback'].solved)
        self.assertEqual(0, stats._stats['feedback'].solutions)

        request.content = {'solution': 'solution'}
        stats.log(request)
        self.assertEqual(2, stats._stats['feedback'].updated)
        self.assertEqual(0, stats._stats['feedback'].rejected)
        self.assertEqual(1, stats._stats['feedback'].solved)
        self.assertEqual(1, stats._stats['feedback'].solutions)

        request.content = {'solution': None}
        stats.log(request)
        self.assertEqual(3, stats._stats['feedback'].updated)
        self.assertEqual(1, stats._stats['feedback'].rejected)
        self.assertEqual(1, stats._stats['feedback'].solved)
        self.assertEqual(0, stats._stats['feedback'].solutions)

    def test_Comments(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)
        volume['solution'].create(guid='solution', context='context', feedback='feedback', content='')
        volume['feedback'].create(guid='feedback', context='context', type='idea', title='', content='')
        volume['review'].create(guid='review', context='context', title='', content='', rating=5)

        request = Request(method='POST', document='comment')
        request.principal = 'user'
        request.content = {'solution': 'solution'}
        stats.log(request)
        self.assertEqual(1, stats._stats['solution'].commented)

        request = Request(method='POST', document='comment')
        request.principal = 'user'
        request.content = {'feedback': 'feedback'}
        stats.log(request)
        self.assertEqual(1, stats._stats['feedback'].commented)

        request = Request(method='POST', document='comment')
        request.principal = 'user'
        request.content = {'review': 'review'}
        stats.log(request)
        self.assertEqual(1, stats._stats['review'].commented)

    def test_Reviewes(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')
        volume['artifact'].create(guid='artifact', context='context', title='', description='')

        request = Request(method='POST', document='review')
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].reviewed)

        request = Request(method='POST', document='review')
        request.principal = 'user'
        request.content = {'artifact': 'artifact'}
        stats.log(request)
        self.assertEqual(1, stats._stats['artifact'].reviewed)

    def test_ContextDownloaded(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact, Implementation])
        stats = NodeStats(volume)
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')
        volume['implementation'].create(guid='implementation', context='context', license='GPLv3', version='1', date=0, stability='stable', notes='')

        request = Request(method='GET', document='implementation', guid='implementation', prop='fake')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['context'].downloaded)

        request = Request(method='GET', document='implementation', guid='implementation', prop='data')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].downloaded)

    def test_ContextReleased(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact, Implementation])
        stats = NodeStats(volume)
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')

        request = Request(method='POST', document='implementation')
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].released)

    def test_ContextFailed(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact, Implementation])
        stats = NodeStats(volume)
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')

        request = Request(method='POST', document='report')
        request.principal = 'user'
        request.content = {'context': 'context'}
        stats.log(request)
        self.assertEqual(1, stats._stats['context'].failed)

    def test_ContextActive(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact, Implementation])
        stats = NodeStats(volume)

        request = Request(method='PUT', document='context', guid='1')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(
                set(['1']),
                stats._stats['context'].active)

        request = Request(method='GET', document='artifact', context='2')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(
                set(['1', '2']),
                stats._stats['context'].active)

        volume['artifact'].create(guid='artifact', context='3', title='', description='')
        request = Request(method='GET', document='review', artifact='artifact')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(
                set(['1', '2', '3']),
                stats._stats['context'].active)

        volume['feedback'].create(guid='feedback', context='4', type='idea', title='', content='')
        request = Request(method='GET', document='solution', feedback='feedback')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(
                set(['1', '2', '3', '4']),
                stats._stats['context'].active)

        request = Request(method='GET', document='context', guid='5')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(
                set(['1', '2', '3', '4', '5']),
                stats._stats['context'].active)

        request = Request(method='POST', document='report')
        request.principal = 'user'
        request.content = {'context': '6'}
        stats.log(request)
        self.assertEqual(
                set(['1', '2', '3', '4', '5', '6']),
                stats._stats['context'].active)

        volume['solution'].create(guid='solution', context='7', feedback='feedback', content='')
        request = Request(method='POST', document='comment')
        request.principal = 'user'
        request.content = {'solution': 'solution'}
        stats.log(request)
        self.assertEqual(
                set(['1', '2', '3', '4', '5', '6', '7']),
                stats._stats['context'].active)

    def test_UserActive(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact, Implementation])
        stats = NodeStats(volume)

        request = Request(method='GET', document='user')
        request.principal = '1'
        stats.log(request)
        self.assertEqual(
                set(['1']),
                stats._stats['user'].active)
        self.assertEqual(
                set([]),
                stats._stats['user'].effective)

        request = Request(method='POST', document='user')
        request.principal = '2'
        stats.log(request)
        self.assertEqual(
                set(['1', '2']),
                stats._stats['user'].active)
        self.assertEqual(
                set(['2']),
                stats._stats['user'].effective)

    def test_ArtifactDownloaded(self):
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)
        volume['artifact'].create(guid='artifact', context='context', title='', description='')

        request = Request(method='GET', document='artifact', guid='artifact', prop='fake')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['artifact'].viewed)
        self.assertEqual(0, stats._stats['artifact'].downloaded)

        request = Request(method='GET', document='artifact', guid='artifact', prop='data')
        request.principal = 'user'
        stats.log(request)
        self.assertEqual(0, stats._stats['artifact'].viewed)
        self.assertEqual(1, stats._stats['artifact'].downloaded)

    def test_Commit(self):
        stats_node_step.value = 1
        volume = Volume('local', [User, Context, Review, Feedback, Solution, Artifact])
        volume['user'].create(guid='user', name='user', color='', pubkey='')
        volume['context'].create(guid='context', type='activity', title='', summary='', description='')
        volume['review'].create(guid='review', context='context', title='', content='', rating=5)
        volume['feedback'].create(guid='feedback', context='context', type='idea', title='', content='')
        volume['solution'].create(guid='solution', context='context', feedback='feedback', content='')
        volume['artifact'].create(guid='artifact', context='context', title='', description='')

        stats = NodeStats(volume)
        request = Request(method='GET', document='user', guid='user')
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', document='context', guid='context')
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', document='review', guid='review')
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', document='feedback', guid='feedback')
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', document='solution', guid='solution')
        request.principal = 'user'
        stats.log(request)
        request = Request(method='GET', document='artifact', guid='artifact')
        request.principal = 'user'
        stats.log(request)

        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(1, stats._stats['user'].viewed)
        self.assertEqual(1, stats._stats['user'].viewed)
        self.assertEqual(set(['user']), stats._stats['user'].active)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(1, stats._stats['context'].viewed)
        self.assertEqual(set(['context']), stats._stats['context'].active)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(1, stats._stats['review'].viewed)
        self.assertEqual(1, stats._stats['feedback'].total)
        self.assertEqual(1, stats._stats['feedback'].viewed)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(1, stats._stats['solution'].viewed)
        self.assertEqual(1, stats._stats['artifact'].total)
        self.assertEqual(1, stats._stats['artifact'].viewed)

        ts = int(time.time())
        stats.commit(ts)

        self.assertEqual(1, stats._stats['user'].total)
        self.assertEqual(0, stats._stats['user'].viewed)
        self.assertEqual(set(), stats._stats['user'].active)
        self.assertEqual(1, stats._stats['context'].total)
        self.assertEqual(0, stats._stats['context'].viewed)
        self.assertEqual(set(), stats._stats['context'].active)
        self.assertEqual(1, stats._stats['review'].total)
        self.assertEqual(0, stats._stats['review'].viewed)
        self.assertEqual(1, stats._stats['feedback'].total)
        self.assertEqual(0, stats._stats['feedback'].viewed)
        self.assertEqual(1, stats._stats['solution'].total)
        self.assertEqual(0, stats._stats['solution'].viewed)
        self.assertEqual(1, stats._stats['artifact'].total)
        self.assertEqual(0, stats._stats['artifact'].viewed)

        rrd = ReadOnlyRrd('stats/node')

        self.assertEqual([
            [('feedback', ts, {
                'updated': 0.0,
                'created': 0.0,
                'deleted': 0.0,
                'rejected': 0.0,
                'solved': 0.0,
                'solutions': 0.0,
                'total': 1.0,
                'commented': 0.0,
                'viewed': 1.0,
                })],
            [('review', ts, {
                'updated': 0.0,
                'created': 0.0,
                'deleted': 0.0,
                'total': 1.0,
                'commented': 0.0,
                'viewed': 1.0,
                })],
            [('solution', ts, {
                'updated': 0.0,
                'created': 0.0,
                'deleted': 0.0,
                'total': 1.0,
                'commented': 0.0,
                'viewed': 1.0,
                })],
            [('artifact', ts, {
                'updated': 0.0,
                'created': 0.0,
                'deleted': 0.0,
                'reviewed': 0.0,
                'downloaded': 0.0,
                'total': 1.0,
                'viewed': 1.0,
                })],
            [('user', ts, {
                'updated': 0.0,
                'effective': 0.0,
                'created': 0.0,
                'deleted': 0.0,
                'active': 1.0,
                'total': 1.0,
                'viewed': 1.0,
                })],
            [('context', ts, {
                'updated': 0.0,
                'failed': 0.0,
                'deleted': 0.0,
                'created': 0.0,
                'reviewed': 0.0,
                'downloaded': 0.0,
                'viewed': 1.0,
                'active': 1.0,
                'total': 1.0,
                'released': 0.0,
                })],
            ],
            [[(name,) + i for i in rrd.get(name, start, end)] for name, start, end in rrd.dbs])


if __name__ == '__main__':
    tests.main()
