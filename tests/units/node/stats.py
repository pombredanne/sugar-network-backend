#!/usr/bin/env python
# sugar-lint: disable

import time
from cStringIO import StringIO

from __init__ import tests

from sugar_network.node.auth import RootAuth
from sugar_network.node.model import Volume
from sugar_network.node.stats import Monitor
from sugar_network.toolkit.coroutine import this


class StatsTest(tests.Test):

    def test_StatContexts(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        self.override(time, 'time', lambda: ts)
        guid1 = this.call(method='POST', path=['context'], content={'title': '', 'summary': '', 'description': '', 'type': 'activity'})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        guid2 = this.call(method='POST', path=['context'], content={'title': '', 'summary': '', 'description': '', 'type': 'activity'})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 2.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='DELETE', path=['context', guid1])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 2.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='DELETE', path=['context', guid2])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 2.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 3, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatReleased(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        self.override(time, 'time', lambda: ts)
        guid = this.call(method='POST', path=['context'], content={'title': '', 'summary': '', 'description': '', 'type': 'activity'})
        agg1 = this.call(method='POST', path=['context', guid, 'releases'], content=StringIO(
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = Activity',
                'bundle_id = %s' % guid,
                'exec = true',
                'icon = icon',
                'activity_version = 1',
                'license = Public Domain',
                ])))))
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        agg2 = this.call(method='POST', path=['context', guid, 'releases'], content=StringIO(
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = Activity',
                'bundle_id = %s' % guid,
                'exec = true',
                'icon = icon',
                'activity_version = 2',
                'license = Public Domain',
                ])))))
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 2.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        agg2 = this.call(method='POST', path=['context', guid, 'releases'], content=StringIO(
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = Activity',
                'bundle_id = %s' % guid,
                'exec = true',
                'icon = icon',
                'activity_version = 3',
                'license = Public Domain',
                ])))))
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 2.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 3, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 0.0, 'topics': 3.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatSolved(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        self.override(time, 'time', lambda: ts)
        guid = this.call(method='POST', path=['context'], content={'title': '', 'summary': '', 'description': '', 'type': 'activity'})
        this.call(method='POST', path=['context', guid, 'releases'], content=StringIO(
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = Activity',
                'bundle_id = %s' % guid,
                'exec = true',
                'icon = icon',
                'activity_version = 1',
                'license = Public Domain',
                ])))))
        this.call(method='GET', path=['context', guid], cmd='solve')
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='GET', path=['context', guid], cmd='solve')
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='GET', path=['context', guid], cmd='solve')
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 1.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 3, 'contexts': 1.0, 'released': 0.0, 'reported': 0.0, 'solved': 1.0, 'topics': 1.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatReported(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        self.override(time, 'time', lambda: ts)
        this.call(method='POST', path=['report'], content={'context': 'context', 'error': '', 'lsb_release': {}, 'uname': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='POST', path=['report'], content={'context': 'context', 'error': '', 'lsb_release': {}, 'uname': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='POST', path=['report'], content={'context': 'context', 'error': '', 'lsb_release': {}, 'uname': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 3, 'contexts': 0.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatUsers(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        self.override(time, 'time', lambda: ts)
        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY2})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 2.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='DELETE', path=['user', tests.UID])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 2.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='DELETE', path=['user', tests.UID2])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 2.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 1.0},
            {'timestamp': ts + 3, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatTopics(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        context = this.volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        self.override(time, 'time', lambda: ts)
        guid1 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        guid2 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 2.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='DELETE', path=['post', guid1])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 2.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='DELETE', path=['post', guid2])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 2.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 1.0, 'posts': 0.0, 'users': 0.0},
            {'timestamp': ts + 3, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_StatPosts(self):
        ts = int(time.time())
        this.volume = self.start_master(auth=RootAuth())
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])

        context = this.volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        self.override(time, 'time', lambda: ts)
        guid1 = this.call(method='POST', path=['post'], content={'topic': 'topic', 'context': context, 'type': 'post', 'title': '', 'message': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 1)
        guid2 = this.call(method='POST', path=['post'], content={'topic': 'topic', 'context': context, 'type': 'post', 'title': '', 'message': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 2.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 2)
        this.call(method='DELETE', path=['post', guid1])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 2.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

        self.override(time, 'time', lambda: ts + 3)
        this.call(method='DELETE', path=['post', guid2])
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            {'timestamp': ts + 1, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 2.0, 'users': 0.0},
            {'timestamp': ts + 2, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 1.0, 'users': 0.0},
            {'timestamp': ts + 3, 'contexts': 0.0, 'released': 0.0, 'reported': 0.0, 'solved': 0.0, 'topics': 0.0, 'posts': 0.0, 'users': 0.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))

    def test_ReuseTotalsOnInitialStart(self):
        this.volume = self.start_master(auth=RootAuth())

        context = this.volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        this.call(method='POST', path=['user'], content={'name': '', 'pubkey': tests.PUBKEY})
        this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        this.call(method='POST', path=['post'], content={'topic': 'topic', 'context': context, 'type': 'post', 'title': '', 'message': ''})

        ts = int(time.time())
        self.override(time, 'time', lambda: ts)
        self.node_routes._stats = Monitor(this.volume, 1, ['RRA:AVERAGE:0.5:1:10'])
        this.call(method='POST', path=['report'], content={'context': context, 'error': '', 'lsb_release': {}, 'uname': ''})
        self.node_routes._stats.commit()
        self.assertEqual([
            {'timestamp': ts + 0, 'contexts': 1.0, 'released': 0.0, 'reported': 1.0, 'solved': 0.0, 'topics': 1.0, 'posts': 1.0, 'users': 1.0},
            ],
            this.call(method='GET', cmd='stats', limit=10))


if __name__ == '__main__':
    tests.main()
