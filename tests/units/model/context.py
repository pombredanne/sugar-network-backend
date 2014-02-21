#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.db import files
from sugar_network.client import IPCConnection, Connection, keyfile
from sugar_network.model.context import Context
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request
from sugar_network.toolkit import i18n, http, coroutine, enforce


class ContextTest(tests.Test):

    def test_SetCommonLayerForPackages(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(['common'], conn.get(['context', guid, 'layer']))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'layer': 'foo',
            })
        self.assertEqual(['foo', 'common'], conn.get(['context', guid, 'layer']))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'layer': ['common', 'bar'],
            })
        self.assertEqual(['common', 'bar'], conn.get(['context', guid, 'layer']))

    def test_Releases(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        activity_info1 = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        bundle1 = self.zips(('topdir/activity/activity.info', activity_info1))
        release1 = conn.upload(['context', context, 'releases'], StringIO(bundle1))
        assert release1 == str(hash(bundle1))
        self.assertEqual({
            release1: {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'spec': {'*-*': {'bundle': str(hash(bundle1))}},
                    'stability': 'stable',
                    'unpack_size': len(activity_info1),
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert files.get(str(hash(bundle1)))

        activity_info2 = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ])
        bundle2 = self.zips(('topdir/activity/activity.info', activity_info2))
        release2 = conn.upload(['context', context, 'releases'], StringIO(bundle2))
        assert release2 == str(hash(bundle2))
        self.assertEqual({
            release1: {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'spec': {'*-*': {'bundle': str(hash(bundle1))}},
                    'stability': 'stable',
                    'unpack_size': len(activity_info1),
                    },
                },
            release2: {
                'seqno': 7,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'spec': {'*-*': {'bundle': str(hash(bundle2))}},
                    'stability': 'stable',
                    'unpack_size': len(activity_info2),
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert files.get(str(hash(bundle1)))
        assert files.get(str(hash(bundle2)))

        conn.delete(['context', context, 'releases', release1])
        self.assertEqual({
            release1: {
                'seqno': 8,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                },
            release2: {
                'seqno': 7,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'spec': {'*-*': {'bundle': str(hash(bundle2))}},
                    'stability': 'stable',
                    'unpack_size': len(activity_info2),
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert files.get(str(hash(bundle1))) is None
        assert files.get(str(hash(bundle2)))

        conn.delete(['context', context, 'releases', release2])
        self.assertEqual({
            release1: {
                'seqno': 8,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                },
            release2: {
                'seqno': 9,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                },
            }, conn.get(['context', context, 'releases']))
        assert files.get(str(hash(bundle1))) is None
        assert files.get(str(hash(bundle2))) is None

    def test_IncrementReleasesSeqnoOnNewReleases(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        conn.put(['context', context], {
            'summary': 'summary2',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])))
        release = conn.upload(['context', context, 'releases'], StringIO(bundle))
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.releases_seqno.value)

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ])))
        release = conn.upload(['context', context, 'releases'], StringIO(bundle))
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            {'event': 'release', 'seqno': 2},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(2, volume.releases_seqno.value)

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ])))
        release = conn.upload(['context', context, 'releases'], StringIO(bundle))
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            {'event': 'release', 'seqno': 2},
            {'event': 'release', 'seqno': 3},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(3, volume.releases_seqno.value)

        conn.delete(['context', context, 'releases', release])
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            {'event': 'release', 'seqno': 2},
            {'event': 'release', 'seqno': 3},
            {'event': 'release', 'seqno': 4},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(4, volume.releases_seqno.value)

    def test_IncrementReleasesSeqnoOnDependenciesChange(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        conn.put(['context', context], {
            'dependencies': 'dep',
            })
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.releases_seqno.value)

    def test_IncrementReleasesSeqnoOnDeletes(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        conn.put(['context', context], {
            'layer': ['deleted'],
            })
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.releases_seqno.value)

        conn.put(['context', context], {
            'layer': [],
            })
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            {'event': 'release', 'seqno': 2},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(2, volume.releases_seqno.value)

    def test_RestoreReleasesSeqno(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            'dependencies': 'dep',
            })
        self.assertEqual(1, volume.releases_seqno.value)

        volume.close()
        volume = db.Volume('master', [])
        self.assertEqual(1, volume.releases_seqno.value)


if __name__ == '__main__':
    tests.main()
