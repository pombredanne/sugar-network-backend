#!/usr/bin/env python
# sugar-lint: disable

import hashlib
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.db import blobs
from sugar_network.client import IPCConnection, Connection, keyfile
from sugar_network.model.context import Context
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request
from sugar_network.toolkit.sugar import color_svg
from sugar_network.toolkit import svg_to_png, i18n, http, coroutine, enforce


class ContextTest(tests.Test):

    def test_PackageImages(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        assert conn.request('GET', ['context', guid, 'artefact_icon']).content == file(volume.blobs.get('assets/package.svg').path).read()
        assert conn.request('GET', ['context', guid, 'icon']).content == file(volume.blobs.get('assets/package.png').path).read()
        assert conn.request('GET', ['context', guid, 'logo']).content == file(volume.blobs.get('assets/package-logo.png').path).read()

    def test_ContextImages(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        svg = color_svg(file(volume.blobs.get('assets/activity.svg').path).read(), guid)
        assert conn.request('GET', ['context', guid, 'artefact_icon']).content == svg
        assert conn.request('GET', ['context', guid, 'icon']).content == svg_to_png(svg, 55).getvalue()
        assert conn.request('GET', ['context', guid, 'logo']).content == svg_to_png(svg, 140).getvalue()

        guid = conn.post(['context'], {
            'type': 'book',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        svg = color_svg(file(volume.blobs.get('assets/book.svg').path).read(), guid)
        assert conn.request('GET', ['context', guid, 'artefact_icon']).content == svg
        assert conn.request('GET', ['context', guid, 'icon']).content == svg_to_png(svg, 55).getvalue()
        assert conn.request('GET', ['context', guid, 'logo']).content == svg_to_png(svg, 140).getvalue()

        guid = conn.post(['context'], {
            'type': 'group',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        svg = color_svg(file(volume.blobs.get('assets/group.svg').path).read(), guid)
        assert conn.request('GET', ['context', guid, 'artefact_icon']).content == svg
        assert conn.request('GET', ['context', guid, 'icon']).content == svg_to_png(svg, 55).getvalue()
        assert conn.request('GET', ['context', guid, 'logo']).content == svg_to_png(svg, 140).getvalue()

    def test_Releases(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        return

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
        assert release1 == str(hashlib.sha1(bundle1).hexdigest())
        self.assertEqual({
            release1: {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'command': 'true',
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert blobs.get(str(hashlib.sha1(bundle1).hexdigest()))

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
        assert release2 == str(hashlib.sha1(bundle2).hexdigest())
        self.assertEqual({
            release1: {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'command': 'true',
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
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
                    'command': 'true',
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert blobs.get(str(hashlib.sha1(bundle1).hexdigest()))
        assert blobs.get(str(hashlib.sha1(bundle2).hexdigest()))

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
                    'command': 'true',
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, conn.get(['context', context, 'releases']))
        assert blobs.get(str(hashlib.sha1(bundle1).hexdigest())) is None
        assert blobs.get(str(hashlib.sha1(bundle2).hexdigest()))

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
        assert blobs.get(str(hashlib.sha1(bundle1).hexdigest())) is None
        assert blobs.get(str(hashlib.sha1(bundle2).hexdigest())) is None

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
