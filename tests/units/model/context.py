#!/usr/bin/env python
# sugar-lint: disable

import hashlib
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.db import blobs
from sugar_network.client import IPCConnection, Connection, keyfile
from sugar_network.client.auth import SugarCreds
from sugar_network.model.context import Context
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request
from sugar_network.toolkit.sugar import color_svg
from sugar_network.toolkit import svg_to_png, i18n, http, coroutine, enforce


class ContextTest(tests.Test):

    def test_PackageImages(self):
        volume = self.start_master()
        conn = Connection(creds=SugarCreds(keyfile.value))

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
        conn = Connection(creds=SugarCreds(keyfile.value))

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
        conn = Connection(creds=SugarCreds(keyfile.value))

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
        assert release1 == str(hashlib.sha1(bundle1).hexdigest())
        self.assertEqual({
            release1: {
                'seqno': 9,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists

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
                'seqno': 9,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
                    },
                },
            release2: {
                'seqno': 12,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists

        conn.delete(['context', context, 'releases', release1])
        self.assertEqual({
            release1: {
                'seqno': 14,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            release2: {
                'seqno': 12,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert not volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists

        conn.delete(['context', context, 'releases', release2])
        self.assertEqual({
            release1: {
                'seqno': 14,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            release2: {
                'seqno': 16,
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            }, volume['context'][context]['releases'])
        assert not volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert not volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists


if __name__ == '__main__':
    tests.main()
