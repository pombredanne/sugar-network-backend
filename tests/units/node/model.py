#!/usr/bin/env python
# sugar-lint: disable

import os
import time

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection, keyfile, api_url
from sugar_network.model.user import User
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node import model, obs
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request, Router
from sugar_network.toolkit import spec, i18n, http, coroutine, enforce


class ModelTest(tests.Test):

    def test_IncrementReleasesSeqno(self):
        events = []
        volume = self.start_master([User, model.Context, Post])
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'group',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        aggid = conn.post(['context', context, 'releases'], -1)
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.releases_seqno.value)

    def test_diff(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid1 = conn.post(['document'], {'prop': 'a'})
        self.utime('master/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = conn.post(['document'], {'prop': 'b'})
        self.utime('master/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = toolkit.Sequence([[1, None]])
        self.assertEqual([
            {'resource': 'document'},
            {'guid': guid1,
                'diff': {
                    'guid': {'value': guid1, 'mtime': 1},
                    'mtime': {'value': 0, 'mtime': 1},
                    'ctime': {'value': 0, 'mtime': 1},
                    'prop': {'value': 'a', 'mtime': 1},
                    'author': {'mtime': 1, 'value': {}},
                    'layer': {'mtime': 1, 'value': []},
                    'tags': {'mtime': 1, 'value': []},
                    },
                },
            {'guid': guid2,
                'diff': {
                    'guid': {'value': guid2, 'mtime': 2},
                    'mtime': {'value': 0, 'mtime': 2},
                    'ctime': {'value': 0, 'mtime': 2},
                    'prop': {'value': 'b', 'mtime': 2},
                    'author': {'mtime': 2, 'value': {}},
                    'layer': {'mtime': 2, 'value': []},
                    'tags': {'mtime': 2, 'value': []},
                    },
                },
            {'commit': [[1, 2]]},
            ],
            [i for i in model.diff(volume, in_seq)])
        self.assertEqual([[1, None]], in_seq)

    def test_diff_Partial(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid1 = conn.post(['document'], {'prop': 'a'})
        self.utime('master/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = conn.post(['document'], {'prop': 'b'})
        self.utime('master/document/%s/%s' % (guid2[:2], guid2), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = model.diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual({'commit': []}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

        patch = model.diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual(guid1, next(patch)['guid'])
        self.assertEqual(guid2, next(patch)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, patch.throw(StopIteration()))
        try:
            next(patch)
            assert False
        except StopIteration:
            pass

    def test_diff_Stretch(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid1 = conn.post(['document'], {'prop': 'a'})
        self.utime('master/document/%s/%s' % (guid1[:2], guid1), 1)
        guid2 = conn.post(['document'], {'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = conn.post(['document'], {'prop': 'c'})
        self.utime('master/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = conn.post(['document'], {'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = conn.post(['document'], {'prop': 'f'})
        self.utime('master/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = model.diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3]]}, patch.throw(StopIteration()))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

        patch = model.diff(volume, in_seq)
        self.assertEqual({'resource': 'document'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 5]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_DoNotStretchContinuesPacket(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid1 = conn.post(['document'], {'prop': 'a'})
        volume['document'].delete(guid1)
        guid2 = conn.post(['document'], {'prop': 'b'})
        volume['document'].delete(guid2)
        guid3 = conn.post(['document'], {'prop': 'c'})
        self.utime('master/document/%s/%s' % (guid3[:2], guid3), 2)
        guid4 = conn.post(['document'], {'prop': 'd'})
        volume['document'].delete(guid4)
        guid5 = conn.post(['document'], {'prop': 'f'})
        self.utime('master/document/%s/%s' % (guid5[:2], guid5), 2)

        in_seq = toolkit.Sequence([[1, None]])
        patch = model.diff(volume, in_seq, toolkit.Sequence([[1, 1]]))
        self.assertEqual({'resource': 'document'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual(guid5, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 1], [3, 3], [5, 5]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_diff_TheSameInSeqForAllDocuments(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        class Document3(db.Resource):
            pass

        volume = self.start_master([User, Document1, Document2, Document3])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid3 = conn.post(['document1'], {})
        self.utime('master/document/%s/%s' % (guid3[:2], guid3), 3)
        guid2 = conn.post(['document2'], {})
        self.utime('master/document/%s/%s' % (guid2[:2], guid2), 2)
        guid1 = conn.post(['document3'], {})
        self.utime('master/document/%s/%s' % (guid1[:2], guid1), 1)

        in_seq = toolkit.Sequence([[1, None]])
        patch = model.diff(volume, in_seq)
        self.assertEqual({'resource': 'document1'}, patch.send(None))
        self.assertEqual(guid3, patch.send(None)['guid'])
        self.assertEqual({'resource': 'document2'}, patch.send(None))
        self.assertEqual(guid2, patch.send(None)['guid'])
        self.assertEqual({'resource': 'document3'}, patch.send(None))
        self.assertEqual(guid1, patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, patch.send(None))
        try:
            patch.send(None)
            assert False
        except StopIteration:
            pass

    def test_merge_Create(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document1(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        class Document2(db.Resource):
            pass

        self.touch(('master/db.seqno', '100'))
        volume = self.start_master([Document1, Document2])

        records = [
                {'resource': 'document1'},
                {'guid': '1', 'diff': {
                    'guid': {'value': '1', 'mtime': 1.0},
                    'ctime': {'value': 2, 'mtime': 2.0},
                    'mtime': {'value': 3, 'mtime': 3.0},
                    'prop': {'value': '4', 'mtime': 4.0},
                    }},
                {'resource': 'document2'},
                {'guid': '5', 'diff': {
                    'guid': {'value': '5', 'mtime': 5.0},
                    'ctime': {'value': 6, 'mtime': 6.0},
                    'mtime': {'value': 7, 'mtime': 7.0},
                    }},
                {'commit': [[1, 2]]},
                ]
        self.assertEqual(([[1, 2]], [[101, 102]]), model.merge(volume, records))

        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 2, 'mtime': 3},
                volume['document1'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(1, os.stat('master/document1/1/1/guid').st_mtime)
        self.assertEqual(2, os.stat('master/document1/1/1/ctime').st_mtime)
        self.assertEqual(3, os.stat('master/document1/1/1/mtime').st_mtime)
        self.assertEqual(4, os.stat('master/document1/1/1/prop').st_mtime)

        self.assertEqual(
                {'guid': '5', 'ctime': 6, 'mtime': 7},
                volume['document2'].get('5').properties(['guid', 'ctime', 'mtime']))
        self.assertEqual(5, os.stat('master/document2/5/5/guid').st_mtime)
        self.assertEqual(6, os.stat('master/document2/5/5/ctime').st_mtime)
        self.assertEqual(7, os.stat('master/document2/5/5/mtime').st_mtime)

    def test_merge_Update(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('master/db.seqno', '100'))
        volume = db.Volume('master', [Document])
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        for i in os.listdir('master/document/1/1'):
            os.utime('master/document/1/1/%s' % i, (2, 2))

        records = [
                {'resource': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '2', 'mtime': 1.0}}},
                {'commit': [[1, 1]]},
                ]
        self.assertEqual(([[1, 1]], []), model.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('master/document/1/1/prop').st_mtime)

        records = [
                {'resource': 'document'},
                {'guid': '1', 'diff': {'prop': {'value': '3', 'mtime': 2.0}}},
                {'commit': [[2, 2]]},
                ]
        self.assertEqual(([[2, 2]], []), model.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(2, os.stat('master/document/1/1/prop').st_mtime)

        records = [
            {'resource': 'document'},
            {'guid': '1', 'diff': {'prop': {'value': '4', 'mtime': 3.0}}},
            {'commit': [[3, 3]]},
            ]
        self.assertEqual(([[3, 3]], [[102, 102]]), model.merge(volume, records))
        self.assertEqual(
                {'guid': '1', 'prop': '4', 'ctime': 1, 'mtime': 1},
                volume['document'].get('1').properties(['guid', 'ctime', 'mtime', 'prop']))
        self.assertEqual(3, os.stat('master/document/1/1/prop').st_mtime)

    def test_merge_MultipleCommits(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        self.touch(('master/db.seqno', '100'))
        volume = db.Volume('master', [Document])

        def generator():
            for i in [
                    {'resource': 'document'},
                    {'commit': [[1, 1]]},
                    {'guid': '1', 'diff': {
                        'guid': {'value': '1', 'mtime': 1.0},
                        'ctime': {'value': 2, 'mtime': 2.0},
                        'mtime': {'value': 3, 'mtime': 3.0},
                        'prop': {'value': '4', 'mtime': 4.0},
                        }},
                    {'commit': [[2, 3]]},
                    ]:
                yield i

        records = generator()
        self.assertEqual(([[1, 3]], [[101, 101]]), model.merge(volume, records))
        assert volume['document'].exists('1')

    def test_diff_ByLayers(self):
        self.override(time, 'time', lambda: 0)
        self.override(NodeRoutes, 'authorize', lambda self, user, role: True)

        class Context(db.Resource):
            pass

        class Post(db.Resource):
            pass

        this.request = Request()
        volume = db.Volume('db', [Context, Post])
        volume['context'].create({'guid': '0', 'ctime': 1, 'mtime': 1, 'layer': ['layer0', 'common']})
        volume['context'].create({'guid': '1', 'ctime': 1, 'mtime': 1, 'layer': ['layer1']})
        volume['post'].create({'guid': '3', 'ctime': 3, 'mtime': 3, 'layer': 'layer3'})

        volume['context'].update('0', {'tags': '0'})
        volume['context'].update('1', {'tags': '1'})
        volume['post'].update('3', {'tags': '3'})
        self.utime('db', 0)

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'resource': 'post'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in model.diff(volume, toolkit.Sequence([[4, None]]))]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'guid': '1', 'diff': {'tags': {'value': '1', 'mtime': 0}}},
            {'resource': 'post'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in model.diff(volume, toolkit.Sequence([[4, None]]), layer='layer1')]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'resource': 'post'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in model.diff(volume, toolkit.Sequence([[4, None]]), layer='layer2')]))

        self.assertEqual(sorted([
            {'resource': 'context'},
            {'guid': '0', 'diff': {'tags': {'value': '0', 'mtime': 0}}},
            {'resource': 'post'},
            {'guid': '3', 'diff': {'tags': {'value': '3', 'mtime': 0}}},
            {'commit': [[4, 6]]},
            ]),
            sorted([i for i in model.diff(volume, toolkit.Sequence([[4, None]]), layer='foo')]))

    def test_Packages(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: {'version': '1.0'})

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api_url.value, http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', '*'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-7.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', 'Gentoo'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            'Gentoo': {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', 'Debian-6.0'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            'Debian-6.0': {
                'seqno': 7,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

    def test_UnresolvedPackages(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, 'resolve failed'))

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api_url.value, http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', '*'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'resolve failed'},
                },
            },
            volume['context'][guid]['releases'])

    def test_PackageOverrides(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api_url.value, http.SugarAuth(keyfile.value))
        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '1'))
        conn.put(['context', guid, 'releases', '*'], {'binary': '1'})
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '1'},
                'Debian-7.0': {'status': '1'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '2'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '2'})
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['2']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '2'},
                'Debian-7.0': {'status': '2'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '3'))
        conn.put(['context', guid, 'releases', 'Debian-6.0'], {'binary': '3'})
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['2']},
                },
            'Debian-6.0': {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['3']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '2'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '4'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '4'})
        self.assertEqual({
            '*': {
                'seqno': 3,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 6,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['4']},
                },
            'Debian-6.0': {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['3']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '4'},
                },
            },
            volume['context'][guid]['releases'])

    def test_solve_SortByVersions(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'commands1'}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0], 'command': 'commands2'}},
                '3': {'value': {'stability': 'stable', 'version': [[3], 0], 'command': 'commands3'}},
                },
            })
        self.assertEqual(
                {context: {'command': 'commands3', 'blob': '3', 'version': [[3], 0]}},
                model.solve(volume, context))

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'stability': 'stable', 'version': [[3], 0], 'command': 'commands3'}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0], 'command': 'commands2'}},
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'commands1'}},
                },
            })
        self.assertEqual(
                {context: {'command': 'commands3', 'blob': '3', 'version': [[3], 0]}},
                model.solve(volume, context))

    def test_solve_SortByStability(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'developer', 'version': [[1], 0], 'command': 'commands1'}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0], 'command': 'commands2'}},
                '3': {'value': {'stability': 'buggy', 'version': [[3], 0], 'command': 'commands3'}},
                },
            })
        self.assertEqual(
                {context: {'command': 'commands2', 'blob': '2', 'version': [[2], 0]}},
                model.solve(volume, context))

    def test_solve_CollectDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'stability': 'stable',
                    'version': [[1], 0],
                    'requires': spec.parse_requires('context2; context4'),
                    'command': 'command',
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'stability': 'stable',
                    'version': [[2], 0],
                    'requires': spec.parse_requires('context3'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'stability': 'stable', 'version': [[3], 0]}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'stability': 'stable', 'version': [[4], 0]}},
                },
            })

        self.assertEqual({
            'context1': {'blob': '1', 'version': [[1], 0], 'command': 'command'},
            'context2': {'blob': '2', 'version': [[2], 0]},
            'context3': {'blob': '3', 'version': [[3], 0]},
            'context4': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context1'))

    def test_solve_DepConditions(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0]}},
                '3': {'value': {'stability': 'stable', 'version': [[3], 0]}},
                '4': {'value': {'stability': 'stable', 'version': [[4], 0]}},
                '5': {'value': {'stability': 'stable', 'version': [[5], 0]}},
                },
            })

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep < 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '2', 'version': [[2], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '3', 'version': [[3], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep > 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '5', 'version': [[5], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep >= 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '5', 'version': [[5], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep > 2; dep < 5'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '4', 'version': [[4], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep > 2; dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '3', 'version': [[3], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep = 1'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
                'dep': {'blob': '1', 'version': [[1], 0]},
                },
                model.solve(volume, 'context1'))

    def test_solve_SwitchToAlternativeBranch(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '6': {'value': {'stability': 'stable', 'version': [[1], 0], 'requires': spec.parse_requires('context4=1'), 'command': 'commands6'}},
                '1': {'value': {'stability': 'stable', 'version': [[2], 0], 'requires': spec.parse_requires('context2'), 'command': 'commands1'}},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'stability': 'stable', 'version': [[1], 0], 'requires': spec.parse_requires('context3; context4=1')}},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'stability': 'stable', 'version': [[1], 0], 'requires': spec.parse_requires('context4=2')}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'stability': 'stable', 'version': [[2], 0]}},
                '5': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                },
            })

        self.assertEqual({
            'context1': {'blob': '6', 'version': [[1], 0], 'command': 'commands6'},
            'context4': {'blob': '5', 'version': [[1], 0]},
            },
            model.solve(volume, 'context1'))

    def test_solve_CommonDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0]}},
                '3': {'value': {'stability': 'stable', 'version': [[3], 0]}},
                '4': {'value': {'stability': 'stable', 'version': [[4], 0]}},
                '5': {'value': {'stability': 'stable', 'version': [[5], 0]}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep=2',
            'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            'dep': {'blob': '2', 'version': [[2], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<5',
            'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            'dep': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<4',
            'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            'dep': {'blob': '3', 'version': [[3], 0]},
            },
            model.solve(volume, 'context'))

    def test_solve_ExtraDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0]}},
                '3': {'value': {'stability': 'stable', 'version': [[3], 0]}},
                '4': {'value': {'stability': 'stable', 'version': [[4], 0]}},
                '5': {'value': {'stability': 'stable', 'version': [[5], 0]}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            'dep': {'blob': '5', 'version': [[5], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': 'command'},
            'dep': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context'))

    def test_solve_Nothing(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                '2': {'value': {'stability': 'stable', 'version': [[2], 0]}},
                '3': {'value': {'stability': 'stable', 'version': [[3], 0]}},
                '4': {'value': {'stability': 'stable', 'version': [[4], 0]}},
                '5': {'value': {'stability': 'stable', 'version': [[5], 0]}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep=0'),
                    }},
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

    def test_solve_Packages(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('dep; package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'stability': 'stable', 'version': [[1], 0]}},
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'dep': {'blob': '2', 'version': [[1], 0]},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

    def test_solve_PackagesByLsbId(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('package1'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package1', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'Ubuntu': {'value': {'binary': ['bin1', 'bin2'], 'devel': ['devel1', 'devel2']}},
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package1': {'packages': ['bin1', 'bin2', 'devel1', 'devel2'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('package2'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package2', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'Ubuntu': {'value': {'binary': ['bin']}},
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package2': {'packages': ['bin'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='fake'))

    def test_solve_PackagesByCommonAlias(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('package1'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package1', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '*': {'value': {'binary': ['pkg1']}},
                'Ubuntu': {'value': {'binary': ['pkg2']}},
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg3']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context))
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake'))
        self.assertEqual({
            'context': {'blob': '1', 'command': 'command', 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake', lsb_release='fake'))

    def test_solve_NoPackages(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'stability': 'stable', 'version': [[1], 0], 'command': 'command',
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, context))


if __name__ == '__main__':
    tests.main()
