#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import json
import sys
import stat
import time
import urllib2
import hashlib
from base64 import b64encode
from cStringIO import StringIO
from os.path import join, exists

import gobject

from __init__ import tests

from sugar_network import db
from sugar_network.db import storage, index
from sugar_network.db import directory as directory_
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, ranges


class VolumeTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        this.broadcast = lambda x: x

    def test_diff(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])

        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)
        volume['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document/2/2', 2)
        volume['document'].create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        self.utime('db/document/3/3', 3)
        volume.blobs.post('1')
        self.touch(('files/foo/2', '22'))
        self.touch(('files/bar/3', '333'))

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                }},
            {'guid': '3', 'patch': {
                'guid': {'value': '3', 'mtime': 3},
                'ctime': {'value': 3, 'mtime': 3},
                'prop': {'value': '3', 'mtime': 3},
                'mtime': {'value': 3, 'mtime': 3},
                }},
            {'content-type': 'application/octet-stream', 'content-length': '1'},
            {'content-type': 'application/octet-stream', 'content-length': '2', 'path': 'foo/2'},
            {'commit': [[1, 5]]},
            ],
            [dict(i) for i in volume.diff(r, files=['foo'])])
        self.assertEqual([[6, None]], r)

        r = [[2, 2]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                }},
            {'commit': [[2, 2]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([], r)

        r = [[6, None]]
        self.assertEqual([
            {'resource': 'document'},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[6, None]], r)

        volume['document'].update('2', {'prop': '22'})

        r = [[6, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'prop': {'value': '22', 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[6, 6]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[7, None]], r)

        volume.blobs.post('4444')
        self.touch(('files/foo/2', '2222'))

        r = [[7, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'content-type': 'application/octet-stream', 'content-length': '4'},
            {'content-type': 'application/octet-stream', 'content-length': '4', 'path': 'foo/2'},
            {'content-type': 'application/octet-stream', 'content-length': '3', 'path': 'bar/3'},
            {'commit': [[7, 9]]},
            ],
            [dict(i) for i in volume.diff(r, files=['foo', 'bar'])])
        self.assertEqual([[10, None]], r)

    def test_diff_SyncUsecase(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property()
            def prop2(self, value):
                return value

        volume = db.Volume('.', [Document])

        volume['document'].create({'guid': 'guid', 'ctime': 1, 'mtime': 1, 'prop1': 1, 'prop2': 1})
        self.utime('db/document/gu/guid', 1)

        # Fresh update to pull
        volume['document'].update('guid', {'prop1': 2})
        self.utime('db/document/gu/guid/prop1', 2)

        # Recently pushed
        volume['document'].update('guid', {'prop2': 2})
        self.utime('db/document/gu/guid/prop2', 2)

        # Exclude `prop2` ack from the pull reanges
        r = [[2, None]]
        ranges.exclude(r, 3, 3)
        self.assertEqual([
            {'resource': 'document'},
            ],
            [dict(i) for i in volume.diff(r)])
        self.assertEqual([[2, 2], [4, None]], r)

        # Pass `prop2` ack in `exclude`
        r = [[2, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': 'guid', 'patch': {
                'prop1': {'value': 2, 'mtime': 2},
                }},
            {'commit': [[2, 2]]},
            ],
            [dict(i) for i in volume.diff(r, [[3, 3]])])
        self.assertEqual([[4, None]], r)

    def test_diff_Partial(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)
        volume['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document/2/2', 2)

        r = [[1, None]]
        patch = volume.diff(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertRaises(StopIteration, patch.throw, StopIteration)
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[1, None]], r)

        r = [[1, None]]
        patch = volume.diff(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual('2', next(patch)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, patch.throw(StopIteration()))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[2, None]], r)

        r = [[1, None]]
        patch = volume.diff(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual('2', next(patch)['guid'])
        self.assertEqual({'commit': [[1, 2]]}, next(patch))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[3, None]], r)

    def test_diff_IgnoreCalcProps(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.CALC)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[2, None]], r)

        volume['document'].update('1', {'prop': '2'})
        self.assertEqual([
            {'resource': 'document'},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[2, None]], r)

        volume['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document/2/2', 2)

        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                }},
            {'commit': [[2, 3]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[4, None]], r)

    def test_diff_IgnoreOneWayResources(self):

        class Document(db.Resource):
            one_way = True

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[2, None]], r)

        r = [[1, None]]
        self.assertEqual([
            ],
            [i for i in volume.diff(r, one_way=True)])
        self.assertEqual([[1, None]], r)

    def test_diff_TheSameInSeqForAllDocuments(self):
        self.override(time, 'time', lambda: 0)

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        class Document3(db.Resource):
            pass

        volume = db.Volume('.', [Document1, Document2, Document3])
        volume['document1'].create({'guid': '3', 'ctime': 3, 'mtime': 3})
        self.utime('db/document1/3/3', 3)
        volume['document2'].create({'guid': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document2/2/2', 2)
        volume['document3'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document3/1/1', 1)

        r = [[1, None]]
        patch = volume.diff(r)
        self.assertEqual({'resource': 'document1'}, patch.send(None))
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual({'resource': 'document2'}, patch.send(None))
        self.assertEqual('2', patch.send(None)['guid'])
        self.assertEqual({'resource': 'document3'}, patch.send(None))
        self.assertEqual('1', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, patch.send(None))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[4, None]], r)

    def test_patch_New(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume1 = db.Volume('1', [Document])
        volume1['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('1/db/document/1/1', 1)
        volume1['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('1/db/document/2/2', 2)
        volume1['document'].create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        self.utime('1/db/document/3/3', 3)
        volume1.blobs.post('1')
        self.touch(('1/files/foo/2', '22'))
        self.touch(('1/files/bar/3', '333'))

        volume2 = db.Volume('2', [Document])
        volume2.patch(volume1.diff([[1, None]], files=['foo']))

        self.assertEqual(
                sorted([
                    (1, '1', 1, '1'),
                    (2, '2', 2, '2'),
                    (3, '3', 3, '3'),
                    ]),
                sorted([(i['ctime'], i['prop'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]]))

        doc = volume2['document'].get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        self.assertEqual(1, doc.meta('ctime')['mtime'])
        self.assertEqual(1, doc.meta('prop')['mtime'])
        self.assertEqual(1, doc.meta('mtime')['mtime'])

        doc = volume2['document'].get('2')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])

        doc = volume2['document'].get('3')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(3, doc.meta('guid')['mtime'])
        self.assertEqual(3, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('prop')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])

        blob = volume2.blobs.get(hashlib.sha1('1').hexdigest())
        self.assertEqual({
            'x-seqno': '1',
            'content-length': '1',
            'content-type': 'application/octet-stream',
            },
            blob)
        self.assertEqual('1', file(blob.path).read())

        blob = volume2.blobs.get('foo/2')
        self.assertEqual({
            'x-seqno': '1',
            'content-length': '2',
            'content-type': 'application/octet-stream',
            },
            blob)
        self.assertEqual('22', file(blob.path).read())

        assert volume2.blobs.get('bar/3') is None

    def test_patch_Update(self):

        class Document(db.Resource):

            @db.stored_property(default='')
            def prop(self, value):
                return value

        volume1 = db.Volume('1', [Document])
        volume1['document'].create({'guid': 'guid', 'ctime': 1, 'mtime': 1})
        volume1['document'].update('guid', {'prop': '1'})
        self.utime('1/db/document/gu/guid', 1)

        volume2 = db.Volume('2', [Document])
        volume2['document'].create({'guid': 'guid', 'ctime': 2, 'mtime': 2})
        volume2['document'].update('guid', {'prop': '2'})
        self.utime('2/db/document/gu/guid', 2)

        self.assertEqual(
                [(2, 2, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]])
        doc = volume2['document'].get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        volume2.patch(volume1.diff([[1, None]]))

        self.assertEqual(
                [(2, 2, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]])
        doc = volume2['document'].get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        os.utime('1/db/document/gu/guid/mtime', (3, 3))
        volume2.patch(volume1.diff([[1, None]]))

        self.assertEqual(
                [(2, 1, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]])
        doc = volume2['document'].get('guid')
        self.assertEqual(3, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        os.utime('1/db/document/gu/guid/prop', (4, 4))
        volume2.patch(volume1.diff([[1, None]]))

        self.assertEqual(
                [(2, 1, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]])
        doc = volume2['document'].get('guid')
        self.assertEqual(4, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(4, doc.meta('prop')['mtime'])
        self.assertEqual('1', doc.meta('prop')['value'])

    def test_diff_AggProps(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1, 'prop': {'1': {'prop': 1}}})
        self.utime('db/document/1/1', 1)
        volume['document'].create({'guid': '2', 'ctime': 2, 'mtime': 2, 'prop': {'2': {'prop': 2}}})
        self.utime('db/document/2/2', 2)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'prop': {'value': {'1': {'prop': 1}}, 'mtime': 1},
                }},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2}}, 'mtime': 2},
                }},
            {'commit': [[1, 2]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[3, None]], r)

        r = [[1, 1]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'prop': {'value': {'1': {'prop': 1}}, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([], r)

        r = [[2, 2]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2}}, 'mtime': 2},
                }},
            {'commit': [[2, 2]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([], r)

        r = [[3, None]]
        self.assertEqual([
            {'resource': 'document'},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[3, None]], r)

        self.assertEqual({
            '1': {'seqno': 1, 'prop': 1},
            },
            volume['document'].get('1')['prop'])
        self.assertEqual({
            '2': {'seqno': 2, 'prop': 2},
            },
            volume['document'].get('2')['prop'])

        volume['document'].update('2', {'prop': {'2': {}, '3': {'prop': 3}}})
        r = [[3, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'prop': {'value': {'2': {}, '3': {'prop': 3}}, 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[3, 3]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[4, None]], r)

        self.assertEqual({
            '2': {'seqno': 3},
            '3': {'seqno': 3, 'prop': 3},
            },
            volume['document'].get('2')['prop'])

        volume['document'].update('1', {'prop': {'1': {'foo': 'bar'}}})
        r = [[4, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'value': {'1': {'foo': 'bar'}}, 'mtime': int(os.stat('db/document/1/1/prop').st_mtime)},
                }},
            {'commit': [[4, 4]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[5, None]], r)

        self.assertEqual({
            '1': {'seqno': 4, 'foo': 'bar'},
            },
            volume['document'].get('1')['prop'])

        volume['document'].update('2', {'prop': {'2': {'restore': True}}})
        r = [[5, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'prop': {'value': {'2': {'restore': True}}, 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[5, 5]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[6, None]], r)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True},
            '3': {'seqno': 3, 'prop': 3},
            },
            volume['document'].get('2')['prop'])

        volume['document'].update('2', {'ctime': 0})
        r = [[6, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'ctime': {'value': 0, 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[6, 6]]},
            ],
            [i for i in volume.diff(r)])
        self.assertEqual([[7, None]], r)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True},
            '3': {'seqno': 3, 'prop': 3},
            },
            volume['document'].get('2')['prop'])

    def test_patch_Aggprops(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])

        volume.patch([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'mtime': 1, 'value': '1'},
                'ctime': {'mtime': 1, 'value': 1},
                'mtime': {'mtime': 1, 'value': 1},
                'prop': {'mtime': 1, 'value': {'1': {}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 1},
            },
            volume['document'].get('1')['prop'])

        volume.patch([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 1, 'value': {'1': {'probe': False}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 1},
            },
            volume['document'].get('1')['prop'])

        volume.patch([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 2, 'value': {'1': {'probe': True}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            },
            volume['document'].get('1')['prop'])

        volume.patch([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 3, 'value': {'2': {'foo': 'bar'}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            '2': {'seqno': 3, 'foo': 'bar'},
            },
            volume['document'].get('1')['prop'])

        volume.patch([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 4, 'value': {'2': {}, '3': {'foo': 'bar'}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            '2': {'seqno': 4},
            '3': {'seqno': 4, 'foo': 'bar'},
            },
            volume['document'].get('1')['prop'])

    def test_patch_Ranges(self):

        class Document(db.Resource):

            @db.stored_property(default='')
            def prop(self, value):
                return value

        volume1 = db.Volume('db1', [Document])
        volume2 = db.Volume('db2', [Document])

        seqno, committed = volume2.patch(volume1.diff([[1, None]]))
        self.assertEqual([], committed)
        self.assertEqual(None, seqno)

        volume1['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        seqno, committed = volume2.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 1]], committed)
        self.assertEqual(1, seqno)
        seqno, committed = volume2.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 1]], committed)
        self.assertEqual(None, seqno)

        volume1['document'].update('1', {'prop': '1'})
        seqno, committed = volume2.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(2, seqno)
        seqno, committed = volume2.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(None, seqno)

        volume3 = db.Volume('db3', [Document])
        seqno, committed = volume3.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(1, seqno)
        seqno, committed = volume3.patch(volume1.diff([[1, None]]))
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(None, seqno)

    def test_patch_CallSetters(self):

        class Document(db.Resource):

            @db.stored_property(db.Numeric)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + 1

        directory = Directory('document', Document, IndexWriter, _SessionSeqno())

        directory.patch('1', {
            'guid': {'mtime': 1, 'value': '1'},
            'ctime': {'mtime': 1, 'value': 1},
            'mtime': {'mtime': 1, 'value': 1},
            'prop': {'mtime': 1, 'value': 1},
            })
        self.assertEqual(2, directory.get('1')['prop'])

    def test_patch_MultipleCommits(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('var/db.seqno', '100'))
        volume = db.Volume('.', [Document])

        def generator():
            for i in [
                    {'resource': 'document'},
                    {'commit': [[1, 1]]},
                    {'guid': '1', 'patch': {
                        'guid': {'value': '1', 'mtime': 1.0},
                        'ctime': {'value': 2, 'mtime': 2.0},
                        'mtime': {'value': 3, 'mtime': 3.0},
                        'prop': {'value': '4', 'mtime': 4.0},
                        }},
                    {'commit': [[2, 3]]},
                    ]:
                yield i

        patch = generator()
        self.assertEqual((101, [[1, 3]]), volume.patch(patch))
        assert volume['document'].exists('1')


class _SessionSeqno(object):

    def __init__(self):
        self._value = 0

    @property
    def value(self):
        return self._value

    def next(self):
        self._value += 1
        return self._value

    def commit(self):
        pass


if __name__ == '__main__':
    tests.main()
