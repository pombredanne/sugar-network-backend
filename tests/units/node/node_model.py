#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import time
import json
import base64
import hashlib
import mimetypes
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node import model, obs
from sugar_network.node.model import User, Volume
from sugar_network.node.auth import Principal as _Principal
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request, Router, ACL, File
from sugar_network.toolkit import spec, i18n, http, coroutine, ranges, enforce


class NodeModelTest(tests.Test):

    def test_diff_volume(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume

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
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r, files=['foo'])])
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
            [i for i in model.diff_volume(r)])
        self.assertEqual([], r)

        r = [[6, None]]
        self.assertEqual([
            {'resource': 'document'},
            ],
            [i for i in model.diff_volume(r)])
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
            [i for i in model.diff_volume(r)])
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
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r, files=['foo', 'bar'])])
        self.assertEqual([[10, None]], r)

    def test_diff_volume_SyncUsecase(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property()
            def prop2(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume

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
            [dict(i) for i in model.diff_volume(r)])
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
            [dict(i) for i in model.diff_volume(r, [[3, 3]])])
        self.assertEqual([[4, None]], r)

    def test_diff_volume_Partial(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume
        volume['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)
        volume['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document/2/2', 2)

        r = [[1, None]]
        patch = model.diff_volume(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertRaises(StopIteration, patch.throw, StopIteration)
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[1, None]], r)

        r = [[1, None]]
        patch = model.diff_volume(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual('2', next(patch)['guid'])
        self.assertEqual({'commit': [[1, 1]]}, patch.throw(StopIteration()))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[2, None]], r)

        r = [[1, None]]
        patch = model.diff_volume(r)
        self.assertEqual({'resource': 'document'}, next(patch))
        self.assertEqual('1', next(patch)['guid'])
        self.assertEqual('2', next(patch)['guid'])
        self.assertEqual({'commit': [[1, 2]]}, next(patch))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[3, None]], r)

    def test_diff_volume_IgnoreOneWayResources(self):

        class Document(db.Resource):
            one_way = True

        volume = Volume('.', [Document])
        this.volume = volume
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
            [i for i in model.diff_volume(r)])
        self.assertEqual([[2, None]], r)

        r = [[1, None]]
        self.assertEqual([
            ],
            [i for i in model.diff_volume(r, one_way=True)])
        self.assertEqual([[1, None]], r)

    def test_diff_volume_TheSameInSeqForAllDocuments(self):
        self.override(time, 'time', lambda: 0)

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        class Document3(db.Resource):
            pass

        volume = Volume('.', [Document1, Document2, Document3])
        this.volume = volume
        volume['document1'].create({'guid': '3', 'ctime': 3, 'mtime': 3})
        self.utime('db/document1/3/3', 3)
        volume['document2'].create({'guid': '2', 'ctime': 2, 'mtime': 2})
        self.utime('db/document2/2/2', 2)
        volume['document3'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document3/1/1', 1)

        r = [[1, None]]
        patch = model.diff_volume(r)
        self.assertEqual({'resource': 'document1'}, patch.send(None))
        self.assertEqual('3', patch.send(None)['guid'])
        self.assertEqual({'resource': 'document2'}, patch.send(None))
        self.assertEqual('2', patch.send(None)['guid'])
        self.assertEqual({'resource': 'document3'}, patch.send(None))
        self.assertEqual('1', patch.send(None)['guid'])
        self.assertEqual({'commit': [[1, 3]]}, patch.send(None))
        self.assertRaises(StopIteration, patch.next)
        self.assertEqual([[4, None]], r)

    def test_diff_volumeLocalProps(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.PUBLIC | ACL.LOCAL)
            def prop2(self, value):
                return value

            @db.stored_property()
            def prop3(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume

        volume['document'].create({'guid': '1', 'prop1': '1', 'prop2': '1', 'prop3': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 0)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 0},
                'ctime': {'value': 1, 'mtime': 0},
                'prop1': {'value': '1', 'mtime': 0},
                'prop3': {'value': '1', 'mtime': 0},
                'mtime': {'value': 1, 'mtime': 0},
                }},
            {'commit': [[1, 1]]},
            ],
            [dict(i) for i in model.diff_volume(r, files=['foo'])])
        self.assertEqual([[2, None]], r)

        volume['document'].update('1', {'prop1': '2'})
        self.utime('db/document', 0)

        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop1': {'value': '2', 'mtime': 0},
                }},
            {'commit': [[2, 2]]},
            ],
            [dict(i) for i in model.diff_volume(r, files=['foo'])])
        self.assertEqual([[3, None]], r)

        volume['document'].update('1', {'prop2': '3'})
        self.utime('db/document', 0)

        self.assertEqual([
            {'resource': 'document'},
            ],
            [dict(i) for i in model.diff_volume(r, files=['foo'])])
        self.assertEqual([[3, None]], r)

        volume['document'].update('1', {'prop1': '4', 'prop2': '4', 'prop3': '4'})
        self.utime('db/document', 0)

        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop1': {'value': '4', 'mtime': 0},
                'prop3': {'value': '4', 'mtime': 0},
                }},
            {'commit': [[3, 3]]},
            ],
            [dict(i) for i in model.diff_volume(r, files=['foo'])])
        self.assertEqual([[4, None]], r)

    def test_patch_volume_New(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume1 = Volume('1', [Document])
        this.volume = volume1
        volume1['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('1/db/document/1/1', 1)
        volume1['document'].create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        self.utime('1/db/document/2/2', 2)
        volume1['document'].create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        self.utime('1/db/document/3/3', 3)
        volume1.blobs.post('1')
        self.touch(('1/files/foo/2', '22'))
        self.touch(('1/files/bar/3', '333'))
        patch = [i for i in model.diff_volume([[1, None]], files=['foo'])]

        volume2 = Volume('2', [Document])
        this.volume = volume2
        model.patch_volume(patch)

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
            blob.meta)
        self.assertEqual('1', file(blob.path).read())

        blob = volume2.blobs.get('foo/2')
        self.assertEqual({
            'x-seqno': '1',
            'content-length': '2',
            'content-type': 'application/octet-stream',
            },
            blob.meta)
        self.assertEqual('22', file(blob.path).read())

        assert volume2.blobs.get('bar/3') is None

    def test_patch_volume_Update(self):

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

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        model.patch_volume(patch)

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
        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        model.patch_volume(patch)

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
        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        model.patch_volume(patch)

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

    def test_patch_volume_Ranges(self):

        class Document(db.Resource):

            @db.stored_property(default='')
            def prop(self, value):
                return value

        volume1 = Volume('db1', [Document])
        volume2 = Volume('db2', [Document])

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([], committed)
        self.assertEqual(None, seqno)

        volume1['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 1]], committed)
        self.assertEqual(1, seqno)

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 1]], committed)
        self.assertEqual(None, seqno)

        volume1['document'].update('1', {'prop': '1'})
        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(2, seqno)

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume2
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(None, seqno)

        volume3 = Volume('db3', [Document])

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume3
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(1, seqno)

        this.volume = volume1
        patch = [i for i in model.diff_volume([[1, None]])]
        this.volume = volume3
        seqno, committed = model.patch_volume(patch)
        self.assertEqual([[1, 2]], committed)
        self.assertEqual(None, seqno)

    def test_patch_volume_MultipleCommits(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(('var/seqno', '100'))
        volume = Volume('.', [Document])
        this.volume = volume

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
        self.assertEqual((101, [[1, 3]]), model.patch_volume(patch))
        assert volume['document']['1'].exists

    def test_patch_volume_SeqnoLess(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume1 = Volume('1', [Document])
        this.volume = volume1
        volume1['document'].create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('1/db/document/1/1', 1)
        volume1.blobs.post('1')
        patch = [i for i in model.diff_volume([[1, None]])]

        volume2 = Volume('2', [Document])
        this.volume = volume2
        model.patch_volume(patch, shift_seqno=False)

        self.assertEqual(
                [(1, '1', 1, '1')],
                [(i['ctime'], i['prop'], i['mtime'], i['guid']) for i in volume2['document'].find()[0]])

        doc = volume2['document'].get('1')
        self.assertEqual(0, doc.get('seqno'))
        assert 'seqno' not in doc.meta('guid')
        assert 'seqno' not in doc.meta('ctime')
        assert 'seqno' not in doc.meta('mtime')
        assert 'seqno' not in doc.meta('prop')

        blob = volume2.blobs.get(hashlib.sha1('1').hexdigest())
        self.assertEqual({
            'x-seqno': '0',
            'content-length': '1',
            'content-type': 'application/octet-stream',
            },
            blob.meta)
        self.assertEqual('1', file(blob.path).read())

    def test_diff_volume_IgnoreSeqnolessUpdates(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.PUBLIC | ACL.LOCAL)
            def prop2(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume

        volume['document'].create({'guid': '1', 'prop1': '1', 'prop2': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop1': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r)])
        self.assertEqual([[2, None]], r)

        volume['document'].update('1', {'prop2': '2'})
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop1': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r)])
        self.assertEqual([[2, None]], r)

        volume['document'].update('1', {'prop1': '2'})
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop1': {'value': '2', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 2]]},
            ],
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r)])
        self.assertEqual([[3, None]], r)

        self.assertEqual(False, volume['document'].patch('1', {'prop1': {'mtime': 2, 'value': '3'}}, seqno=False))
        self.assertEqual('3', volume['document']['1']['prop1'])
        self.utime('db/document/1/1', 1)

        r = [[1, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'commit': [[1, 2]]},
            ],
            [i.meta if isinstance(i, File) else i for i in model.diff_volume(r)])
        self.assertEqual([[3, None]], r)

    def test_diff_volume_AggProps(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, db.Property())
            def prop(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume
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
                'prop': {'value': {'1': {'prop': 1, 'ctime': 0}}, 'mtime': 1},
                }},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2, 'ctime': 0}}, 'mtime': 2},
                }},
            {'commit': [[1, 2]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([[3, None]], r)

        r = [[1, 1]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'prop': {'value': {'1': {'prop': 1, 'ctime': 0}}, 'mtime': 1},
                }},
            {'commit': [[1, 1]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([], r)

        r = [[2, 2]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2, 'ctime': 0}}, 'mtime': 2},
                }},
            {'commit': [[2, 2]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([], r)

        r = [[3, None]]
        self.assertEqual([
            {'resource': 'document'},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([[3, None]], r)

        self.assertEqual({
            '1': {'seqno': 1, 'prop': 1, 'ctime': 0},
            },
            volume['document'].get('1')['prop'])
        self.assertEqual({
            '2': {'seqno': 2, 'prop': 2, 'ctime': 0},
            },
            volume['document'].get('2')['prop'])

        volume['document'].update('2', {'prop': {'2': {}, '3': {'prop': 3}}})
        r = [[3, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'prop': {'value': {'2': {'ctime': 0}, '3': {'prop': 3, 'ctime': 0}}, 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[3, 3]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([[4, None]], r)

        self.assertEqual({
            '2': {'seqno': 3, 'ctime': 0},
            '3': {'seqno': 3, 'prop': 3, 'ctime': 0},
            },
            volume['document'].get('2')['prop'])

        volume['document'].update('1', {'prop': {'1': {'foo': 'bar'}}})
        r = [[4, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'value': {'1': {'foo': 'bar', 'ctime': 0}}, 'mtime': int(os.stat('db/document/1/1/prop').st_mtime)},
                }},
            {'commit': [[4, 4]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([[5, None]], r)

        self.assertEqual({
            '1': {'seqno': 4, 'foo': 'bar', 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

        volume['document'].update('2', {'prop': {'2': {'restore': True}}})
        r = [[5, None]]
        self.assertEqual([
            {'resource': 'document'},
            {'guid': '2', 'patch': {
                'prop': {'value': {'2': {'restore': True, 'ctime': 0}}, 'mtime': int(os.stat('db/document/2/2/prop').st_mtime)},
                }},
            {'commit': [[5, 5]]},
            ],
            [i for i in model.diff_volume(r)])
        self.assertEqual([[6, None]], r)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True, 'ctime': 0},
            '3': {'seqno': 3, 'prop': 3, 'ctime': 0},
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
            [i for i in model.diff_volume(r)])
        self.assertEqual([[7, None]], r)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True, 'ctime': 0},
            '3': {'seqno': 3, 'prop': 3, 'ctime': 0},
            },
            volume['document'].get('2')['prop'])

    def test_patch_volume_Aggprops(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, db.Property())
            def prop(self, value):
                return value

        volume = Volume('.', [Document])
        this.volume = volume

        model.patch_volume([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'guid': {'mtime': 1, 'value': '1'},
                'ctime': {'mtime': 1, 'value': 1},
                'mtime': {'mtime': 1, 'value': 1},
                'prop': {'mtime': 1, 'value': {'1': {'ctime': 0}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 1, 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

        model.patch_volume([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 1, 'value': {'1': {'probe': False, 'ctime': 0}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 1, 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

        model.patch_volume([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 2, 'value': {'1': {'probe': True, 'ctime': 0}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True, 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

        model.patch_volume([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 3, 'value': {'2': {'foo': 'bar', 'ctime': 0}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True, 'ctime': 0},
            '2': {'seqno': 3, 'foo': 'bar', 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

        model.patch_volume([
            {'resource': 'document'},
            {'guid': '1', 'patch': {
                'prop': {'mtime': 4, 'value': {'2': {'ctime': 0}, '3': {'foo': 'bar', 'ctime': 0}}},
                }},
            ])
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True, 'ctime': 0},
            '2': {'seqno': 4, 'ctime': 0},
            '3': {'seqno': 4, 'foo': 'bar', 'ctime': 0},
            },
            volume['document'].get('1')['prop'])

    def test_IncrementReleasesSeqnoOnNewReleases(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.release_seqno.value)

        conn.put(['context', context], {
            'summary': 'summary2',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.release_seqno.value)

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
        self.assertEqual(1, volume.release_seqno.value)

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
        self.assertEqual(2, volume.release_seqno.value)

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
        self.assertEqual(3, volume.release_seqno.value)

        conn.delete(['context', context, 'releases', release])
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            {'event': 'release', 'seqno': 2},
            {'event': 'release', 'seqno': 3},
            {'event': 'release', 'seqno': 4},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(4, volume.release_seqno.value)

    def test_IncrementReleasesSeqnoOnDependenciesChange(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.release_seqno.value)

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
            {'seqno': 1, 'event': 'release'}
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.release_seqno.value)
        del events[:]

        conn.put(['context', context], {
            'dependencies': 'dep',
            })
        self.assertEqual([
            {'event': 'release', 'seqno': 2},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(2, volume.release_seqno.value)

    def test_IncrementReleasesSeqnoOnDeletes(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.release_seqno.value)

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
            {'seqno': 1, 'event': 'release'}
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.release_seqno.value)
        del events[:]

        conn.delete(['context', context])
        self.assertEqual([
            {'event': 'release', 'seqno': 2},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(2, volume.release_seqno.value)
        del events[:]

    def test_RestoreReleasesSeqno(self):
        events = []
        volume = self.start_master()
        this.broadcast = lambda x: events.append(x)
        conn = Connection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            'dependencies': 'dep',
            })
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
        self.assertEqual(1, volume.release_seqno.value)

        volume.close()
        volume = Volume('master', [])
        this.volume = volume
        self.assertEqual(1, volume.release_seqno.value)

    def test_Packages(self):
        self.override(time, 'time', lambda: 0)
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: {'version': '1.0'})

        volume = self.start_master([User, model.Context])
        conn = Connection()

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
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                'ctime': 0,
                'seqno': 3,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-7.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                }},
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
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                'ctime': 0,
                'seqno': 5,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                }},
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
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                'ctime': 0,
                'seqno': 7,
                },
            'resolves': {'value': {
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                }},
            },
            volume['context'][guid]['releases'])

    def test_UnresolvedPackages(self):
        self.override(time, 'time', lambda: 0)
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, 'resolve failed'))

        volume = self.start_master([User, model.Context])
        conn = Connection()

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
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                'ctime': 0,
                'seqno': 3,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': 'resolve failed'},
                }},
            },
            volume['context'][guid]['releases'])

    def test_PackageOverrides(self):
        self.override(time, 'time', lambda: 0)
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])

        volume = self.start_master([User, model.Context])
        conn = Connection()
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
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['1']},
                'ctime': 0,
                'seqno': 3,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '1'},
                'Debian-7.0': {'status': '1'},
                }},
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '2'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '2'})
        self.assertEqual({
            '*': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['1']},
                'ctime': 0,
                'seqno': 3,
                },
            'Debian': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['2']},
                'ctime': 0,
                'seqno': 4,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '2'},
                'Debian-7.0': {'status': '2'},
                }},
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '3'))
        conn.put(['context', guid, 'releases', 'Debian-6.0'], {'binary': '3'})
        self.assertEqual({
            '*': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['1']},
                'ctime': 0,
                'seqno': 3,
                },
            'Debian': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['2']},
                'ctime': 0,
                'seqno': 4,
                },
            'Debian-6.0': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['3']},
                'ctime': 0,
                'seqno': 5,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '2'},
                }},
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '4'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '4'})
        self.assertEqual({
            '*': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['1']},
                'ctime': 0,
                'seqno': 3,
                },
            'Debian': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['4']},
                'ctime': 0,
                'seqno': 6,
                },
            'Debian-6.0': {
                'author': {tests.UID: {'name': 'test', 'role': db.Author.INSYSTEM | db.Author.ORIGINAL}},
                'value': {'binary': ['3']},
                'ctime': 0,
                'seqno': 5,
                },
            'resolves': {'value': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '4'},
                }},
            },
            volume['context'][guid]['releases'])

    def test_solve_SortByVersions(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 3), 'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'}},
                model.solve(volume, context))

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 3), 'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'}},
                model.solve(volume, context))

    def test_solve_SortByStability(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'developer', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'buggy', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 2), 'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[2], 0], 'size': 0, 'content-type': 'mime'}},
                model.solve(volume, context))

    def test_solve_CollectDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable',
                    'version': [[1], 0],
                    'requires': spec.parse_requires('context2; context4'),
                    'commands': {'activity': {'exec': 'command'}},
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'context2': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[2], 0], 'size': 0, 'content-type': 'mime'},
            'context3': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'},
            'context4': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': [[4], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context1'))

    def test_solve_CommandDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable',
                    'version': [[1], 0],
                    'requires': [],
                    'commands': {
                        'activity': {'exec': 1, 'requires': spec.parse_requires('context2')},
                        'application': {'exec': 2},
                        },
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': [],
                    }},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 1), 'size': 0, 'content-type': 'mime'},
            'context2': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[2], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context1', command='activity'))
        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('application', 2), 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context1', command='application'))

    def test_solve_DepConditions(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep < 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[2], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': [[5], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep >= 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': [[5], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep < 5'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': [[4], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep = 1'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'size': 0, 'content-type': 'mime'},
                },
                model.solve(volume, 'context1'))

    def test_solve_SwitchToAlternativeBranch(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '6': {'value': {'bundles': {'*-*': {'blob': '6'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=1'), 'commands': {'activity': {'exec': 6}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context2'), 'commands': {'activity': {'exec': 1}}}},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3; context4=1')}},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=2')}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/6', 'version': [[1], 0], 'command': ('activity', 6), 'size': 0, 'content-type': 'mime'},
            'context4': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': [[1], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context1'))

    def test_solve_CommonDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep=2',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[2], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<5',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': [[4], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<4',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': [[3], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

    def test_solve_ExtraDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': [[5], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': [[4], 0], 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

    def test_solve_Nothing(self):
        volume = Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep=0'),
                    }},
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

    def test_solve_Packages(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'resolves': {'value': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep; package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': [[1], 0], 'size': 0, 'content-type': 'mime'},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

    def test_solve_PackagesByLsbId(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
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
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package1': {'packages': ['bin1', 'bin2'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package2'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package2', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'Ubuntu': {'value': {'binary': ['bin']}},
                'resolves': {'value': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package2': {'packages': ['bin'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='fake'))

    def test_solve_PackagesByCommonAlias(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package1'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package1', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '*': {'value': {'binary': ['pkg1']}},
                'Ubuntu': {'value': {'binary': ['pkg2']}},
                'resolves': {'value': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg3']},
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake'))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 'command'), 'size': 0, 'content-type': 'mime'},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake', lsb_release='fake'))

    def test_solve_NoPackages(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, context))

    def test_solve_IgnoreAbsentContexts(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('absent'), 'commands': {'activity': {'exec': 2}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'commands': {'activity': {'exec': 1}}}},
                },
            })

        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': [[1], 0], 'command': ('activity', 1), 'size': 0, 'content-type': 'mime'},
            },
            model.solve(volume, 'context'))

    def test_load_bundle_Activity(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])
        changelog = "LOG"
        bundle = self.zips(
                ('topdir/activity/activity.info', activity_info),
                ('topdir/CHANGELOG', changelog),
                )
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob, bundle_id)

        self.assertEqual({
            'content-type': 'application/vnd.olpc-sugar',
            'content-disposition': 'attachment; filename="Activity-1%s"' % (mimetypes.guess_extension('application/vnd.olpc-sugar') or ''),
            'content-length': str(len(bundle)),
            'x-seqno': '3',
            }, blobs.get(blob.digest).meta)
        self.assertEqual(bundle_id, context)
        self.assertEqual([[1], 0], release['version'])
        self.assertEqual('developer', release['stability'])
        self.assertEqual(['Public Domain'], release['license'])
        self.assertEqual('developer', release['stability'])
        self.assertEqual({
            'dep': [],
            'sugar': [([1, 0], [[0, 88], 0])],
            },
            release['requires'])
        self.assertEqual({
            '*-*': {
                'blob': blob.digest,
                'unpack_size': len(activity_info) + len(changelog),
                },
            },
            release['bundles'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity 1 release',
            'es': 'Activity 1 release',
            'fr': 'Activity 1 release',
            }, post['title'])
        self.assertEqual({
            'en-us': 'LOG',
            }, post['message'])

    def test_load_bundle_NonActivity(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'book',
            'title': 'NonActivity',
            'summary': 'summary',
            'description': 'description',
            })
        bundle = 'non-activity'
        blob = blobs.post(bundle)
        blob.meta['content-type'] = 'application/pdf'

        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='2', license='GPL')
        context, release = model.load_bundle(blob, bundle_id)

        self.assertEqual({
            'content-type': 'application/pdf',
            'content-disposition': 'attachment; filename="NonActivity-2.pdf"',
            'content-length': str(len(bundle)),
            'x-seqno': '6',
            }, blobs.get(blob.digest).meta)
        self.assertEqual(bundle_id, context)
        self.assertEqual([[2], 0], release['version'])
        self.assertEqual(['GPL'], release['license'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'NonActivity 2 release',
            'es': 'NonActivity 2 release',
            'fr': 'NonActivity 2 release',
            }, post['title'])
        self.assertEqual({
            'en-us': '',
            }, post['message'])

    def test_load_bundle_ReuseActivityLicense(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        activity_info_wo_license = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            ])
        bundle = self.zips(('topdir/activity/activity.info', activity_info_wo_license))
        blob_wo_license = blobs.post(bundle)
        self.assertRaises(http.BadRequest, model.load_bundle, blob_wo_license, bundle_id)

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            'newest': {'value': {'release': 3, 'license': ['Newest']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob_wo_license, bundle_id)
        self.assertEqual(['Newest'], release['license'])

    def test_load_bundle_ReuseNonActivityLicense(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'book',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        blob = blobs.post('non-activity')
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        self.assertRaises(http.BadRequest, model.load_bundle, blob, bundle_id)

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = model.load_bundle(blob, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = model.load_bundle(blob, bundle_id)
        self.assertEqual(['New'], release['license'])

        volume['context'].update(bundle_id, {'releases': {
            'new': {'value': {'release': 2, 'license': ['New']}},
            'old': {'value': {'release': 1, 'license': ['Old']}},
            'newest': {'value': {'release': 3, 'license': ['Newest']}},
            }})
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='1')
        context, release = model.load_bundle(blob, bundle_id)
        self.assertEqual(['Newest'], release['license'])

    def test_load_bundle_WrontContextType(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'group',
            'title': 'NonActivity',
            'summary': 'summary',
            'description': 'description',
            })

        blob = blobs.post('non-activity')
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id], version='2', license='GPL')
        self.assertRaises(http.BadRequest, model.load_bundle, blob, bundle_id)

        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])
        changelog = "LOG"
        bundle = self.zips(
                ('topdir/activity/activity.info', activity_info),
                ('topdir/CHANGELOG', changelog),
                )
        blob = blobs.post(bundle)
        self.assertRaises(http.BadRequest, model.load_bundle, blob, bundle_id)

    def test_load_bundle_MissedContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        conn = Connection()

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])))
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request()
        self.assertRaises(http.NotFound, model.load_bundle, blob, initial=False)

    def test_load_bundle_CreateContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        conn = Connection()

        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = org.laptop.ImageViewerActivity',
                    'name      = Image Viewer',
                    'summary   = The Image Viewer activity is a simple and fast image viewer tool',
                    'description = It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
                    'homepage  = http://wiki.sugarlabs.org/go/Activities/Image_Viewer',
                    'activity_version = 1',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'mime_types = image/bmp;image/gif',
                    ])),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', ''),
                )
        blob = blobs.post(bundle)

        this.principal = Principal(tests.UID)
        this.request = Request()
        context, release = model.load_bundle(blob, initial=True)
        self.assertEqual('org.laptop.ImageViewerActivity', context)

        context = volume['context'].get('org.laptop.ImageViewerActivity')
        self.assertEqual({'en': 'Image Viewer'}, context['title'])
        self.assertEqual({'en': 'The Image Viewer activity is a simple and fast image viewer tool'}, context['summary'])
        self.assertEqual({'en': 'It has features one would expect of a standard image viewer, like zoom, rotate, etc.'}, context['description'])
        self.assertEqual('http://wiki.sugarlabs.org/go/Activities/Image_Viewer', context['homepage'])
        self.assertEqual(['image/bmp', 'image/gif'], context['mime_types'])
        assert context['ctime'] > 0
        assert context['mtime'] > 0
        self.assertEqual({tests.UID: {'role': db.Author.INSYSTEM | db.Author.ORIGINAL, 'name': 'user'}}, context['author'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Image Viewer 1 release',
            'es': 'Image Viewer 1 release',
            'fr': 'Image Viewer 1 release',
            }, post['title'])

    def test_load_bundle_UpdateContext(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()
        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            '%s = admin' % tests.UID,
            ]))

        conn.post(['context'], {
            'guid': 'org.laptop.ImageViewerActivity',
            'type': 'activity',
            'title': {'en': ''},
            'summary': {'en': ''},
            'description': {'en': ''},
            })
        svg = '\n'.join([
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd" [',
            '  <!ENTITY fill_color "#123456">',
            '  <!ENTITY stroke_color "#123456">',
            ']>',
            '<svg xmlns="http://www.w3.org/2000/svg" width="50" height="50">',
            '    <rect x="3" y="7" width="44" height="36" style="fill:&fill_color;;stroke:&stroke_color;;stroke-width:3"/>',
            '    <polyline points="15,7 25,1 35,7" style="fill:none;;stroke:&stroke_color;;stroke-width:1.25"/>',
            '    <circle cx="14" cy="19" r="4.5" style="fill:&stroke_color;;stroke:&stroke_color;;stroke-width:1.5"/>',
            '    <polyline points="3,36 16,32 26,35" style="fill:none;;stroke:&stroke_color;;stroke-width:2.5"/>',
            '    <polyline points="15,43 37,28 47,34 47,43" style="fill:&stroke_color;;stroke:&stroke_color;;stroke-width:3"/>',
            '    <polyline points="22,41.5 35,30 27,41.5" style="fill:&fill_color;;stroke:none;;stroke-width:0"/>',
            '    <polyline points="26,23 28,25 30,23" style="fill:none;;stroke:&stroke_color;;stroke-width:.9"/>',
            '    <polyline points="31.2,20 33.5,17.7 35.8,20" style="fill:none;;stroke:&stroke_color;;stroke-width:1"/>',
            '    <polyline points="36,13 38.5,15.5 41,13" style="fill:none;;stroke:&stroke_color;;stroke-width:1"/>',
            '</svg>',
            ])
        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = org.laptop.ImageViewerActivity',
                    'name      = Image Viewer',
                    'summary   = The Image Viewer activity is a simple and fast image viewer tool',
                    'description = It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
                    'homepage  = http://wiki.sugarlabs.org/go/Activities/Image_Viewer',
                    'activity_version = 22',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'mime_types = image/bmp;image/gif',
                    ])),
                ('ImageViewer.activity/locale/ru/LC_MESSAGES/org.laptop.ImageViewerActivity.mo',
                    base64.b64decode('3hIElQAAAAAMAAAAHAAAAHwAAAARAAAA3AAAAAAAAAAgAQAADwAAACEBAAAOAAAAMQEAAA0AAABAAQAACgAAAE4BAAAMAAAAWQEAAA0AAABmAQAAJwAAAHQBAAAUAAAAnAEAABAAAACxAQAABwAAAMIBAAAIAAAAygEAANEBAADTAQAAIQAAAKUDAAATAAAAxwMAABwAAADbAwAAFwAAAPgDAAAhAAAAEAQAAB0AAAAyBAAAQAAAAFAEAAA9AAAAkQQAADUAAADPBAAAFAAAAAUFAAAQAAAAGgUAAAEAAAACAAAABwAAAAAAAAADAAAAAAAAAAwAAAAJAAAAAAAAAAoAAAAEAAAAAAAAAAAAAAALAAAABgAAAAgAAAAFAAAAAENob29zZSBkb2N1bWVudABEb3dubG9hZGluZy4uLgBGaXQgdG8gd2luZG93AEZ1bGxzY3JlZW4ASW1hZ2UgVmlld2VyAE9yaWdpbmFsIHNpemUAUmV0cmlldmluZyBzaGFyZWQgaW1hZ2UsIHBsZWFzZSB3YWl0Li4uAFJvdGF0ZSBhbnRpY2xvY2t3aXNlAFJvdGF0ZSBjbG9ja3dpc2UAWm9vbSBpbgBab29tIG91dABQcm9qZWN0LUlkLVZlcnNpb246IFBBQ0tBR0UgVkVSU0lPTgpSZXBvcnQtTXNnaWQtQnVncy1UbzogClBPVC1DcmVhdGlvbi1EYXRlOiAyMDEyLTA5LTI3IDE0OjU3LTA0MDAKUE8tUmV2aXNpb24tRGF0ZTogMjAxMC0wOS0yMiAxMzo1MCswMjAwCkxhc3QtVHJhbnNsYXRvcjoga3JvbTlyYSA8a3JvbTlyYUBnbWFpbC5jb20+Ckxhbmd1YWdlLVRlYW06IExBTkdVQUdFIDxMTEBsaS5vcmc+Ckxhbmd1YWdlOiAKTUlNRS1WZXJzaW9uOiAxLjAKQ29udGVudC1UeXBlOiB0ZXh0L3BsYWluOyBjaGFyc2V0PVVURi04CkNvbnRlbnQtVHJhbnNmZXItRW5jb2Rpbmc6IDhiaXQKUGx1cmFsLUZvcm1zOiBucGx1cmFscz0zOyBwbHVyYWw9KG4lMTA9PTEgJiYgbiUxMDAhPTExID8gMCA6IG4lMTA+PTIgJiYgbiUxMDw9NCAmJiAobiUxMDA8MTAgfHwgbiUxMDA+PTIwKSA/IDEgOiAyKTsKWC1HZW5lcmF0b3I6IFBvb3RsZSAyLjAuMwoA0JLRi9Cx0LXRgNC40YLQtSDQtNC+0LrRg9C80LXQvdGCANCX0LDQs9GA0YPQt9C60LAuLi4A0KPQvNC10YHRgtC40YLRjCDQsiDQvtC60L3QtQDQn9C+0LvQvdGL0Lkg0Y3QutGA0LDQvQDQn9GA0L7RgdC80L7RgtGAINC60LDRgNGC0LjQvdC+0LoA0JjRgdGC0LjQvdC90YvQuSDRgNCw0LfQvNC10YAA0J/QvtC70YPRh9C10L3QuNC1INC40LfQvtCx0YDQsNC20LXQvdC40LksINC/0L7QtNC+0LbQtNC40YLQtS4uLgDQn9C+0LLQtdGA0L3Rg9GC0Ywg0L/RgNC+0YLQuNCyINGH0LDRgdC+0LLQvtC5INGB0YLRgNC10LvQutC4ANCf0L7QstC10YDQvdGD0YLRjCDQv9C+INGH0LDRgdC+0LLQvtC5INGB0YLRgNC10LvQutC1ANCf0YDQuNCx0LvQuNC30LjRgtGMANCe0YLQtNCw0LvQuNGC0YwA')),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', svg),
                )

        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', 'org.laptop.ImageViewerActivity'])
        context, release = model.load_bundle(blob, initial=True)

        context = volume['context'].get('org.laptop.ImageViewerActivity')
        self.assertEqual({
            'en': 'Image Viewer',
            'ru': u'Просмотр картинок',
            },
            context['title'])
        self.assertEqual({
            'en': 'The Image Viewer activity is a simple and fast image viewer tool',
            },
            context['summary'])
        self.assertEqual({
            'en': 'It has features one would expect of a standard image viewer, like zoom, rotate, etc.',
            },
            context['description'])
        self.assertEqual(svg, file(blobs.get(context['artefact_icon']).path).read())
        assert context['icon'] != 'missing.png'
        assert context['logo'] != 'missing-logo.png'
        self.assertEqual('http://wiki.sugarlabs.org/go/Activities/Image_Viewer', context['homepage'])
        self.assertEqual(['image/bmp', 'image/gif'], context['mime_types'])

    def test_load_bundle_3rdPartyRelease(self):
        i18n._default_langs = ['en']
        volume = self.start_master()
        blobs = volume.blobs
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        bundle = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = Activity2',
            'bundle_id = %s' % bundle_id,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            ])))
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID2)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob, bundle_id)

        assert tests.UID in volume['context'][bundle_id]['author']
        assert tests.UID2 not in volume['context'][bundle_id]['author']
        self.assertEqual({'en': 'Activity'}, volume['context'][bundle_id]['title'])

        post = volume['post'][release['announce']]
        assert tests.UID not in post['author']
        assert tests.UID2 in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity 1 third-party release',
            'es': 'Activity 1 third-party release',
            'fr': 'Activity 1 third-party release',
            }, post['title'])

        blobs.delete(blob.digest)
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob, bundle_id)

        assert tests.UID in volume['context'][bundle_id]['author']
        assert tests.UID2 not in volume['context'][bundle_id]['author']
        self.assertEqual({'en': 'Activity2'}, volume['context'][bundle_id]['title'])

        post = volume['post'][release['announce']]
        assert tests.UID in post['author']
        assert tests.UID2 not in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'Activity2 1 release',
            'es': 'Activity2 1 release',
            'fr': 'Activity2 1 release',
            }, post['title'])

    def test_load_bundle_PopulateRequires(self):
        volume = self.start_master()
        blobs = volume.blobs
        conn = Connection()

        bundle_id = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        bundle = self.zips(
                ('ImageViewer.activity/activity/activity.info', '\n'.join([
                    '[Activity]',
                    'bundle_id = %s' % bundle_id,
                    'name      = Image Viewer',
                    'activity_version = 22',
                    'license   = GPLv2+',
                    'icon      = activity-imageviewer',
                    'exec      = true',
                    'requires  = dep1, dep2<10, dep3<=20, dep4>30, dep5>=40, dep6>5<7, dep7>=1<=3',
                    ])),
                ('ImageViewer.activity/activity/activity-imageviewer.svg', ''),
                )
        blob = blobs.post(bundle)
        this.principal = Principal(tests.UID)
        this.request = Request(method='POST', path=['context', bundle_id])
        context, release = model.load_bundle(blob, bundle_id)

        self.assertEqual({
            'dep5': [([1, 0], [[40], 0])],
            'dep4': [([1], [[30], 0])],
            'dep7': [([1, 0], [[1], 0]), ([-1, 0], [[3], 0])],
            'dep6': [([1], [[5], 0]), ([-1], [[7], 0])],
            'dep1': [],
            'dep3': [([-1, 0], [[20], 0])],
            'dep2': [([-1], [[10], 0])],
            },
            release['requires'])

    def test_apply_batch(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': '1'},
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': '2'},
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'prop': '1', 'author': {'test': {'role': db.Author.ORIGINAL}}},
            {'prop': '2', 'author': {'test': {'role': db.Author.ORIGINAL}}},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['prop', 'author'])['result']))
        assert not exists('batch')
        assert not exists('batch.meta')

    def test_apply_batch_MapPK(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        self.override(toolkit, 'uuid', lambda: 'local')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {},
                'keys': ['guid'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        self.assertRaises(http.BadRequest, model.apply_batch, './batch')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote'},
                'keys': ['guid'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'guid': 'local'},
            ]),
            sorted(this.call(method='GET', path=['document'])['result']))

    def test_apply_batch_MapFKAfterCreatingPK(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        self.uuid = 0

        def uuid():
            self.uuid += 1
            return 'local%s' % self.uuid
        self.override(toolkit, 'uuid', uuid)

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote', 'prop': ''},
                'keys': ['guid'],
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': 'remote'},
                'keys': ['prop'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'prop': '', 'guid': 'local1'},
            {'prop': 'local1', 'guid': 'local2'},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))

    def test_apply_batch_MapFKBeforeCreatingPK(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        self.uuid = 0

        def uuid():
            self.uuid += 1
            return 'local%s' % self.uuid
        self.override(toolkit, 'uuid', uuid)

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': 'remote'},
                'keys': ['prop'],
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote', 'prop': ''},
                'keys': ['guid'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'prop': '', 'guid': 'local1'},
            {'prop': 'local1', 'guid': 'local2'},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))

    def test_apply_batch_NoPKInPropsWithoutMap(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        self.override(toolkit, 'uuid', lambda: 'local')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote'},
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        self.assertRaises(http.BadRequest, model.apply_batch, './batch')

    def test_apply_batch_MapPathAfterCreatingPK(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        self.override(toolkit, 'uuid', lambda: 'local')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote', 'prop': '1'},
                'keys': ['guid'],
                }),
            json.dumps({
                'op': {'method': 'PUT', 'path': ['document','remote']},
                'content': {'prop': '2'},
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'prop': '2', 'guid': 'local'},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))

    def test_apply_batch_MapPathBeforeCreatingPK(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        self.override(toolkit, 'uuid', lambda: 'local')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'PUT', 'path': ['document','remote']},
                'content': {'prop': '2'},
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote', 'prop': '1'},
                'keys': ['guid'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        self.assertEqual(sorted([
            {'prop': '1', 'guid': 'local'},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))
        self.assertEqual({
            'guid_map': {'remote': 'local'},
            'failed': [[1, 1]],
            'principal': ['test', 0xF],
            },
            json.load(file('batch.meta')))

    def test_apply_batch_Blobs(self):

        class Document(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = self.start_master([User, Document])
        self.override(toolkit, 'uuid', lambda: 'local')

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote'},
                'keys': ['guid'],
                }),
            json.dumps({
                'op': {'method': 'PUT', 'path': ['document','remote', 'blob']},
                'content': 'file',
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))
        model.apply_batch('./batch')

        digest = hashlib.sha1('file').hexdigest()
        self.assertEqual(digest, volume['document']['local']['blob'])
        self.assertEqual('file', file(volume.blobs.get(digest).path).read())

    def test_apply_batch_Fails(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self_, value):
                return value

            @prop.setter
            def prop(self_, value):
                if value >= self.prop_to_fail:
                    raise RuntimeError()
                return value

        volume = self.start_master([User, Document])

        self.touch(('batch', [
            json.dumps({
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote1', 'prop': 1},
                'keys': ['guid'],
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote2', 'prop': 2},
                'keys': ['guid'],
                }),
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'guid': 'remote3', 'prop': 3},
                'keys': ['guid'],
                }),
            ]))
        self.touch(('batch.meta', [
            json.dumps({
                'principal': ['test', 0xF],
                }),
            ]))

        def uuid():
            self.uuid += 1
            return 'local%s' % self.uuid
        self.uuid = 0
        self.override(toolkit, 'uuid', uuid)

        self.prop_to_fail = 1
        model.apply_batch('./batch')
        self.assertEqual(
                sorted([]),
                sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))
        self.assertEqual({
            'guid_map': {'remote1': 'local1', 'remote2': 'local2', 'remote3': 'local3'},
            'failed': [[1, 3]],
            'principal': ['test', 0xF],
            },
            json.load(file('batch.meta')))
        assert exists('batch')

        self.prop_to_fail = 2
        model.apply_batch('./batch')
        self.assertEqual(
                sorted([{'guid': 'local1', 'prop': 1}]),
                sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))
        self.assertEqual({
            'guid_map': {'remote1': 'local1', 'remote2': 'local2', 'remote3': 'local3'},
            'failed': [[2, 3]],
            'principal': ['test', 0xF],
            },
            json.load(file('batch.meta')))
        assert exists('batch')

        self.prop_to_fail = 3
        model.apply_batch('./batch')
        self.assertEqual(
                sorted([{'guid': 'local1', 'prop': 1}, {'guid': 'local2', 'prop': 2}]),
                sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))
        self.assertEqual({
            'guid_map': {'remote1': 'local1', 'remote2': 'local2', 'remote3': 'local3'},
            'failed': [[3, 3]],
            'principal': ['test', 0xF],
            },
            json.load(file('batch.meta')))
        assert exists('batch')

        self.prop_to_fail = 4
        model.apply_batch('./batch')
        self.assertEqual(
                sorted([{'guid': 'local1', 'prop': 1}, {'guid': 'local2', 'prop': 2}, {'guid': 'local3', 'prop': 3}]),
                sorted(this.call(method='GET', path=['document'], reply=['guid', 'prop'])['result']))
        assert not exists('batch.meta')
        assert not exists('batch')


class Principal(_Principal):

    admin = True


if __name__ == '__main__':
    tests.main()
