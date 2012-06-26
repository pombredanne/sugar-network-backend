#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import sys
import stat
import json
import time
import hashlib
from cStringIO import StringIO
from os.path import join, exists

import gobject

from __init__ import tests

from active_document import document, storage, env, index
from active_document import directory as directory_
from active_document.directory import Directory
from active_document.metadata import active_property, Metadata
from active_document.metadata import StoredProperty, BlobProperty
from active_document.index import IndexWriter


class DocumentTest(tests.Test):

    def test_ActiveProperty_Slotted(self):

        class Document(document.Document):

            @active_property(slot=1)
            def slotted(self, value):
                return value

            @active_property(StoredProperty)
            def not_slotted(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual(1, directory.metadata['slotted'].slot)

        directory.create({'slotted': 'slotted', 'not_slotted': 'not_slotted', 'user': []})

        docs, total = directory.find(0, 100, order_by='slotted')
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('slotted', 'not_slotted')],
                [(i.slotted, i.not_slotted) for i in docs])

        self.assertRaises(RuntimeError, directory.find, 0, 100, order_by='not_slotted')

    def test_ActiveProperty_SlottedIUnique(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop_1(self, value):
                return value

            @active_property(slot=1)
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter)

    def test_ActiveProperty_Terms(self):

        class Document(document.Document):

            @active_property(prefix='T')
            def term(self, value):
                return value

            @active_property(StoredProperty)
            def not_term(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual('T', directory.metadata['term'].prefix)

        guid = directory.create({'term': 'term', 'not_term': 'not_term', 'user': []})

        docs, total = directory.find(0, 100, term='term')
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertRaises(RuntimeError, directory.find, 0, 100, not_term='not_term')
        self.assertEqual(0, directory.find(0, 100, query='not_term:not_term')[-1])
        self.assertEqual(1, directory.find(0, 100, query='not_term:=not_term')[-1])

    def test_ActiveProperty_TermsUnique(self):

        class Document(document.Document):

            @active_property(prefix='P')
            def prop_1(self, value):
                return value

            @active_property(prefix='P')
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter)

    def test_ActiveProperty_FullTextSearch(self):

        class Document(document.Document):

            @active_property(full_text=False, slot=1)
            def no(self, value):
                return value

            @active_property(full_text=True, slot=2)
            def yes(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual(False, directory.metadata['no'].full_text)
        self.assertEqual(True, directory.metadata['yes'].full_text)

        guid = directory.create({'no': 'foo', 'yes': 'bar', 'user': []})

        self.assertEqual(0, directory.find(0, 100, query='foo')[-1])
        self.assertEqual(1, directory.find(0, 100, query='bar')[-1])

    def test_StoredProperty_Defaults(self):

        class Document(document.Document):

            @active_property(StoredProperty, default='default')
            def w_default(self, value):
                return value

            @active_property(StoredProperty)
            def wo_default(self, value):
                return value

            @active_property(slot=1, default='not_stored_default')
            def not_stored_default(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual('default', directory.metadata['w_default'].default)
        self.assertEqual(None, directory.metadata['wo_default'].default)
        self.assertEqual('not_stored_default', directory.metadata['not_stored_default'].default)

        guid = directory.create({'wo_default': 'wo_default', 'user': []})

        docs, total = directory.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('default', 'wo_default', 'not_stored_default')],
                [(i.w_default, i.wo_default, i.not_stored_default) for i in docs])

        self.assertRaises(RuntimeError, directory.create, {'user': []})

    def test_properties_Blob(self):

        class Document(document.Document):

            @active_property(BlobProperty, mime_type='mime_type')
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertRaises(RuntimeError, directory.create, {'blob': 'probe', 'user': []})

        guid = directory.create({'user': []})

        self.assertRaises(RuntimeError, directory.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: directory.get(guid).blob)
        self.assertRaises(RuntimeError, directory.update, guid, {'blob': 'foo'})

        data = 'payload'

        directory.set_blob(guid, 'blob', StringIO(data))
        self.assertEqual({
            'seqno': 2,
            'mtime': os.stat(join(tests.tmpdir, guid[:2], guid, 'blob')).st_mtime,
            'digest': hashlib.sha1(data).hexdigest(),
            'path': join(tests.tmpdir, guid[:2], guid, 'blob.blob'),
            'mime_type': 'mime_type',
            },
            directory.get(guid).meta('blob'))

    def test_properties_Override(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, meta):
                meta['path'] = 'new-blob'
                return meta

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        guid = directory.create({'user': []})
        doc = directory.get(guid)

        self.touch(('new-blob', 'new-blob'))
        directory.set_blob(guid, 'blob', StringIO('old-blob'))
        self.assertEqual('new-blob', file(doc.meta('blob')['path']).read())

    def test_find_MaxLimit(self):

        class Document(document.Document):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'user': []})
        directory.create({'user': []})
        directory.create({'user': []})

        env.find_limit.value = 1
        docs, total = directory.find(0, 1024)
        self.assertEqual(1, len([i for i in docs]))

    def test_update(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop_1(self, value):
                return value

            @active_property(StoredProperty)
            def prop_2(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop_1': '1', 'prop_2': '2', 'user': []})
        self.assertEqual(
                [('1', '2')],
                [(i.prop_1, i.prop_2) for i in directory.find(0, 1024)[0]])

        directory.update(guid, {'prop_1': '3', 'prop_2': '4'})
        self.assertEqual(
                [('3', '4')],
                [(i.prop_1, i.prop_2) for i in directory.find(0, 1024)[0]])

    def test_delete(self):

        class Document(document.Document):

            @active_property(prefix='P')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid_1 = directory.create({'prop': '1', 'user': []})
        guid_2 = directory.create({'prop': '2', 'user': []})
        guid_3 = directory.create({'prop': '3', 'user': []})

        self.assertEqual(
                ['1', '2', '3'],
                [i.prop for i in directory.find(0, 1024)[0]])

        directory.delete(guid_2)
        self.assertEqual(
                ['1', '3'],
                [i.prop for i in directory.find(0, 1024)[0]])

        directory.delete(guid_3)
        self.assertEqual(
                ['1'],
                [i.prop for i in directory.find(0, 1024)[0]])

        directory.delete(guid_1)
        self.assertEqual(
                [],
                [i.prop for i in directory.find(0, 1024)[0]])

    def test_crawler(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        self.touch(
                ('1/1/guid', '{"value": "1"}'),
                ('1/1/ctime', '{"value": 1}'),
                ('1/1/mtime', '{"value": 1}'),
                ('1/1/prop', '{"value": "prop-1"}'),
                ('1/1/layer', '{"value": ["public"]}'),
                ('1/1/user', '{"value": ["me"]}'),
                ('1/1/seqno', '{"value": 0}'),

                ('2/2/guid', '{"value": "2"}'),
                ('2/2/ctime', '{"value": 2}'),
                ('2/2/mtime', '{"value": 2}'),
                ('2/2/prop', '{"value": "prop-2"}'),
                ('2/2/layer', '{"value": ["public"]}'),
                ('2/2/user', '{"value": ["me"]}'),
                ('2/2/seqno', '{"value": 0}'),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        for i in directory.populate():
            pass

        doc = directory.get('1')
        self.assertEqual(1, doc['ctime'])
        self.assertEqual(1, doc['mtime'])
        self.assertEqual('prop-1', doc['prop'])

        doc = directory.get('2')
        self.assertEqual(2, doc['ctime'])
        self.assertEqual(2, doc['mtime'])
        self.assertEqual('prop-2', doc['prop'])

        self.assertEqual(
                [
                    (1, 1, 'prop-1'),
                    (2, 2, 'prop-2'),
                    ],
                [(i.ctime, i.mtime, i.prop) for i in directory.find(0, 10)[0]])

    def test_create_with_guid(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create_with_guid('guid', {'prop': 'foo', 'user': []})
        self.assertEqual(
                [('guid', 'foo', [], ['public'])],
                [(i.guid, i.prop, i.user, i.layer) for i in directory.find(0, 1024)[0]])

        directory.update(guid, {'prop': 'probe'})
        self.assertEqual(
                [('guid', 'probe')],
                [(i.guid, i.prop) for i in directory.find(0, 1024)[0]])

    def test_on_create(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @classmethod
            def before_create(cls, props):
                super(Document, cls).before_create(props)
                props['prop'] = 'foo'

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'user': []})
        self.assertEqual(
                ['foo'],
                [i.prop for i in directory.find(0, 1024)[0]])

        directory.create({'prop': 'bar', 'user': []})
        self.assertEqual(
                ['foo', 'foo'],
                [i.prop for i in directory.find(0, 1024)[0]])

    def test_on_create_ReplaceGuid(self):

        class Document(document.Document):

            @classmethod
            def before_create(cls, props):
                props['guid'] = props.pop('uid')
                super(Document, cls).before_create(props)

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'uid': 'guid', 'user': []})
        self.assertEqual(
                ['guid'],
                [i.guid for i in directory.find(0, 1024)[0]])

    def test_on_update(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @classmethod
            def before_update(cls, props):
                super(Document, cls).before_update(props)
                props['prop'] = 'foo'

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop': 'bar', 'user': []})
        self.assertEqual(
                ['bar'],
                [i.prop for i in directory.find(0, 1024)[0]])

        directory.update(guid, {'prop': 'probe'})
        self.assertEqual(
                ['foo'],
                [i.prop for i in directory.find(0, 1024)[0]])

    def test_times(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop': '1', 'user': []})
        doc = directory.get(guid)
        self.assertNotEqual(0, doc['ctime'])
        self.assertNotEqual(0, doc['mtime'])
        assert doc['ctime'] == doc['mtime']

        time.sleep(1)

        directory.update(guid, {})
        doc = directory.get(guid)
        assert doc['ctime'] == doc['mtime']

        directory.update(guid, {'prop': '2'})
        doc = directory.get(guid)
        assert doc['ctime'] < doc['mtime']

    def test_seqno(self):

        class Document(document.Document):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid_1 = directory.create({'user': []})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(1, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

        guid_2 = directory.create({'user': []})
        seqno = directory.get(guid_2).get('seqno')
        self.assertEqual(2, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_2[:2], guid_2)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_2[:2], guid_2)))['seqno'],
                seqno)

        directory.set_blob(guid_1, 'blob', StringIO('blob'))
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(3, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                1)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                1)
        self.assertEqual(
                json.load(file('%s/%s/blob' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

        directory.update(guid_1, {'prop': 'new'})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(4, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                1)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('%s/%s/blob' % (guid_1[:2], guid_1)))['seqno'],
                3)

    def test_Events(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 0

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.touch(
                ('1/1/guid', '{"value": "1"}'),
                ('1/1/ctime', '{"value": 1}'),
                ('1/1/mtime', '{"value": 1}'),
                ('1/1/prop', '{"value": "prop-1"}'),
                ('1/1/layer', '{"value": ["public"]}'),
                ('1/1/user', '{"value": ["me"]}'),
                ('1/1/seqno', '{"value": 0}'),
                )

        def notification_cb(event):
            if 'props' in event:
                del event['props']
            events.append(event)

        events = []
        directory = Directory(tests.tmpdir, Document, IndexWriter,
                notification_cb=notification_cb)

        for i in directory.populate():
            pass

        directory.create_with_guid('guid', {'prop': 'prop', 'user': []})
        directory.set_blob('guid', 'blob', StringIO('blob'))
        directory.update('guid', {'prop': 'prop2'})
        directory.delete('guid')
        directory.commit()

        self.assertEqual([
            {'event': 'commit', 'seqno': 0},
            {'event': 'sync', 'seqno': 0},
            {'guid': 'guid', 'event': 'create'},
            {'guid': 'guid', 'event': 'update'},
            {'guid': 'guid', 'event': 'update_blob', 'prop': 'blob', 'seqno': 2},
            {'guid': 'guid', 'event': 'update'},
            {'guid': 'guid', 'event': 'delete'},
            {'event': 'commit', 'seqno': 3}
            ],
            events)

    def test_diff(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        def read_diff(diff):
            result = []
            for meta, data in diff:
                if hasattr(data, 'read'):
                    result.append((meta, data.read()))
                else:
                    result.append((meta, data))
            return result

        self.override(time, 'time', lambda: 1)
        directory.create_with_guid('1', {'prop': '1'})
        directory.set_blob('1', 'blob', StringIO('1'))
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))

        self.override(time, 'time', lambda: 2)
        directory.create_with_guid('2', {'prop': '2'})
        directory.set_blob('2', 'blob', StringIO('2'))
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        self.override(time, 'time', lambda: 3)
        directory.create_with_guid('3', {'prop': '3'})
        for i in os.listdir('3/3'):
            os.utime('3/3/%s' % i, (3, 3))

        sequence, diff = directory.diff(xrange(100), 2)
        self.assertEqual([
            ({'guid': '1', 'prop': 'blob', 'mtime': 1, 'digest': hashlib.sha1('1').hexdigest()}, '1'),
            ({'guid': '1'}, {
                'guid': {'value': '1', 'mtime': 1},
                'layer': {'value': ['public'], 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'user': {'value': [], 'mtime': 1}}),
            ({'guid': '2', 'prop': 'blob', 'mtime': 2, 'digest': hashlib.sha1('2').hexdigest()}, '2'),
            ({'guid': '2'}, {
                'guid': {'value': '2', 'mtime': 2},
                'layer': {'value': ['public'], 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'user': {'value': [], 'mtime': 2}}),
            ({'guid': '3'}, {
                'guid': {'value': '3', 'mtime': 3},
                'layer': {'value': ['public'], 'mtime': 3},
                'ctime': {'value': 3, 'mtime': 3},
                'prop': {'value': '3', 'mtime': 3},
                'mtime': {'value': 3, 'mtime': 3},
                'user': {'value': [], 'mtime': 3}}),
            ],
            read_diff(diff))
        self.assertEqual([0, 5], sequence)

        sequence, diff = directory.diff([3, 4], 2)
        self.assertEqual([
            ({'guid': '2', 'prop': 'blob', 'mtime': 2, 'digest': hashlib.sha1('2').hexdigest()}, '2'),
            ({'guid': '2'}, {
                'guid': {'value': '2', 'mtime': 2},
                'layer': {'value': ['public'], 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'user': {'value': [], 'mtime': 2}}),
            ],
            read_diff(diff))
        self.assertEqual([3, 4], sequence)

        sequence, diff = directory.diff([3], 2)
        self.assertEqual([
            ],
            read_diff(diff))
        self.assertEqual([], sequence)

    def test_merge_New(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)

        self.override(time, 'time', lambda: 1)
        directory1.create_with_guid('1', {'prop': '1'})
        directory1.set_blob('1', 'blob', StringIO('1'))
        for i in os.listdir('document1/1/1'):
            os.utime('document1/1/1/%s' % i, (1, 1))

        self.override(time, 'time', lambda: 2)
        directory1.create_with_guid('2', {'prop': '2'})
        directory1.set_blob('2', 'blob', StringIO('2'))
        for i in os.listdir('document1/2/2'):
            os.utime('document1/2/2/%s' % i, (2, 2))

        self.override(time, 'time', lambda: 3)
        directory1.create_with_guid('3', {'prop': '3'})
        for i in os.listdir('document1/3/3'):
            os.utime('document1/3/3/%s' % i, (3, 3))

        __, diff = directory1.diff(xrange(100), 2)
        directory2 = Directory('document2', Document, IndexWriter)
        directory2.merge(diff)

        self.assertEqual(
                sorted([
                    {'layer': ['public'], 'ctime': 1, 'prop': '1', 'user': [], 'mtime': 1, 'guid': '1'},
                    {'layer': ['public'], 'ctime': 2, 'prop': '2', 'user': [], 'mtime': 2, 'guid': '2'},
                    {'layer': ['public'], 'ctime': 3, 'prop': '3', 'user': [], 'mtime': 3, 'guid': '3'},
                    ]),
                sorted([i.properties() for i in directory2.find(0, 1024)[0]]))

        doc = directory2.get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        self.assertEqual(1, doc.meta('layer')['mtime'])
        self.assertEqual(1, doc.meta('ctime')['mtime'])
        self.assertEqual(1, doc.meta('prop')['mtime'])
        self.assertEqual(1, doc.meta('user')['mtime'])
        self.assertEqual(1, doc.meta('mtime')['mtime'])
        self.assertEqual(1, doc.meta('blob')['mtime'])

        doc = directory2.get('2')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('layer')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])

        doc = directory2.get('3')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(3, doc.meta('guid')['mtime'])
        self.assertEqual(3, doc.meta('layer')['mtime'])
        self.assertEqual(3, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('prop')['mtime'])
        self.assertEqual(3, doc.meta('user')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(None, doc.meta('blob'))

    def test_merge_Update(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)
        directory2 = Directory('document2', Document, IndexWriter)

        self.override(time, 'time', lambda: 1)
        directory1.create_with_guid('guid', {})
        directory1.set_blob('guid', 'blob', StringIO('1'))
        for i in os.listdir('document1/gu/guid'):
            os.utime('document1/gu/guid/%s' % i, (1, 1))

        self.override(time, 'time', lambda: 2)
        directory2.create_with_guid('guid', {})
        directory2.set_blob('guid', 'blob', StringIO('2'))
        for i in os.listdir('document2/gu/guid'):
            os.utime('document2/gu/guid/%s' % i, (2, 2))

        self.assertEqual(
                [{'layer': ['public'], 'ctime': 2, 'user': [], 'mtime': 2, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('layer')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        __, diff = directory1.diff(xrange(100), 2)
        directory2.merge(diff)

        self.assertEqual(
                [{'layer': ['public'], 'ctime': 2, 'user': [], 'mtime': 2, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('layer')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        os.utime('document1/gu/guid/mtime', (3, 3))
        __, diff = directory1.diff(xrange(100), 2)
        directory2.merge(diff)

        self.assertEqual(
                [{'layer': ['public'], 'ctime': 2, 'user': [], 'mtime': 1, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(3, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('layer')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        os.utime('document1/gu/guid/blob', (4, 4))
        __, diff = directory1.diff(xrange(100), 2)
        directory2.merge(diff)

        self.assertEqual(
                [{'layer': ['public'], 'ctime': 2, 'user': [], 'mtime': 1, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(4, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('layer')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(4, doc.meta('blob')['mtime'])
        self.assertEqual('1', file('document2/gu/guid/blob.blob').read())

    def test_migrate(self):

        class Document(document.Document):

            @active_property(prefix='P', localized=True)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.touch(
                ('gu/guid/.seqno', ''),
                ('gu/guid/guid', '"guid"'),
                ('gu/guid/guid.seqno', ''),
                ('gu/guid/ctime', '1'),
                ('gu/guid/ctime.seqno', ''),
                ('gu/guid/mtime', '1'),
                ('gu/guid/mtime.seqno', ''),
                ('gu/guid/layer', '["public"]'),
                ('gu/guid/layer.seqno', ''),
                ('gu/guid/user', '["me"]'),
                ('gu/guid/user.seqno', ''),
                ('gu/guid/prop', '"prop"'),
                ('gu/guid/prop.seqno', ''),
                ('gu/guid/blob', 'blob'),
                ('gu/guid/blob.seqno', ''),
                ('gu/guid/blob.sha1', 'digest'),
                )
        for i in os.listdir('gu/guid'):
            os.utime('gu/guid/%s' % i, (1, 1))

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass
        assert exists('layout')
        self.assertEqual(str(directory_._LAYOUT_VERSION), file('layout').read())

        assert not exists('gu/guid/.seqno')
        assert not exists('gu/guid/guid.seqno')
        assert not exists('gu/guid/ctime.seqno')
        assert not exists('gu/guid/mtime.seqno')
        assert not exists('gu/guid/layer.seqno')
        assert not exists('gu/guid/user.seqno')
        assert not exists('gu/guid/prop.seqno')
        assert not exists('gu/guid/blob.seqno')
        assert not exists('gu/guid/blob.sha1')
        assert exists('gu/guid/blob.blob')

        def test_meta():
            doc = directory.get('guid')
            self.assertEqual(
                    {'value': 'guid', 'mtime': 1, 'seqno': 1},
                    doc.meta('guid'))
            self.assertEqual(
                    {'value': 1, 'mtime': 1, 'seqno': 1},
                    doc.meta('ctime'))
            self.assertEqual(
                    {'value': 1, 'mtime': 1, 'seqno': 1},
                    doc.meta('mtime'))
            self.assertEqual(
                    {'value': ['public'], 'mtime': 1, 'seqno': 1},
                    doc.meta('layer'))
            self.assertEqual(
                    {'value': ['me'], 'mtime': 1, 'seqno': 1},
                    doc.meta('user'))
            self.assertEqual(
                    {'value': {env.DEFAULT_LANG: 'prop'}, 'mtime': 1, 'seqno': 1},
                    doc.meta('prop'))
            self.assertEqual(
                    {'digest': 'digest', 'mtime': 1, 'seqno': 1, 'path': tests.tmpdir + '/gu/guid/blob.blob'},
                    doc.meta('blob'))
            self.assertEqual('blob', file('gu/guid/blob.blob').read())

        test_meta()

        directory.close()
        with file('layout', 'w') as f:
            f.write('*')
        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass
        self.assertEqual(str(directory_._LAYOUT_VERSION), file('layout').read())

        test_meta()


if __name__ == '__main__':
    tests.main()
