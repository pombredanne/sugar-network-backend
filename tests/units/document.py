#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import sys
import stat
import json
import time
import urllib2
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

            @active_property(BlobProperty, mime_type='application/json')
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertRaises(RuntimeError, directory.create, {'blob': 'probe', 'user': []})

        guid = directory.create({'user': []})
        blob_path = join(tests.tmpdir, guid[:2], guid, 'blob')

        self.assertRaises(RuntimeError, directory.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: directory.get(guid).blob)
        self.assertRaises(RuntimeError, directory.update, guid, {'blob': 'foo'})

        data = 'payload'
        directory.set_blob(guid, 'blob', StringIO(data))
        self.assertEqual({
            'seqno': 2,
            'mtime': os.stat(blob_path).st_mtime,
            'digest': hashlib.sha1(data).hexdigest(),
            'path': join(tests.tmpdir, guid[:2], guid, 'blob.blob'),
            'mime_type': 'application/json',
            },
            directory.get(guid).meta('blob'))
        self.assertEqual(data, file(blob_path + '.blob').read())

        data = json.dumps({'foo': -1})
        directory.set_blob(guid, 'blob', {'foo': -1})
        self.assertEqual({
            'seqno': 3,
            'mtime': os.stat(blob_path).st_mtime,
            'digest': hashlib.sha1(data).hexdigest(),
            'path': join(tests.tmpdir, guid[:2], guid, 'blob.blob'),
            'mime_type': 'application/json',
            },
            directory.get(guid).meta('blob'))
        self.assertEqual(data, file(blob_path + '.blob').read())

    def test_properties_Override(self):

        class Document(document.Document):

            @active_property(slot=1, default=1)
            def prop1(self, value):
                return value

            @active_property(slot=2, default=2)
            def prop2(self, value):
                return -1

            @active_property(BlobProperty)
            def blob(self, meta):
                meta['path'] = 'new-blob'
                return meta

            @active_property(BlobProperty)
            def empty_blob(self, meta):
                return {'url': 'url'}

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        guid = directory.create({'user': []})
        doc = directory.get(guid)

        self.touch(('new-blob', 'new-blob'))
        directory.set_blob(guid, 'blob', StringIO('old-blob'))

        self.assertEqual('new-blob', doc.meta('blob')['path'])
        self.assertEqual({'url': 'url'}, doc.meta('empty_blob'))
        self.assertEqual('1', doc.meta('prop1')['value'])
        self.assertEqual(-1, doc.meta('prop2'))

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
                ('1/1/user', '{"value": ["me"]}'),
                ('1/1/seqno', '{"value": 0}'),

                ('2/2/guid', '{"value": "2"}'),
                ('2/2/ctime', '{"value": 2}'),
                ('2/2/mtime', '{"value": 2}'),
                ('2/2/prop', '{"value": "prop-2"}'),
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

        guid = directory.create(guid='guid', prop='foo', user=[])
        self.assertEqual(
                [('guid', 'foo', [])],
                [(i.guid, i.prop, i.user) for i in directory.find(0, 1024)[0]])

        directory.update(guid, {'prop': 'probe'})
        self.assertEqual(
                [('guid', 'probe')],
                [(i.guid, i.prop) for i in directory.find(0, 1024)[0]])

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

    def test_diff(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create(guid='1', prop='1', ctime=1, mtime=1)
        directory.set_blob('1', 'blob', StringIO('1'))
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))

        directory.create(guid='2', prop='2', ctime=2, mtime=2)
        directory.set_blob('2', 'blob', StringIO('2'))
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        directory.create(guid='3', prop='3', ctime=3, mtime=3)
        for i in os.listdir('3/3'):
            os.utime('3/3/%s' % i, (3, 3))

        self.assertEqual([
            ({'guid': '1', 'prop': 'blob', 'mtime': 1, 'digest': hashlib.sha1('1').hexdigest(), 'mime_type': 'application/octet-stream'}, '1'),
            ({'seqno': 2, 'guid': '1'}, {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'user': {'value': [], 'mtime': 1}}),
            ({'guid': '2', 'prop': 'blob', 'mtime': 2, 'digest': hashlib.sha1('2').hexdigest(), 'mime_type': 'application/octet-stream'}, '2'),
            ({'seqno': 4, 'guid': '2'}, {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'user': {'value': [], 'mtime': 2}}),
            ({'seqno': 5, 'guid': '3'}, {
                'guid': {'value': '3', 'mtime': 3},
                'ctime': {'value': 3, 'mtime': 3},
                'prop': {'value': '3', 'mtime': 3},
                'mtime': {'value': 3, 'mtime': 3},
                'user': {'value': [], 'mtime': 3}}),
            ],
            read_diff(directory, xrange(100), 2))

        self.assertEqual([
            ({'guid': '2', 'prop': 'blob', 'mtime': 2, 'digest': hashlib.sha1('2').hexdigest(), 'mime_type': 'application/octet-stream'}, '2'),
            ({'seqno': 4, 'guid': '2'}, {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'user': {'value': [], 'mtime': 2}}),
            ],
            read_diff(directory, [3, 4], 2))

        self.assertEqual([
            ],
            read_diff(directory, [3], 2))

        self.assertEqual([
            ],
            read_diff(directory, xrange(6, 100), 2))
        directory.update(guid='2', prop='22')
        self.assertEqual([
            ({'seqno': 6, 'guid': '2'}, {
                'prop': {'value': '22', 'mtime': os.stat('2/2/prop').st_mtime},
                }),
            ],
            read_diff(directory, xrange(6, 100), 2))

    def test_diff_WithBlobsSetByUrl(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create(guid='1', ctime=1, mtime=1)
        directory.set_blob('1', 'blob', url='http://sugarlabs.org')
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))

        data = urllib2.urlopen('http://sugarlabs.org').read()
        self.assertEqual([
            ({'guid': '1', 'prop': 'blob', 'mtime': 1, 'digest': None, 'mime_type': 'application/octet-stream'}, data),
            ({'seqno': 2, 'guid': '1'}, {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'user': {'value': [], 'mtime': 1}}),
            ],
            read_diff(directory, xrange(100), 2))

    def test_merge_New(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)

        directory1.create(guid='1', prop='1', ctime=1, mtime=1)
        directory1.set_blob('1', 'blob', StringIO('1'))
        for i in os.listdir('document1/1/1'):
            os.utime('document1/1/1/%s' % i, (1, 1))

        directory1.create(guid='2', prop='2', ctime=2, mtime=2)
        directory1.set_blob('2', 'blob', StringIO('2'))
        for i in os.listdir('document1/2/2'):
            os.utime('document1/2/2/%s' % i, (2, 2))

        directory1.create(guid='3', prop='3', ctime=3, mtime=3)
        for i in os.listdir('document1/3/3'):
            os.utime('document1/3/3/%s' % i, (3, 3))

        directory2 = Directory('document2', Document, IndexWriter)
        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, **header)

        self.assertEqual(
                sorted([
                    {'ctime': 1, 'prop': '1', 'user': [], 'mtime': 1, 'guid': '1'},
                    {'ctime': 2, 'prop': '2', 'user': [], 'mtime': 2, 'guid': '2'},
                    {'ctime': 3, 'prop': '3', 'user': [], 'mtime': 3, 'guid': '3'},
                    ]),
                sorted([i.properties() for i in directory2.find(0, 1024)[0]]))

        doc = directory2.get('1')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        self.assertEqual(1, doc.meta('ctime')['mtime'])
        self.assertEqual(1, doc.meta('prop')['mtime'])
        self.assertEqual(1, doc.meta('user')['mtime'])
        self.assertEqual(1, doc.meta('mtime')['mtime'])
        self.assertEqual(1, doc.meta('blob')['mtime'])

        doc = directory2.get('2')
        self.assertEqual(4, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])

        doc = directory2.get('3')
        self.assertEqual(5, doc.get('seqno'))
        self.assertEqual(3, doc.meta('guid')['mtime'])
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

        directory1.create(guid='guid', ctime=1, mtime=1)
        directory1.set_blob('guid', 'blob', StringIO('1'))
        for i in os.listdir('document1/gu/guid'):
            os.utime('document1/gu/guid/%s' % i, (1, 1))

        directory2.create(guid='guid', ctime=2, mtime=2)
        directory2.set_blob('guid', 'blob', StringIO('2'))
        for i in os.listdir('document2/gu/guid'):
            os.utime('document2/gu/guid/%s' % i, (2, 2))

        self.assertEqual(
                [{'ctime': 2, 'user': [], 'mtime': 2, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, **header)

        self.assertEqual(
                [{'ctime': 2, 'user': [], 'mtime': 2, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        os.utime('document1/gu/guid/mtime', (3, 3))
        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, **header)

        self.assertEqual(
                [{'ctime': 2, 'user': [], 'mtime': 1, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(3, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('blob')['mtime'])
        self.assertEqual('2', file('document2/gu/guid/blob.blob').read())

        os.utime('document1/gu/guid/blob', (4, 4))
        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, **header)

        self.assertEqual(
                [{'ctime': 2, 'user': [], 'mtime': 1, 'guid': 'guid'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('guid')
        self.assertEqual(4, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('user')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(4, doc.meta('blob')['mtime'])
        self.assertEqual('1', file('document2/gu/guid/blob.blob').read())

    def test_merge_SeqnoLessMode(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)
        directory1.create(guid='1', prop='1', ctime=1, mtime=1)

        directory2 = Directory('document2', Document, IndexWriter)
        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, increment_seqno=False, **header)
        self.assertEqual(
                [{'ctime': 1, 'user': [], 'mtime': 1, 'guid': '1', 'prop': '1'}],
                [i.properties() for i in directory2.find(0, 1024)[0]])
        doc = directory2.get('1')
        self.assertEqual(0, doc.get('seqno'))
        self.assertEqual(0, doc.meta('guid')['seqno'])
        self.assertEqual(0, doc.meta('prop')['seqno'])

        directory3 = Directory('document3', Document, IndexWriter)
        for header, diff in directory1.diff(xrange(100), 2):
            directory3.merge(diff=diff, **header)
        self.assertEqual(
                [{'ctime': 1, 'user': [], 'mtime': 1, 'guid': '1', 'prop': '1'}],
                [i.properties() for i in directory3.find(0, 1024)[0]])
        doc = directory3.get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['seqno'])
        self.assertEqual(1, doc.meta('prop')['seqno'])

        directory1.update(guid='1', prop='2', ctime=2, mtime=2)

        for header, diff in directory1.diff(xrange(100), 2):
            directory3.merge(diff=diff, increment_seqno=False, **header)
        self.assertEqual(
                [{'ctime': 2, 'user': [], 'mtime': 2, 'guid': '1', 'prop': '2'}],
                [i.properties() for i in directory3.find(0, 1024)[0]])
        doc = directory3.get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['seqno'])
        self.assertEqual(1, doc.meta('prop')['seqno'])

        time.sleep(1)
        directory1.update(guid='1', prop='3', ctime=3, mtime=3)

        for header, diff in directory1.diff(xrange(100), 2):
            print diff
            directory3.merge(diff=diff, **header)
        self.assertEqual(
                [{'ctime': 3, 'user': [], 'mtime': 3, 'guid': '1', 'prop': '3'}],
                [i.properties() for i in directory3.find(0, 1024)[0]])
        doc = directory3.get('1')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['seqno'])
        self.assertEqual(2, doc.meta('prop')['seqno'])

    def test_merge_AvoidCalculatedBlobs(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return {'url': 'http://foo/bar', 'mime_type': 'image/png'}

        directory1 = Directory('document1', Document, IndexWriter)
        directory1.create(guid='guid', ctime=1, mtime=1)
        for i in os.listdir('document1/gu/guid'):
            os.utime('document1/gu/guid/%s' % i, (1, 1))

        directory2 = Directory('document2', Document, IndexWriter)
        for header, diff in directory1.diff(xrange(100), 2):
            directory2.merge(diff=diff, **header)

        doc = directory2.get('guid')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        assert not exists('document2/gu/guid/blob')


def read_diff(directory, *args, **kwargs):
    result = []
    for header, data in directory.diff(*args, **kwargs):
        if hasattr(data, 'read'):
            result.append((header, data.read()))
        else:
            result.append((header, data))
    return result


if __name__ == '__main__':
    tests.main()
