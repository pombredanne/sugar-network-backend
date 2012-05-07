#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import sys
import stat
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

        directory.create({'slotted': 'slotted', 'not_slotted': 'not_slotted'})

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

        guid = directory.create({'term': 'term', 'not_term': 'not_term'})

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

        guid = directory.create({'no': 'foo', 'yes': 'bar'})

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

        guid = directory.create({'wo_default': 'wo_default'})

        docs, total = directory.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('default', 'wo_default', 'not_stored_default')],
                [(i.w_default, i.wo_default, i.not_stored_default) for i in docs])

        self.assertRaises(RuntimeError, directory.create, {})

    def test_properties_Blob(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertRaises(RuntimeError, directory.create, {'blob': 'probe'})

        guid = directory.create({})

        self.assertRaises(RuntimeError, directory.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: directory.get(guid).blob)
        self.assertRaises(RuntimeError, directory.update, guid, {'blob': 'foo'})

        data = 'payload'

        directory.set_blob(guid, 'blob', StringIO(data))
        self.assertEqual(data, directory.get_blob(guid, 'blob').read())
        self.assertEqual(
                {'size': len(data), 'sha1sum': hashlib.sha1(data).hexdigest()},
                directory.stat_blob(guid, 'blob'))

    def test_find_MaxLimit(self):

        class Document(document.Document):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({})
        directory.create({})
        directory.create({})

        env.find_limit.value = 1
        docs, total = directory.find(0, 1024)
        self.assertEqual(1, len([i for i in docs]))

    def test_update(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop_1(self, value):
                return value

            @prop_1.setter
            def prop_1(self, value):
                return value

            @active_property(StoredProperty)
            def prop_2(self, value):
                return value

            @prop_2.setter
            def prop_2(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop_1': '1', 'prop_2': '2'})
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

        guid_1 = directory.create({'prop': '1'})
        guid_2 = directory.create({'prop': '2'})
        guid_3 = directory.create({'prop': '3'})

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
                ('1/1/.seqno', ''),

                ('1/1/guid', '1'),
                ('1/1/ctime', '1'),
                ('1/1/mtime', '1'),
                ('1/1/prop', '"prop-1"'),
                ('1/1/layers', '["public"]'),
                ('1/1/author', '["me"]'),

                ('2/2/.seqno', ''),
                ('2/2/guid', '2'),
                ('2/2/ctime', '2'),
                ('2/2/mtime', '2'),
                ('2/2/prop', '"prop-2"'),
                ('2/2/layers', '["public"]'),
                ('2/2/author', '["me"]'),
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

            @prop.setter
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create_with_guid('guid', {'prop': 'foo'})
        self.assertEqual(
                [('guid', 'foo', ['me'], ['public'])],
                [(i.guid, i.prop, i.author, i.layers) for i in directory.find(0, 1024)[0]])

        directory.update(guid, {'prop': 'probe'})
        self.assertEqual(
                [('guid', 'probe')],
                [(i.guid, i.prop) for i in directory.find(0, 1024)[0]])

    def test_AssertPermissions(self):

        class Document(document.Document):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.metadata['prop']._permissions = 0
        guid = directory.create({})
        doc = directory.get(guid)

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, lambda: doc['prop'])
        directory.metadata['prop']._permissions = env.ACCESS_READ
        doc['prop']

        directory.metadata['prop']._permissions = 0
        documents, total = directory.find(0, 100, reply=['guid', 'prop'])
        self.assertRaises(env.Forbidden, lambda: documents.next().prop)
        directory.metadata['prop']._permissions = env.ACCESS_READ
        documents, total = directory.find(0, 100, reply=['guid', 'prop'])
        documents.next().prop

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        directory.create({'prop': '1'})

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        directory.create({'prop': '1'})

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.update, guid, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        self.assertRaises(env.Forbidden, directory.update, guid, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        directory.update(guid, {'prop': '1'})

    def test_authorize_property_Blobs(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({})

        directory.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, directory.get_blob, guid, 'blob')
        directory.metadata['blob']._permissions = env.ACCESS_READ
        directory.get_blob(guid, 'blob')

        directory.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, directory.set_blob, guid, 'blob', StringIO('data'))
        directory.metadata['blob']._permissions = env.ACCESS_WRITE
        directory.set_blob(guid, 'blob', StringIO('data'))

    def test_times(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop': '1'})
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

    def test_UpdateInternalProps(self):

        class Document(document.Document):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertRaises(env.Forbidden, directory.create, {'ctime': 1})
        self.assertRaises(env.Forbidden, directory.create, {'mtime': 1})

    def test_seqno(self):

        class Document(document.Document):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({})
        self.assertEqual(
                os.stat('%s/%s/.seqno' % (guid[:2], guid)).st_mtime,
                directory.get(guid).get('seqno', raw=True))
        self.assertEqual(1, directory.get(guid).get('seqno', raw=True))

        guid_2 = directory.create({})
        self.assertEqual(
                os.stat('%s/%s/.seqno' % (guid_2[:2], guid_2)).st_mtime,
                directory.get(guid_2).get('seqno', raw=True))
        self.assertEqual(2, directory.get(guid_2).get('seqno', raw=True))

    def test_merge_New(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop': '2'})

        ts = int(time.time())
        directory.merge('1', {
            'guid': ('1', 1),
            'prop': ('1', 1),
            'ctime': (1, 1),
            'mtime': (1, 1),
            'layers': (['public'], 1),
            'author': (['me'], 1),
            })
        directory.merge('3', {
            'guid': ('3', ts + 60),
            'prop': ('3', ts + 60),
            'ctime': (ts + 60, ts + 60),
            'mtime': (ts + 60, ts + 60),
            'layers': (['public'], ts + 60),
            'author': (['me'], ts + 60),
            })

        self.assertEqual(
                [('1', '1', 1), (guid, '2', directory.get(guid)['ctime']), ('3', '3', ts + 60)],
                [(i.guid, i.prop, i.ctime) for i in directory.find(0, 100)[0]])

    def test_diff(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'prop': '1'})
        directory.create({'prop': '2'})
        directory.create({'prop': '3'})
        directory.create({'prop': '4'})

        directory_._DIFF_PAGE_SIZE = 2
        diff_rage, docs = directory.diff(xrange(10))

        self.assertEqual([None, None], diff_rage)
        self.assertEqual(
                ['1', '2', '3', '4'],
                [diff.get('prop')[0] for guid, diff in docs])
        self.assertEqual([1, 4], diff_rage)


if __name__ == '__main__':
    tests.main()
