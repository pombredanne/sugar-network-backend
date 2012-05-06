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

from active_document import document, storage, env, index, document_class
from active_document.document_class import active_property
from active_document.metadata import StoredProperty, BlobProperty
from active_document.index import IndexWriter


class DocumentTest(tests.Test):

    def test_ActiveProperty_Slotted(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def slotted(self, value):
                return value

            @active_property(StoredProperty)
            def not_slotted(self, value):
                return value

        doc = Document(slotted='slotted', not_slotted='not_slotted')
        self.assertEqual(1, doc.metadata['slotted'].slot)

        doc.post()
        docs, total = Document.find(0, 100, order_by='slotted')
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('slotted', 'not_slotted')],
                [(i.slotted, i.not_slotted) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, order_by='not_slotted')

    def test_ActiveProperty_SlottedIUnique(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop_1(self, value):
                return value

            @active_property(slot=1)
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_ActiveProperty_Terms(self):

        class Document(TestDocument):

            @active_property(prefix='T')
            def term(self, value):
                return value

            @active_property(StoredProperty)
            def not_term(self, value):
                return value

        doc = Document(term='term', not_term='not_term')
        self.assertEqual('T', doc.metadata['term'].prefix)

        doc.post()
        docs, total = Document.find(0, 100, term='term')
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, not_term='not_term')
        self.assertEqual(0, Document.find(0, 100, query='not_term:not_term')[-1])
        self.assertEqual(1, Document.find(0, 100, query='not_term:=not_term')[-1])

    def test_ActiveProperty_TermsUnique(self):

        class Document(TestDocument):

            @active_property(prefix='P')
            def prop_1(self, value):
                return value

            @active_property(prefix='P')
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_ActiveProperty_FullTextSearch(self):

        class Document(TestDocument):

            @active_property(full_text=False, slot=1)
            def no(self, value):
                return value

            @active_property(full_text=True, slot=2)
            def yes(self, value):
                return value

        doc = Document(no='foo', yes='bar')
        self.assertEqual(False, doc.metadata['no'].full_text)
        self.assertEqual(True, doc.metadata['yes'].full_text)

        doc.post()
        self.assertEqual(0, Document.find(0, 100, query='foo')[-1])
        self.assertEqual(1, Document.find(0, 100, query='bar')[-1])

    def test_StoredProperty_Defaults(self):

        class Document(TestDocument):

            @active_property(StoredProperty, default='default')
            def w_default(self, value):
                return value

            @active_property(StoredProperty)
            def wo_default(self, value):
                return value

            @active_property(slot=1, default='not_stored_default')
            def not_stored_default(self, value):
                return value

        doc = Document(wo_default='wo_default')
        self.assertEqual('default', doc.metadata['w_default'].default)
        self.assertEqual(None, doc.metadata['wo_default'].default)
        self.assertEqual('not_stored_default', doc.metadata['not_stored_default'].default)

        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('default', 'wo_default', 'not_stored_default')],
                [(i.w_default, i.wo_default, i.not_stored_default) for i in docs])

        self.assertRaises(RuntimeError, Document)

    def test_properties_Blob(self):

        class Document(TestDocument):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.assertRaises(RuntimeError, Document, blob='probe')

        doc = Document()
        doc.post()

        self.assertRaises(RuntimeError, Document.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).blob)
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).__setitem__('blob', 'foo'))

        data = 'payload'

        doc.set_blob('blob', StringIO(data))
        self.assertEqual(data, doc.get_blob('blob').read())
        self.assertEqual(
                {'size': len(data), 'sha1sum': hashlib.sha1(data).hexdigest()},
                doc.stat_blob('blob'))

    def test_find_MaxLimit(self):

        class Document(TestDocument):
            pass

        Document().post()
        Document().post()
        Document().post()

        env.find_limit.value = 1
        docs, total = Document.find(0, 1024)
        self.assertEqual(1, len([i for i in docs]))

    def test_update(self):

        class Document(TestDocument):

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

        doc_1 = Document(prop_1='1', prop_2='2')
        doc_1.post()
        self.assertEqual(
                [('1', '2')],
                [(i.prop_1, i.prop_2) for i in Document.find(0, 1024)[0]])

        doc_1.prop_1 = '3'
        doc_1.prop_2 = '4'
        doc_1.post()
        self.assertEqual(
                [('3', '4')],
                [(i.prop_1, i.prop_2) for i in Document.find(0, 1024)[0]])

        doc_2 = Document(doc_1.guid)
        doc_2.prop_2 = '6'
        doc_2.post()
        self.assertEqual(
                [('3', '6')],
                [(i.prop_1, i.prop_2) for i in Document.find(0, 1024)[0]])

    def test_delete(self):

        class Document(TestDocument):

            @active_property(prefix='P')
            def prop(self, value):
                return value

        doc_1 = Document(prop='1')
        doc_1.post()

        doc_2 = Document(prop='2')
        doc_2.post()

        doc_3 = Document(prop='3')
        doc_3.post()

        self.assertEqual(
                ['1', '2', '3'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(doc_2.guid)
        self.assertEqual(
                ['1', '3'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(doc_3.guid)
        self.assertEqual(
                ['1'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(doc_1.guid)
        self.assertEqual(
                [],
                [i.prop for i in Document.find(0, 1024)[0]])

    def test_crawler(self):

        class Document(TestDocument):

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

        Document.init(tests.tmpdir, IndexWriter)
        for i in Document.populate():
            pass

        doc = Document('1')
        self.assertEqual(1, doc['ctime'])
        self.assertEqual(1, doc['mtime'])
        self.assertEqual('prop-1', doc['prop'])

        doc = Document('2')
        self.assertEqual(2, doc['ctime'])
        self.assertEqual(2, doc['mtime'])
        self.assertEqual('prop-2', doc['prop'])

        self.assertEqual(
                [
                    (1, 1, 'prop-1'),
                    (2, 2, 'prop-2'),
                    ],
                [(i.ctime, i.mtime, i.prop) for i in Document.find(0, 10)[0]])

    def test_on_create(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

            def on_create(self, properties, cache):
                TestDocument.on_create(self, properties, cache)
                cache['guid'] = properties.pop('manual_guid')
                properties['prop'] = 'foo'

        doc = Document(manual_guid='guid')
        doc.post()
        self.assertEqual(
                [('guid', 'foo', ['me'], ['public'])],
                [(i.guid, i.prop, i.author, i.layers) for i in Document.find(0, 1024)[0]])

        doc_2 = Document(doc.guid)
        doc_2.prop = 'probe'
        doc_2.post()
        self.assertEqual(
                [('guid', 'probe')],
                [(i.guid, i.prop) for i in Document.find(0, 1024)[0]])

        doc_3 = Document(manual_guid='guid2')
        doc_3.prop = 'bar'
        doc_3.post()
        self.assertEqual(
                [('guid', 'probe'), ('guid2', 'bar')],
                [(i.guid, i.prop) for i in Document.find(0, 1024)[0]])

    def test_on_modify(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

            def on_modify(self, properties):
                properties['prop'] = 'foo'

        doc = Document(prop='probe')
        doc.post()
        self.assertEqual(
                ['probe'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_2 = Document(doc.guid)
        doc_2.post()
        self.assertEqual(
                ['probe'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_3 = Document(doc.guid)
        doc_3.prop = 'trigger'
        doc_3.post()
        self.assertEqual(
                ['foo'],
                [i.prop for i in Document.find(0, 1024)[0]])

    def test_on_post(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

            def on_post(self, properties):
                properties['prop'] += '!'

        doc = Document(prop='probe')
        doc.post()
        self.assertEqual(
                ['probe!'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_2 = Document(doc.guid)
        doc_2.post()
        self.assertEqual(
                ['probe!'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_3 = Document(doc.guid)
        doc_3.prop = 'trigger'
        doc_3.post()
        self.assertEqual(
                ['trigger!'],
                [i.prop for i in Document.find(0, 1024)[0]])

    def test_AssertPermissions(self):

        class Document(TestDocument):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

        Document()

        Document.metadata['prop']._permissions = 0
        doc = Document()
        doc.post()

        Document.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, lambda: doc['prop'])
        Document.metadata['prop']._permissions = env.ACCESS_READ
        doc['prop']

        Document.metadata['prop']._permissions = 0
        documents, total = Document.find(0, 100, reply=['guid', 'prop'])
        self.assertRaises(env.Forbidden, lambda: documents.next().prop)
        Document.metadata['prop']._permissions = env.ACCESS_READ
        documents, total = Document.find(0, 100, reply=['guid', 'prop'])
        documents.next().prop

        Document.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, Document, prop='1')
        Document.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, Document, prop='1')
        Document.metadata['prop']._permissions = env.ACCESS_CREATE
        Document(prop='1')

        Document.metadata['prop']._permissions = 0
        doc_2 = Document()
        self.assertRaises(env.Forbidden, doc_2.__setitem__, 'prop', '1')
        Document.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, doc_2.__setitem__, 'prop', '1')
        Document.metadata['prop']._permissions = env.ACCESS_CREATE
        doc_2['prop'] = '1'

        Document.metadata['prop']._permissions = 0
        doc_2 = Document(doc.guid)
        self.assertRaises(env.Forbidden, doc_2.__setitem__, 'prop', '1')
        Document.metadata['prop']._permissions = env.ACCESS_CREATE
        self.assertRaises(env.Forbidden, doc_2.__setitem__, 'prop', '1')
        Document.metadata['prop']._permissions = env.ACCESS_WRITE
        doc_2['prop'] = '1'

    def test_authorize_property_Blobs(self):

        class Document(TestDocument):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        doc = Document()
        doc.post()

        Document.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, doc.get_blob, 'blob')
        Document.metadata['blob']._permissions = env.ACCESS_READ
        doc.get_blob('blob')

        Document.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, doc.set_blob, 'blob', StringIO('data'))
        Document.metadata['blob']._permissions = env.ACCESS_WRITE
        doc.set_blob('blob', StringIO('data'))

    def test_times(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

        doc = Document(prop='1')
        self.assertNotEqual(0, doc['ctime'])
        self.assertNotEqual(0, doc['mtime'])
        assert doc['ctime'] == doc['mtime']
        time.sleep(1)
        doc.post()
        assert doc['ctime'] == doc['mtime']

        doc_2 = Document(doc.guid)
        doc_2.post()
        assert doc_2['ctime'] == doc_2['mtime']

        doc_3 = Document(doc.guid)
        doc_3['prop'] = '2'
        doc_3.post()
        assert doc_3['ctime'] < doc_3['mtime']

    def test_UpdateInternalProps(self):

        class Document(TestDocument):
            pass

        self.assertRaises(env.Forbidden, lambda: Document(ctime=1))
        doc = Document()
        doc['ctime']
        self.assertRaises(env.Forbidden, lambda: doc.__setitem__('ctime', 1))
        doc.post()

        self.assertRaises(env.Forbidden, lambda: Document(mtime=1))
        doc = Document()
        doc['mtime']
        self.assertRaises(env.Forbidden, lambda: doc.__setitem__('mtime', 1))
        doc.post()

    def test_seqno(self):

        class Document(TestDocument):
            pass

        Document.init(tests.tmpdir, IndexWriter)

        doc_1 = Document()
        doc_1.post()
        self.assertEqual(
                os.stat('%s/%s/.seqno' % (doc_1.guid[:2], doc_1.guid)).st_mtime,
                Document(doc_1.guid).get('seqno', raw=True))
        self.assertEqual(1, Document(doc_1.guid).get('seqno', raw=True))

        doc_2 = Document()
        doc_2.post()
        self.assertEqual(
                os.stat('%s/%s/.seqno' % (doc_2.guid[:2], doc_2.guid)).st_mtime,
                Document(doc_2.guid).get('seqno', raw=True))
        self.assertEqual(2, Document(doc_2.guid).get('seqno', raw=True))

    def test_merge_New(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

        doc = Document(prop='2')
        doc.post()

        ts = int(time.time())
        Document.merge('1', {
            'guid': ('1', 1),
            'prop': ('1', 1),
            'ctime': (1, 1),
            'mtime': (1, 1),
            'layers': (['public'], 1),
            'author': (['me'], 1),
            })
        Document.merge('3', {
            'guid': ('3', ts + 60),
            'prop': ('3', ts + 60),
            'ctime': (ts + 60, ts + 60),
            'mtime': (ts + 60, ts + 60),
            'layers': (['public'], ts + 60),
            'author': (['me'], ts + 60),
            })

        self.assertEqual(
                [('1', '1', 1), (doc.guid, '2', doc.ctime), ('3', '3', ts + 60)],
                [(i.guid, i.prop, i.ctime) for i in Document.find(0, 100)[0]])

    def test_diff(self):

        class Document(TestDocument):

            @active_property(slot=1)
            def prop(self, value):
                return value

        Document(prop='1').post()
        Document(prop='2').post()
        Document(prop='3').post()
        Document(prop='4').post()

        document_class._DIFF_PAGE_SIZE = 2
        diff_rage, docs = Document.diff(xrange(10))

        self.assertEqual([None, None], diff_rage)
        self.assertEqual(
                ['1', '2', '3', '4'],
                [diff.get('prop')[0] for guid, diff in docs])
        self.assertEqual([1, 4], diff_rage)


class TestDocument(document.Document):

    def __init__(self, guid=None, indexed_props=None, **kwargs):
        self.init(tests.tmpdir, IndexWriter)
        document.Document.__init__(self, guid, indexed_props, **kwargs)


if __name__ == '__main__':
    tests.main()
