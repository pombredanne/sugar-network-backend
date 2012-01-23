#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import stat
import time
from cStringIO import StringIO
from os.path import join, exists

import gobject

from __init__ import tests

from active_document import document, storage, env, index
from active_document.document_class import active_property
from active_document.metadata import StoredProperty, BlobProperty
from active_document.metadata import CounterProperty
from active_document.metadata import AggregatorProperty


class DocumentTest(tests.Test):

    def test_ActiveProperty_Slotted(self):

        class Document(document.Document):

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

        class Document(document.Document):

            @active_property(slot=1)
            def prop_1(self, value):
                return value

            @active_property(slot=1)
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_ActiveProperty_Terms(self):

        class Document(document.Document):

            @active_property(prefix='T')
            def term(self, value):
                return value

            @active_property(StoredProperty)
            def not_term(self, value):
                return value

        doc = Document(term='term', not_term='not_term')
        self.assertEqual('T', doc.metadata['term'].prefix)

        doc.post()
        docs, total = Document.find(0, 100, request={'term': 'term'})
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, request={'not_term': 'not_term'})
        self.assertEqual(0, Document.find(0, 100, query='not_term:not_term')[-1])
        self.assertEqual(1, Document.find(0, 100, query='not_term:=not_term')[-1])

    def test_ActiveProperty_TermsUnique(self):

        class Document(document.Document):

            @active_property(prefix='P')
            def prop_1(self, value):
                return value

            @active_property(prefix='P')
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_ActiveProperty_Multiple(self):

        class Document(document.Document):

            @active_property(prefix='A', multiple=True)
            def by_space(self, value):
                return value

            @active_property(prefix='b', multiple=True, separator=';')
            def by_semicolon(self, value):
                return value

        by_space = ' 1  2\t3 \n'
        by_semicolon = '  4; 5 ;\t6\n'

        doc = Document(by_space=by_space, by_semicolon=by_semicolon)
        self.assertEqual(True, doc.metadata['by_space'].multiple)
        self.assertEqual(None, doc.metadata['by_space'].separator)
        self.assertEqual(True, doc.metadata['by_semicolon'].multiple)
        self.assertEqual(';', doc.metadata['by_semicolon'].separator)

        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [(by_space, by_semicolon)],
                [(i.by_space, i.by_semicolon) for i in docs])

        self.assertEqual(1, Document.find(0, 100, request={'by_space': '1'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_space': '2'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_space': '3'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '4'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '5'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '6'})[-1])

    def test_ActiveProperty_FullTextSearch(self):

        class Document(document.Document):

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

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.assertRaises(RuntimeError, Document, blob='probe')

        doc = Document()
        doc.post()

        self.assertRaises(RuntimeError, Document.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).blob)
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).__setitem__('blob', 'foo'))

        doc.set_blob('blob', StringIO('data'))
        stream = StringIO()
        for i in doc.get_blob('blob'):
            stream.write(i)
        self.assertEqual('data', stream.getvalue())

    def test_AggregatorProperty(self):

        voter = [-1]

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return voter[0]

        class Document(document.Document):

            @active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        doc = Document()
        self.assertEqual('0', doc['vote'])
        self.assertEqual('0', doc['counter'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', 'foo')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '-1')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '')
        doc.post()
        path = join('document', doc.guid[:2], doc.guid)
        assert not exists(join(path, 'vote'))

        doc = Document(doc.guid)
        doc['vote'] = '100'
        self.assertEqual('1', doc['vote'])
        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('1', '1')],
                [(i.vote, i.counter) for i in docs])

        voter[:] = [-2]

        doc_2 = Document(doc.guid)
        self.assertEqual('0', doc_2['vote'])
        self.assertEqual('1', doc_2['counter'])
        doc_2['vote'] = '1'
        doc_2.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('1', '2')],
                [(i.vote, i.counter) for i in docs])

        voter[:] = [-1]

        doc_3 = Document(doc.guid)
        self.assertEqual('1', doc_3['vote'])
        self.assertEqual('2', doc_3['counter'])
        doc_3['vote'] = '0'
        doc_3.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('0', '1')],
                [(i.vote, i.counter) for i in docs])

        voter[:] = [-2]

        doc_4 = Document(doc.guid)
        self.assertEqual('1', doc_4['vote'])
        self.assertEqual('1', doc_4['counter'])
        doc_4['vote'] = '0'
        doc_4.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('0', '0')],
                [(i.vote, i.counter) for i in docs])

    def test_AggregatorProperty_DoNotAggregateOnNoChanches(self):

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return -1

        class Document(document.Document):

            @active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        doc = Document()
        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total.value)
        self.assertEqual(
                [('0', '0')],
                [(i.vote, i.counter) for i in docs])

    def test_find_MaxLimit(self):

        class Document(document.Document):
            pass

        Document().post()
        Document().post()
        Document().post()

        env.find_limit.value = 1
        docs, total = Document.find(0, 1024)
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

        class Document(document.Document):

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

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return -1

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(CounterProperty, slot=2)
            def counter(self, value):
                return value

            @active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        self.touch(
                ('document/1/1/.document', ''),

                ('document/1/1/seqno', '0'),
                ('document/1/1/ctime', '1'),
                ('document/1/1/mtime', '1'),
                ('document/1/1/prop', 'prop-1'),
                ('document/1/1/vote/-1', ''),
                ('document/1/1/counter', '0'),

                ('document/2/2/seqno', '0'),
                ('document/2/2/.document', ''),
                ('document/2/2/ctime', '2'),
                ('document/2/2/mtime', '2'),
                ('document/2/2/prop', 'prop-2'),
                ('document/2/2/vote/-2', ''),
                ('document/2/2/vote/-3', ''),
                ('document/2/2/counter', '0'),
                )
        os.chmod('document/1/1/vote/-1', os.stat('document/1/1/vote/-1').st_mode | stat.S_ISVTX)
        os.chmod('document/2/2/vote/-2', os.stat('document/2/2/vote/-2').st_mode | stat.S_ISVTX)
        os.chmod('document/2/2/vote/-3', os.stat('document/2/2/vote/-3').st_mode | stat.S_ISVTX)

        Document.init()
        for i in Document.populate():
            pass

        doc = Document('1')
        self.assertEqual('1', doc['ctime'])
        self.assertEqual('1', doc['mtime'])
        self.assertEqual('1', doc['vote'])
        self.assertEqual('1', doc['counter'])
        self.assertEqual('prop-1', doc['prop'])

        doc = Document('2')
        self.assertEqual('2', doc['ctime'])
        self.assertEqual('2', doc['mtime'])
        self.assertEqual('0', doc['vote'])
        self.assertEqual('2', doc['counter'])
        self.assertEqual('prop-2', doc['prop'])

        self.assertEqual(
                [
                    ('1', '1', '1', '1', 'prop-1'),
                    ('2', '2', '0', '2', 'prop-2'),
                    ],
                [(i.ctime, i.mtime, i.vote, i.counter, i.prop) for i in Document.find(0, 10)[0]])

    def test_on_create(self):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

            def on_create(self, properties, cache):
                document.Document.on_create(self, properties, cache)
                cache['guid'] = properties.pop('manual_guid')
                properties['prop'] = 'foo'

        doc = Document(manual_guid='guid')
        doc.post()
        self.assertEqual(
                [('guid', 'foo')],
                [(i.guid, i.prop) for i in Document.find(0, 1024)[0]])

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

        class Document(document.Document):

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

        class Document(document.Document):

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

    def test_typecast_int(self):

        class Document(document.Document):

            @active_property(slot=1, typecast=int)
            def prop(self, value):
                return value

        self.assertRaises(RuntimeError, lambda: Document(prop='foo').post())
        self.assertRaises(RuntimeError, lambda: Document(prop='0.').post())

        Document(prop='-1').post()
        self.assertEqual(
                ['-1'],
                [i.prop for i in Document.find(0, 1)[0]])

    def test_typecast_bool(self):

        class Document(document.Document):

            @active_property(slot=1, typecast=bool)
            def prop(self, value):
                return value

        self.assertRaises(RuntimeError, lambda: Document(prop='foo').post())
        self.assertRaises(RuntimeError, lambda: Document(prop='true').post())
        self.assertRaises(RuntimeError, lambda: Document(prop='True').post())

        Document(prop='0').post()
        self.assertEqual(
                sorted(['0']),
                sorted([i.prop for i in Document.find(0, 10)[0]]))

        Document(prop='1').post()
        self.assertEqual(
                sorted(['0', '1']),
                sorted([i.prop for i in Document.find(0, 10)[0]]))

        Document(prop='-100').post()
        self.assertEqual(
                sorted(['0', '1', '1']),
                sorted([i.prop for i in Document.find(0, 10)[0]]))

    def test_typecast_enum(self):

        class Document(document.Document):

            @active_property(slot=1, typecast=['foo', 'bar'], default='foo')
            def prop(self, value):
                return value

            @active_property(slot=2, typecast=['foo', 'bar'], multiple=True, default='foo')
            def multiple_prop(self, value):
                return value

        self.assertRaises(RuntimeError, lambda: Document(prop='1').post())
        self.assertRaises(RuntimeError, lambda: Document(prop='true').post())

        Document(prop='foo').post()
        self.assertEqual(
                sorted(['foo']),
                sorted([i.prop for i in Document.find(0, 10)[0]]))

        Document(prop='bar').post()
        self.assertEqual(
                sorted(['foo', 'bar']),
                sorted([i.prop for i in Document.find(0, 10)[0]]))

        self.assertRaises(RuntimeError, lambda: Document(multiple_prop='1').post())
        self.assertRaises(RuntimeError, lambda: Document(multiple_prop='foo 2').post())
        self.assertRaises(RuntimeError, lambda: Document(multiple_prop='3 bar').post())

        Document(multiple_prop='bar      foo').post()
        self.assertEqual(
                sorted(['foo', 'foo', 'bar foo']),
                sorted([i.multiple_prop for i in Document.find(0, 10)[0]]))

    def test_authorize_document(self):

        class Document(document.Document):

            mode = 0

            @classmethod
            def authorize_document(cls, mode, document=None):
                if not (mode & cls.mode):
                    raise env.Forbidden()

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

        doc = Document()
        self.assertRaises(env.Forbidden, doc.post)
        Document.mode = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, doc.post)
        Document.mode = env.ACCESS_CREATE
        doc.post()

        Document.mode = 0
        self.assertRaises(env.Forbidden, Document.find, 0, 100)
        Document.mode = env.ACCESS_READ
        Document.find(0, 100)

        Document.mode = 0
        self.assertRaises(env.Forbidden, Document, doc.guid)
        Document.mode = env.ACCESS_READ
        Document(doc.guid)

        Document.mode = env.ACCESS_READ
        doc_2 = Document(doc.guid)
        self.assertRaises(env.Forbidden, doc.post)
        Document.mode = env.ACCESS_READ | env.ACCESS_CREATE
        self.assertRaises(env.Forbidden, doc.post)
        Document.mode = env.ACCESS_READ | env.ACCESS_WRITE
        doc.post()

        Document.mode = 0
        self.assertRaises(env.Forbidden, Document.delete, doc.guid)
        Document.mode = env.ACCESS_DELETE
        Document.delete(doc.guid)

    def test_authorize_property(self):

        class Document(document.Document):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

        Document.init()

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

        class Document(document.Document):

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

        class Document(document.Document):

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

        class Document(document.Document):
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

        self.assertRaises(env.Forbidden, lambda: Document(seqno=1))
        doc = Document()
        self.assertRaises(env.Forbidden, lambda: doc.__getitem__('seqno'))
        self.assertRaises(env.Forbidden, lambda: doc.__setitem__('seqno', 1))
        doc.post()

        doc = Document(raw=['seqno'], seqno=1024)
        self.assertEqual(1024, doc['seqno'])
        doc.post()
        self.assertEqual(
                [doc.guid],
                [i.guid for i in Document.find(0, 1024, request={'seqno': 1024})[0]])

    def test_seqno(self):

        class Document(document.Document):
            pass

        class Seqno(object):

            seqno = 0

            def next(self):
                Seqno.seqno += 1
                return Seqno.seqno

        Document.init()
        Document._seqno = Seqno()

        doc = Document()
        doc.post()
        self.assertEqual('1', Document(doc.guid, raw=['seqno']).seqno)

        doc = Document()
        doc.post()
        self.assertEqual('2', Document(doc.guid, raw=['seqno']).seqno)

        doc = Document(raw=['seqno'], seqno=1024)
        doc.post()
        self.assertEqual('1024', Document(doc.guid, raw=['seqno']).seqno)

        doc = Document()
        doc.post()
        self.assertEqual('3', Document(doc.guid, raw=['seqno']).seqno)

    def test_CounterProperty(self):

        class Document(document.Document):

            @active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @counter.setter
            def counter(self, value):
                return value

        doc = Document()
        doc.post()
        self.assertEqual(
                ['0'],
                [i.counter for i in Document.find(0, 10)[0]])
        self.assertEqual('0', Document(doc.guid).counter)

        doc = Document(doc.guid, raw=['counter'])
        doc.counter = 1
        doc.post()
        self.assertEqual(
                ['1'],
                [i.counter for i in Document.find(0, 10)[0]])
        self.assertEqual('1', Document(doc.guid).counter)

        doc = Document(doc.guid, raw=['counter'])
        doc.counter = 2
        doc.post()
        self.assertEqual(
                ['3'],
                [i.counter for i in Document.find(0, 10)[0]])
        self.assertEqual('3', Document(doc.guid).counter)

        doc = Document(doc.guid, raw=['counter'])
        doc.counter = -3
        doc.post()
        self.assertEqual(
                ['0'],
                [i.counter for i in Document.find(0, 10)[0]])
        self.assertEqual('0', Document(doc.guid).counter)

        doc_2 = Document(counter='3', raw=['counter'])
        doc_2.post()
        self.assertEqual(
                ['3'],
                [i.counter for i in Document.find(0, 10, request={'guid': doc_2.guid})[0]])
        self.assertEqual('3', Document(doc_2.guid).counter)

    def test_merge_AggregatorProperty(self):

        class Vote(AggregatorProperty):

            @property
            def value(self):
                pass

        class Document(document.Document):

            @active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        doc = Document()
        doc.post()

        ts = time.time()
        diff = {
                'counter': ('0', ts + 60),
                'vote': [(('enabled', True), ts + 60), (('None', True), ts + 60)],
                }
        doc.merge(diff)
        self.assertEqual(
                [(doc.guid, '1', '2')],
                [(i.guid, i.vote, i.counter) for i in Document.find(0, 100)[0]])


if __name__ == '__main__':
    tests.main()
