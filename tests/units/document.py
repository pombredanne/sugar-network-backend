#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

from cStringIO import StringIO

import gobject

from __init__ import tests

from active_document import document, storage, env, index
from active_document.metadata import StoredProperty, BlobProperty
from active_document.metadata import CounterProperty, IndexedProperty
from active_document.metadata import AggregatorProperty


class DocumentTest(tests.Test):

    def test_Property_Large(self):

        class Document(document.Document):

            @document.active_property(StoredProperty, large=True)
            def large(self, value):
                return value

            @large.setter
            def large(self, value):
                return value

        doc = Document(large='probe')
        self.assertEqual(True, doc.metadata['large'].large)

        doc.post()
        self.assertRaises(RuntimeError, Document.find, 0, 100, reply='large')
        self.assertEqual('probe', Document(doc.guid).large)

        doc2 = Document(doc.guid)
        doc2.large = 'foo'
        doc2.post()
        self.assertEqual('foo', Document(doc2.guid).large)

    def test_IndexedProperty_Slotted(self):

        class Document(document.Document):

            @document.active_property(slot=1)
            def slotted(self, value):
                return value

            @document.active_property(StoredProperty)
            def not_slotted(self, value):
                return value

        doc = Document(slotted='slotted', not_slotted='not_slotted')
        self.assertEqual(1, doc.metadata['slotted'].slot)

        doc.post()
        docs, total = Document.find(0, 100, order_by=['slotted'], group_by='slotted')
        self.assertEqual(1, total)
        self.assertEqual(
                [('slotted', 'not_slotted')],
                [(i.slotted, i.not_slotted) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, order_by=['not_slotted'])
        self.assertRaises(RuntimeError, Document.find, 0, 100, group_by='not_slotted')

    def test_IndexedProperty_SlottedIUnique(self):

        class Document(document.Document):

            @document.active_property(slot=1)
            def prop_1(self, value):
                return value

            @document.active_property(slot=1)
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_IndexedProperty_Terms(self):

        class Document(document.Document):

            @document.active_property(prefix='T')
            def term(self, value):
                return value

            @document.active_property(StoredProperty)
            def not_term(self, value):
                return value

        doc = Document(term='term', not_term='not_term')
        self.assertEqual('T', doc.metadata['term'].prefix)

        doc.post()
        docs, total = Document.find(0, 100, request={'term': 'term'})
        self.assertEqual(1, total)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, request={'not_term': 'not_term'})
        self.assertEqual(0, Document.find(0, 100, query='not_term:not_term')[-1])
        self.assertEqual(1, Document.find(0, 100, query='not_term:=not_term')[-1])

    def test_IndexedProperty_TermsUnique(self):

        class Document(document.Document):

            @document.active_property(prefix='P')
            def prop_1(self, value):
                return value

            @document.active_property(prefix='P')
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Document, prop_1='1', prop_2='2')

    def test_IndexedProperty_Multiple(self):

        class Document(document.Document):

            @document.active_property(prefix='A', multiple=True)
            def by_space(self, value):
                return value

            @document.active_property(prefix='b', multiple=True, separator=';')
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
        self.assertEqual(1, total)
        self.assertEqual(
                [(by_space, by_semicolon)],
                [(i.by_space, i.by_semicolon) for i in docs])

        self.assertEqual(1, Document.find(0, 100, request={'by_space': '1'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_space': '2'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_space': '3'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '4'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '5'})[-1])
        self.assertEqual(1, Document.find(0, 100, request={'by_semicolon': '6'})[-1])

    def test_IndexedProperty_FullTextSearch(self):

        class Document(document.Document):

            @document.active_property(full_text=False, slot=1)
            def no(self, value):
                return value

            @document.active_property(full_text=True, slot=2)
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

            @document.active_property(StoredProperty, default='default')
            def w_default(self, value):
                return value

            @document.active_property(StoredProperty)
            def wo_default(self, value):
                return value

            @document.active_property(IndexedProperty, slot=1, default='not_stored_default')
            def not_stored_default(self, value):
                return value

        doc = Document(wo_default='wo_default')
        self.assertEqual('default', doc.metadata['w_default'].default)
        self.assertEqual(None, doc.metadata['wo_default'].default)
        self.assertEqual('not_stored_default', doc.metadata['not_stored_default'].default)

        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
        self.assertEqual(
                [('default', 'wo_default', 'not_stored_default')],
                [(i.w_default, i.wo_default, i.not_stored_default) for i in docs])

        self.assertRaises(RuntimeError, Document)

    def test_StoredProperty_ConstructOnly(self):

        class Document(document.Document):

            @document.active_property(StoredProperty, construct_only=True)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

        doc = Document(prop='foo')
        self.assertEqual(True, doc.metadata['prop'].construct_only)

        doc.prop = 'bar'
        doc.post()

        doc_2 = Document(doc.guid)
        self.assertRaises(RuntimeError, doc_2.__setitem__, 'prop', 'fail')

    def test_properties_Blob(self):

        class Document(document.Document):

            @document.active_property(BlobProperty)
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

    def test_GroupedProperty(self):

        class Document(document.Document):
            pass

        doc = Document()
        assert 'grouped' in doc.metadata
        self.assertRaises(RuntimeError, lambda: Document(grouped='foo'))

        doc = Document('1', indexed_props={'grouped': 'foo'})
        self.assertEqual('foo', doc['grouped'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'grouped', 'bar')
        doc.post()

        self.assertRaises(RuntimeError, lambda: Document(doc.guid)['grouped'])

    def test_AggregatorProperty(self):

        voter = [-1]

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return voter[0]

        class Document(document.Document):

            @document.active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @document.active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        doc = Document()
        self.assertEqual('0', doc['vote'])
        self.assertEqual('0', doc['counter'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', 'foo')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '-1')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '')
        doc['vote'] = '100'
        self.assertEqual('1', doc['vote'])
        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
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
        self.assertEqual(1, total)
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
        self.assertEqual(1, total)
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
        self.assertEqual(1, total)
        self.assertEqual(
                [('0', '0')],
                [(i.vote, i.counter) for i in docs])

    def test_AggregatorProperty_DoNotAggregateOnNoChanches(self):

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return -1

        class Document(document.Document):

            @document.active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

            @document.active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        doc = Document()
        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
        self.assertEqual(
                [('0', '0')],
                [(i.vote, i.counter) for i in docs])

    def test_authorize_Disabled(self):

        class Document(document.Document):

            @document.active_property(StoredProperty, default='nil')
            def prop_1(self, value):
                return value

            @document.active_property(StoredProperty)
            def prop_2(self, value):
                return value

            def authorize(self, prop):
                return prop != 'prop_1'

        self.assertRaises(RuntimeError, Document, prop_1='foo', prop_2='bar')
        doc = Document(prop_2='bar')
        self.assertRaises(RuntimeError, doc.__setitem__, 'prop_1', 'foo')

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

            @document.active_property(slot=1)
            def prop_1(self, value):
                return value

            @prop_1.setter
            def prop_1(self, value):
                return value

            @document.active_property(StoredProperty)
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
        doc_2.prop_1 = '5'
        doc_2.prop_2 = '6'
        doc_2.post()
        self.assertEqual(
                [('5', '6')],
                [(i.prop_1, i.prop_2) for i in Document.find(0, 1024)[0]])

    def test_delete(self):

        class Document(document.Document):

            @document.active_property(prefix='P')
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

            @document.active_property(slot=1)
            def prop(self, value):
                return value

            @document.active_property(CounterProperty, slot=2)
            def counter(self, value):
                return value

            @document.active_property(Vote, counter='counter')
            def vote(self, value):
                return value

        self.touch(
                ('document/1/1/.document', ''),
                ('document/1/1/prop', 'prop-1'),
                ('document/1/1/vote/-1', ''),
                ('document/2/2/.document', ''),
                ('document/2/2/prop', 'prop-2'),
                ('document/2/2/vote/-2', ''),
                ('document/2/2/vote/-3', ''),
                )

        Document.init()
        for i in Document.populate():
            pass

        doc = Document('1')
        self.assertEqual('1', doc['vote'])
        self.assertEqual('1', doc['counter'])
        self.assertEqual('prop-1', doc['prop'])

        doc = Document('2')
        self.assertEqual('0', doc['vote'])
        self.assertEqual('2', doc['counter'])
        self.assertEqual('prop-2', doc['prop'])

    def test_on_create(self):

        class Document(document.Document):

            @document.active_property(slot=1)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

            def on_create(self, properties):
                properties['prop'] = 'foo'

        doc = Document()
        doc.post()
        self.assertEqual(
                ['foo'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_2 = Document(doc.guid)
        doc_2.prop = 'probe'
        doc_2.post()
        self.assertEqual(
                ['probe'],
                [i.prop for i in Document.find(0, 1024)[0]])

        doc_3 = Document()
        doc_3.prop = 'bar'
        doc_3.post()
        self.assertEqual(
                ['probe', 'bar'],
                [i.prop for i in Document.find(0, 1024)[0]])

    def test_on_modify(self):

        class Document(document.Document):

            @document.active_property(slot=1)
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

            @document.active_property(slot=1)
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

            @document.active_property(slot=1, typecast=int)
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

            @document.active_property(slot=1, typecast=bool)
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

            @document.active_property(slot=1, typecast=['foo', 'bar'], default='foo')
            def prop(self, value):
                return value

            @document.active_property(slot=2, typecast=['foo', 'bar'], multiple=True, default='foo')
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


if __name__ == '__main__':
    tests.main()
