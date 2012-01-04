#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from active_document import document, storage, env, index
from active_document.metadata import StoredProperty, BlobProperty
from active_document.metadata import CounterProperty, IndexedProperty
from active_document.metadata import AggregatorProperty


class DocumentTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        storage.get = lambda *args: Storage()

    def tearDown(self):
        index.close_indexes()
        tests.Test.tearDown(self)

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

        doc.receive('blob', StringIO('data'))
        stream = StringIO()
        doc.send('blob', stream)
        self.assertEqual('data', stream.getvalue())

    def test_GroupedProperty(self):

        class Document(document.Document):
            pass

        doc = Document()
        assert 'grouped' in doc.metadata
        self.assertRaises(RuntimeError, lambda: Document(grouped='foo'))

        doc = Document(indexed_props={'grouped': 'foo'})
        self.assertEqual('foo', doc['grouped'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'grouped', 'bar')
        doc.post()

        self.assertRaises(RuntimeError, lambda: Document(doc.guid)['grouped'])

    def test_CounterProperty(self):

        class Document(document.Document):

            @document.active_property(CounterProperty, slot=1)
            def counter(self, value):
                return value

        self.assertRaises(RuntimeError, lambda: Document(counter='0'))

        doc = Document(indexed_props={'counter': '1'})
        self.assertEqual('1', doc['counter'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'counter', '2')
        doc.post()

        self.assertEqual('1', Document(doc.guid)['counter'])

    def test_AggregatorProperty(self):

        class Vote(AggregatorProperty):

            @property
            def value(self):
                return -1

        class Document(document.Document):

            @document.active_property(Vote)
            def vote(self, value):
                return value

        doc = Document()
        self.assertEqual('0', doc['vote'])
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', 'foo')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '-1')
        self.assertRaises(RuntimeError, doc.__setitem__, 'vote', '')
        doc['vote'] = '100'
        self.assertEqual('1', doc['vote'])
        doc.post()

        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
        self.assertEqual(
                [('1')],
                [(i.vote) for i in docs])

        doc['vote'] = '0'
        self.assertEqual('0', doc['vote'])
        doc.post()

        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
        self.assertEqual(
                [('0')],
                [(i.vote) for i in docs])

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

    def test_post_IncludeNotStored(self):

        class Document(document.Document):

            @document.active_property(IndexedProperty, slot=1)
            def prop(self, value):
                return value

        doc = Document(indexed_props={'prop': 'foo'})
        doc.post()

        self.assertEqual(
                ['foo'],
                [(i.prop) for i in Document.find(0, 1)[0]])

    def test_find_MaxLimit(self):

        class Document(document.Document):
            pass

        Document().post()
        Document().post()
        Document().post()

        env.find_limit.value = 1
        self.assertEqual(1, len(Document.find(0, 1024)[0]))

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

        Document(prop='1').post()
        Document(prop='2').post()
        Document(prop='3').post()

        self.assertEqual(
                ['1', '2', '3'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(Document.find(0, 1, query='prop:=2')[0][0].guid)
        self.assertEqual(
                ['1', '3'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(Document.find(0, 1, query='prop:=3')[0][0].guid)
        self.assertEqual(
                ['1'],
                [i.prop for i in Document.find(0, 1024)[0]])

        Document.delete(Document.find(0, 1, query='prop:=1')[0][0].guid)
        self.assertEqual(
                [],
                [i.prop for i in Document.find(0, 1024)[0]])


class Storage(object):

    def __init__(self):
        self.data = {}
        storage.get = self.get
        storage.put = self.put
        storage.delete = self.delete
        storage.walk = self.walk

    def get(self, guid, props=None):
        record = Record()
        record.guid = guid
        record.update(self.data.get(guid) or {})
        record.update(props or {})
        return record

    def put(self, guid, props):
        self.data[guid] = props.copy()

    def delete(self, guid):
        del self.data[guid]

    def walk(self):
        for guid, props in self.data.items():
            yield guid, props


class Record(dict):

    modified = True

    def set(self, name, value):
        self[name] = value

    def send(self, name, stream):
        stream.write(self[name])

    def receive(self, name, stream):
        self[name] = stream.read()

    def is_aggregated(self, name, value):
        return name in self and value in self[name]

    def aggregate(self, name, value):
        self.setdefault(name, [])
        self[name].append(value)

    def disaggregate(self, name, value):
        self.setdefault(name, [])
        if value in self[name]:
            self[name].remove(value)


if __name__ == '__main__':
    tests.main()
