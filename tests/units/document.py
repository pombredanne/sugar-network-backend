#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from active_document import document, storage, env, index


class DocumentTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.storage = Storage()

    def tearDown(self):
        index.close_indexes()
        tests.Test.tearDown(self)

    def test_properties_Slotted(self):

        class Document(document.Document):

            @document.active_property(slot=1)
            def slotted(self, value):
                return value

            @document.active_property()
            def not_slotted(self, value):
                return value

        doc = Document(slotted='slotted', not_slotted='not_slotted')

        self.assertEqual(
                sorted([
                    ('guid', 0),
                    ('slotted', 1),
                    ('not_slotted', None),
                    ]),
                sorted([(name, i.slot) for name, i in doc.metadata.items()]))

        doc.post()
        docs, total = Document.find(0, 100, order_by=['slotted'], group_by='slotted')
        self.assertEqual(1, total)
        self.assertEqual(
                [('slotted', 'not_slotted')],
                [(i.slotted, i.not_slotted) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, order_by=['not_slotted'])
        self.assertRaises(RuntimeError, Document.find, 0, 100, group_by='not_slotted')

    def test_properties_Terms(self):

        class Document(document.Document):

            @document.active_property(prefix='T')
            def term(self, value):
                return value

            @document.active_property()
            def not_term(self, value):
                return value

        doc = Document(term='term', not_term='not_term')

        self.assertEqual(
                sorted([
                    ('guid', 'I'),
                    ('term', 'T'),
                    ('not_term', None),
                    ]),
                sorted([(name, i.prefix) for name, i in doc.metadata.items()]))

        doc.post()
        docs, total = Document.find(0, 100, request={'term': 'term'})
        self.assertEqual(1, total)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertRaises(RuntimeError, Document.find, 0, 100, request={'not_term': 'not_term'})
        self.assertEqual(0, Document.find(0, 100, query='not_term:not_term')[-1])
        self.assertEqual(1, Document.find(0, 100, query='not_term:=not_term')[-1])

    def test_properties_Defaults(self):

        class Document(document.Document):

            @document.active_property(default='default')
            def w_default(self, value):
                return value

            @document.active_property()
            def wo_default(self, value):
                return value

        doc = Document(wo_default='wo_default')

        self.assertEqual(
                sorted([
                    ('guid', None),
                    ('w_default', 'default'),
                    ('wo_default', None),
                    ]),
                sorted([(name, i.default) for name, i in doc.metadata.items()]))

        doc.post()
        docs, total = Document.find(0, 100)
        self.assertEqual(1, total)
        self.assertEqual(
                [('default', 'wo_default')],
                [(i.w_default, i.wo_default) for i in docs])

        self.assertRaises(RuntimeError, Document)

    def test_properties_Multiple(self):

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

        self.assertEqual(
                sorted([
                    ('guid', False, None),
                    ('by_space', True, None),
                    ('by_semicolon', True, ';'),
                    ]),
                sorted([(name, i.multiple, i.separator) for name, i in doc.metadata.items()]))

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

    def test_properties_FullTextSearch(self):

        class Document(document.Document):

            @document.active_property(full_text=False)
            def no(self, value):
                return value

            @document.active_property(full_text=True, slot=1)
            def yes(self, value):
                return value

        doc = Document(no='foo', yes='bar')

        self.assertEqual(
                sorted([
                    ('guid', False),
                    ('no', False),
                    ('yes', True),
                    ]),
                sorted([(name, i.full_text) for name, i in doc.metadata.items()]))

        doc.post()
        self.assertEqual(0, Document.find(0, 100, query='foo')[-1])
        self.assertEqual(1, Document.find(0, 100, query='bar')[-1])

    def test_properties_Large(self):

        class Document(document.Document):

            @document.active_property(large=True)
            def large(self, value):
                return value

            @large.setter
            def large(self, value):
                return value

        doc = Document(large='probe')

        self.assertEqual(
                sorted([
                    ('guid', False),
                    ('large', True),
                    ]),
                sorted([(name, i.large) for name, i in doc.metadata.items()]))

        doc.post()
        self.assertRaises(RuntimeError, Document.find, 0, 100, reply='large')
        self.assertEqual('probe', Document(doc.guid).large)

        doc2 = Document(doc.guid)
        doc2.large = 'foo'
        doc2.post()
        self.assertEqual('foo', Document(doc2.guid).large)

    def test_properties_Blob(self):

        class Document(document.Document):

            @document.active_property(blob=True)
            def blob(self, value):
                return value

        self.assertRaises(RuntimeError, Document, blob='probe')
        doc = Document()

        self.assertEqual(
                sorted([
                    ('guid', False),
                    ('blob', True),
                    ]),
                sorted([(name, i.blob) for name, i in doc.metadata.items()]))

        doc.post()
        self.assertRaises(RuntimeError, Document.find, 0, 100, reply='blob')
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).blob)
        self.assertRaises(RuntimeError, lambda: Document(doc.guid).__setitem__('blob', 'foo'))

        doc.receive('blob', StringIO('data'))
        stream = StringIO()
        doc.send('blob', stream)
        self.assertEqual('data', stream.getvalue())

    def test_properties_ConstructOnly(self):

        class Document(document.Document):

            @document.active_property(construct_only=True)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value

        doc = Document(prop='foo')

        self.assertEqual(
                sorted([
                    ('guid', False),
                    ('prop', True),
                    ]),
                sorted([(name, i.construct_only) for name, i in doc.metadata.items()]))

        doc.prop = 'bar'
        doc.post()

        doc_2 = Document(doc.guid)
        self.assertRaises(RuntimeError, doc_2.__setitem__, 'prop', 'fail')

    def test_authorize_Disabled(self):

        class Document(document.Document):

            @document.active_property(default='nil')
            def prop_1(self, value):
                return value

            @document.active_property()
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
        self.assertEqual(1, len(Document.find(0, 1024)[0]))

    def test_update(self):

        class Document(document.Document):

            @document.active_property(slot=1)
            def prop_1(self, value):
                return value

            @prop_1.setter
            def prop_1(self, value):
                return value

            @document.active_property()
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

    def get(self, name, guid, props=None):
        self.data.setdefault(name, {})
        self.data[name].setdefault(name, {})
        record = Record()
        record.guid = guid
        record.update(self.data[name].get(guid) or {})
        record.update(props or {})
        return record

    def put(self, name, guid, props):
        self.data.setdefault(name, {})
        self.data[name].setdefault(name, {})
        self.data[name][guid] = props.copy()

    def delete(self, name, guid):
        del self.data[name][guid]

    def walk(self, name):
        self.data.setdefault(name, {})
        for guid, props in self.data[name].items():
            yield guid, props


class Record(dict):

    modified = True

    def set(self, name, value):
        self[name] = value

    def send(self, name, stream):
        stream.write(self[name])

    def receive(self, name, stream):
        self[name] = stream.read()


if __name__ == '__main__':
    tests.main()
