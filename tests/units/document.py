#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

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

    def test_find_MaxLimit(self):

        class Document(document.Document):
            pass

        Document().post()
        Document().post()
        Document().post()

        env.find_limit.value = 1
        self.assertEqual(1, len(Document.find(0, 1024)[0]))


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


if __name__ == '__main__':
    tests.main()
