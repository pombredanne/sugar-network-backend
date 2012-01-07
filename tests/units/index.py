#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import uuid
import time
from os.path import exists

import gobject

from __init__ import tests

from active_document import index, env
from active_document.metadata import Metadata, IndexedProperty, GuidProperty
from active_document.metadata import CounterProperty


class IndexTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        env.index_pool.value = 10
        Index.docs = []
        self.mainloop = gobject.MainLoop()

    def tearDown(self):
        index.shutdown()
        tests.Test.tearDown(self)

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 0, 'I')
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 0, 'K')
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 1, 'I')
        IndexedProperty('key', 1, 'K')
        IndexedProperty('guid', 0, 'I')

    def test_Create(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': 'value_1'}, True)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'}], 1),
                db.find(reply=['key']))

        db.store('2', {'key': 'value_2'}, True)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'},
                  {'guid': '2', 'key': 'value_2'}], 2),
                db.find(reply=['key']))

    def test_update(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': 'value_1', 'var_2': 'value_2'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_1', 'var_2': 'value_2'}], 1),
                db.find(reply=['var_1', 'var_2']))

        db.store('1', {'var_1': 'value_3'}, False)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_3', 'var_2': 'value_2'}], 1),
                db.find(reply=['var_1', 'var_2']))

    def test_delete(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': 'value'}, True)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'key': 'value'}], 1),
                db.find(reply=['key']))

        db.delete('1')
        self.mainloop.run()
        self.assertEqual(
                ([], 0),
                db.find(reply=['key']))

    def test_find(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', full_text=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db.find(query='у', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}], 1),
                db.find(query='у AND ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 2),
                db.find(query='var_3:ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 3),
                db.find(query='var_3:ю OR var_2:у', reply=['var_1']))

    def test_find_WithProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', full_text=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db.find(request={'var_2': 'у'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='var_1:0', request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}], 1),
                db.find(query='var_3:ю', request={'var_2': 'б'}, reply=['var_1']))

    def test_find_WithAllBooleanProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', boolean=True, full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_WithBooleanProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', boolean=False, full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_ExactQuery(self):
        db = Index({'key': IndexedProperty('key', 1, 'K', full_text=True)})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': 'фу'}, True)
        self.mainloop.run()
        db.store('2', {'key': 'фу бар'}, True)
        self.mainloop.run()
        db.store('3', {'key': 'фу бар тест'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': 'фу'}, {'guid': '2', 'key': 'фу бар'}, {'guid': '3', 'key': 'фу бар тест'}], 3),
                db.find(query='key:фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': 'фу бар'}, {'guid': '3', 'key': 'фу бар тест'}], 2),
                db.find(query='key:"фу бар"', reply=['key']))

        self.assertEqual(
                ([{'guid': '1', 'key': 'фу'}], 1),
                db.find(query='key:=фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': 'фу бар'}], 1),
                db.find(query='key:="фу бар"', reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': 'фу бар тест'}], 1),
                db.find(query='key:="фу бар тест"', reply=['key']))

    def test_find_ExactQueryTerms(self):
        term = 'azAZ09_'

        db = Index({term: IndexedProperty(term, 1, 'T', full_text=True)})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {term: 'test'}, True)
        self.mainloop.run()
        db.store('2', {term: 'test fail'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db.find(query='%s:=test' % term, reply=['guid']))

    def test_find_ReturnPortions(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': '1'}, True)
        self.mainloop.run()
        db.store('2', {'key': '2'}, True)
        self.mainloop.run()
        db.store('3', {'key': '3'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': '1'}], 3),
                db.find(offset=0, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': '2'}], 3),
                db.find(offset=1, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': '3'}], 3),
                db.find(offset=2, limit=1, reply=['key']))
        self.assertEqual(
                ([], 3),
                db.find(offset=3, limit=1, reply=['key']))

    def test_find_OrderBy(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B'),
            'var_3': IndexedProperty('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '5'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': '2', 'var_3': '5'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': '3', 'var_3': '4'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}, {'guid': '3', 'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['var_2']))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}, {'guid': '3', 'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['+var_2']))
        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}, {'guid': '2', 'var_1': '2'}, {'guid': '1', 'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['-var_2']))

        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}, {'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '+var_2']))
        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}, {'guid': '2', 'var_1': '2'}, {'guid': '1', 'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '-var_2']))
        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}, {'guid': '1', 'var_1': '1'}, {'guid': '3', 'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['-var_3', '-var_2']))

    def test_find_GroupBy(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B'),
            'var_3': IndexedProperty('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '3'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': '1', 'var_3': '4'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': '2', 'var_3': '4'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1', 'grouped': 2}, {'guid': '3', 'var_1': '3', 'grouped': 1}], 2),
                db.find(reply=['var_1'], group_by='var_2'))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1', 'grouped': 1}, {'guid': '2', 'var_1': '2', 'grouped': 2}], 2),
                db.find(reply=['var_1'], group_by='var_3'))

    def test_TermsAreLists(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B', multiple=True),
            'var_3': IndexedProperty('var_3', 3, 'C', multiple=True, separator=';'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1 2', 'var_3': '4;5'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': ' 2  3 ', 'var_3': ' 5 ; 6 '}, True)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find(request={'var_2': '1'}, reply=['var_1']))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 2),
                db.find(request={'var_2': '2'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}], 1),
                db.find(request={'var_3': '6'}, reply=['var_1']))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 2),
                db.find(request={'var_3': '5'}, reply=['var_1']))

    def test_FlushThreshold(self):
        env.index_flush_threshold.value = 2
        env.index_flush_timeout.value = 0
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        changed = []
        db.connect('changed', lambda *args: changed.append(True))

        def cb():
            db.store('1', {'key': '1'}, True)
            db.store('2', {'key': '2'}, True)
            db.store('3', {'key': '3'}, True)
            db.store('4', {'key': '4'}, True)
            db.store('5', {'key': '5'}, True)
        gobject.idle_add(cb)

        gobject.timeout_add_seconds(2, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(2, len(changed))
        self.assertEqual(5, db.find()[-1])

    def test_FlushTimeout(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 2
        db = Index({})

        changed = []
        db.connect('changed', lambda *args: changed.append(True))

        def create():
            db.store(str(uuid.uuid1()), {}, True)

        gobject.idle_add(create)
        gobject.timeout_add(1000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(0, len(changed))
        gobject.timeout_add(3000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(1, len(changed))
        gobject.idle_add(create)
        gobject.timeout_add(1000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(1, len(changed))
        gobject.timeout_add(3000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(2, len(changed))
        self.assertEqual(2, db.find()[-1])

    def test_Populate(self):
        Index.docs = [
                {'guid': '1', 'key': 'a'},
                {'guid': '2', 'key': 'b'},
                {'guid': '3', 'key': 'c'},
                ]

        env.index_flush_threshold.value = 3
        env.index_flush_timeout.value = 0
        db = Index({'key': IndexedProperty('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': 'a'},
                  {'guid': '2', 'key': 'b'},
                  {'guid': '3', 'key': 'c'}], 3),
                db.find(reply=['key']))

    def test_LayoutVersion(self):
        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        assert exists('index/version')
        os.utime('index/index', (0, 0))
        index.shutdown()

        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertEqual(0, os.stat('index/index').st_mtime)
        index.shutdown()

        env.LAYOUT_VERSION += 1
        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertNotEqual(0, os.stat('index/index').st_mtime)
        index.shutdown()

    def test_ReadFromWritingDB(self):
        env.index_flush_threshold.value = 10
        env.index_flush_timeout.value = 0
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        changed = []
        db.connect('changed', lambda *args: changed.append(True))

        def cb():
            db.store('1', {'key': '1'}, True)
            db.store('2', {'key': '2'}, True)
            db.store('3', {'key': '3'}, True)
        gobject.idle_add(cb)

        gobject.timeout_add_seconds(2, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': '1'},
                  {'guid': '2', 'key': '2'},
                  {'guid': '3', 'key': '3'}], 3),
                db.find(reply=['key']))
        self.assertEqual(0, len(changed))

    def test_CounterProperty(self):
        db = Index({'counter': CounterProperty('counter', 1)})
        db.connect('changed', lambda *args: self.mainloop.quit())
        db.connect('failed', lambda *args: self.mainloop.quit())

        db.store('1', {'counter': 'foo'}, True)
        self.mainloop.run()
        self.assertEqual(
                ([], 0),
                db.find())

        db.store('1', {'counter': '-1'}, True)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'counter': '-1'}], 1),
                db.find())

        db.store('1', {'counter': '-1'}, False)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'counter': '-2'}], 1),
                db.find())

        db.store('1', {'counter': '4'}, False)
        self.mainloop.run()
        self.assertEqual(
                ([{'guid': '1', 'counter': '2'}], 1),
                db.find())

    def test_Callbacks(self):
        db = Index({})
        db.connect('changed', lambda *args: self.mainloop.quit())

        pre_stored = []
        post_stored = []
        deleted = []

        db.store('1', {}, True,
                lambda *args: pre_stored.append(args),
                lambda *args: post_stored.append(args))
        self.mainloop.run()
        self.assertEqual(1, len(pre_stored))
        self.assertEqual(1, len(post_stored))

        db.store('1', {}, False,
                lambda *args: pre_stored.append(args),
                lambda *args: post_stored.append(args))
        self.mainloop.run()
        self.assertEqual(2, len(pre_stored))
        self.assertEqual(2, len(post_stored))

        db.delete('1', lambda *args: deleted.append(args))
        self.mainloop.run()
        self.assertEqual(1, len(deleted))


class Index(index.Index):

    docs = []

    def __init__(self, props):
        metadata = Metadata()
        metadata.update(props)
        metadata.name = 'index'
        metadata['guid'] = GuidProperty()

        def crawler():
            for i in Index.docs:
                yield i['guid'], i
        metadata.crawler = crawler

        def to_document(guid, props):
            props['guid'] = guid
            return props
        metadata.to_document = to_document

        self._index = index.get_index(metadata)

    def store(self, guid, properties, new, *args):
        self._index.store(guid, properties, new, *args)

    def delete(self, guid, *args):
        self._index.delete(guid, *args)

    def find(self, offset=0, limit=1024, request=None, query=None, reply=None,
            order_by=None, group_by=None):
        return self._index.find(offset, limit, request, query, reply,
                order_by, group_by)

    def connect(self, *args, **kwargs):
        self._index.connect(*args, **kwargs)


if __name__ == '__main__':
    tests.main()
