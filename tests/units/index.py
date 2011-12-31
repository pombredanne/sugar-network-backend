#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import uuid
import time
from os.path import exists

import gobject

from __init__ import tests

from active_document import index, env, index_db
from active_document.properties import Property


class Index(index.Index):

    docs = []

    def __init__(self, properties):
        Index._writer = None
        index.Index.__init__(self, 'index', properties, self.crawler)

    def crawler(self):
        for i in Index.docs:
            yield i['guid'], i


class IndexTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        env.index_pool.value = 10
        Index.docs = []
        self.mainloop = gobject.MainLoop()

    def tearDown(self):
        index_db.shutdown()
        tests.Test.tearDown(self)

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, Property, 'key', 0, 'I')
        self.assertRaises(RuntimeError, Property, 'key', 0, 'K')
        self.assertRaises(RuntimeError, Property, 'key', 1, 'I')
        Property('key', 1, 'K')
        Property('guid', 0, 'I')

    def test_Create(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid_1 = '1'
        db.store(guid_1, {'key': 'value_1'}, True)
        self.mainloop.run()

        __, entries, total = db.find(reply=['guid', 'key'])
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    ]),
                sorted(entries))

        guid_2 = '2'
        db.store(guid_2, {'key': 'value_2'}, True)
        self.mainloop.run()

        __, entries, total = db.find(reply=['guid', 'key'])
        self.assertEqual(2, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    {'guid': guid_2, 'key': 'value_2'},
                    ]),
                sorted(entries))

    def test_update(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = '1'
        db.store(guid, {'var_1': 'value_1', 'var_2': 'value_2'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([guid], [{'var_1': 'value_1', 'var_2': 'value_2'}], 1),
                db.find(reply=['var_1', 'var_2']))

        db.store(guid, {'var_1': 'value_3'}, False)
        self.mainloop.run()

        self.assertEqual(
                ([guid], [{'var_1': 'value_3', 'var_2': 'value_2'}], 1),
                db.find(reply=['var_1', 'var_2']))

    def test_update_AvoidGuidOverwrite(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = '1'
        db.store(guid, {'key': 'value_1'}, True)
        self.mainloop.run()

        self.assertRaises(RuntimeError, db.store, guid, {'guid': 'fake', 'key': 'value_2'}, False)

    def test_delete(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = '1'
        db.store(guid, {'key': 'value'}, True)
        self.mainloop.run()

        self.assertEqual(
                ([guid], [{'key': 'value'}], 1),
                db.find(reply=['key']))

        db.delete(guid)
        self.mainloop.run()

        self.assertEqual(
                ([], [], 0),
                db.find(reply=['key']))

    def test_find(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1', '2'], [{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(query='у', reply=['var_1']))

        self.assertEqual(
                (['2'], [{'var_1': '2'}], 1),
                db.find(query='у AND ю', reply=['var_1']))

        self.assertEqual(
                (['2', '3'], [{'var_1': '2'}, {'var_1': '3'}], 2),
                db.find(query='var_3:ю', reply=['var_1']))

        self.assertEqual(
                (['1', '2', '3'], [{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(query='var_3:ю OR var_2:у', reply=['var_1']))

    def test_find_MaxLimit(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': '1'}, True)
        self.mainloop.run()
        db.store('2', {'key': '2'}, True)
        self.mainloop.run()
        db.store('3', {'key': '3'}, True)
        self.mainloop.run()

        env.find_limit.value = 1

        self.assertEqual(
                (['1'], [{'key': '1'}], 3),
                db.find(reply=['key'], limit=None))

        self.assertEqual(
                (['2'], [{'key': '2'}], 3),
                db.find(reply=['key'], offset=1, limit=1024))

    def test_find_WithProps(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1', '2'], [{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': 'у'}, reply=['var_1']))

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], [], 0),
                db.find(query='var_1:0', request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                (['3'], [{'var_1': '3'}], 1),
                db.find(query='var_3:ю', request={'var_2': 'б'}, reply=['var_1']))

    def test_find_WithAllBooleanProps(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A', boolean=True),
            'var_2': Property('var_2', 2, 'B', boolean=True),
            'var_3': Property('var_3', 3, 'C', boolean=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], [], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_WithBooleanProps(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A', boolean=True),
            'var_2': Property('var_2', 2, 'B', boolean=False),
            'var_3': Property('var_3', 3, 'C', boolean=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], [], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_ExactQuery(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': 'фу'}, True)
        self.mainloop.run()
        db.store('2', {'key': 'фу бар'}, True)
        self.mainloop.run()
        db.store('3', {'key': 'фу бар тест'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1', '2', '3'], [{'key': 'фу'}, {'key': 'фу бар'}, {'key': 'фу бар тест'}], 3),
                db.find(query='key:фу', reply=['key']))
        self.assertEqual(
                (['2', '3'], [{'key': 'фу бар'}, {'key': 'фу бар тест'}], 2),
                db.find(query='key:"фу бар"', reply=['key']))

        self.assertEqual(
                (['1'], [{'key': 'фу'}], 1),
                db.find(query='key:=фу', reply=['key']))
        self.assertEqual(
                (['2'], [{'key': 'фу бар'}], 1),
                db.find(query='key:="фу бар"', reply=['key']))
        self.assertEqual(
                (['3'], [{'key': 'фу бар тест'}], 1),
                db.find(query='key:="фу бар тест"', reply=['key']))

    def test_find_ReturnPortions(self):
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'key': '1'}, True)
        self.mainloop.run()
        db.store('2', {'key': '2'}, True)
        self.mainloop.run()
        db.store('3', {'key': '3'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1'], [{'key': '1'}], 3),
                db.find(offset=0, limit=1, reply=['key']))
        self.assertEqual(
                (['2'], [{'key': '2'}], 3),
                db.find(offset=1, limit=1, reply=['key']))
        self.assertEqual(
                (['3'], [{'key': '3'}], 3),
                db.find(offset=2, limit=1, reply=['key']))
        self.assertEqual(
                ([], [], 3),
                db.find(offset=3, limit=1, reply=['key']))

    def test_find_OrderBy(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '5'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': '2', 'var_3': '5'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': '3', 'var_3': '4'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1', '2', '3'], [{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['var_2']))
        self.assertEqual(
                (['1', '2', '3'], [{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['+var_2']))
        self.assertEqual(
                (['3', '2', '1'], [{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['-var_2']))

        self.assertEqual(
                (['3', '1', '2'], [{'var_1': '3'}, {'var_1': '1'}, {'var_1': '2'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '+var_2']))
        self.assertEqual(
                (['3', '2', '1'], [{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '-var_2']))
        self.assertEqual(
                (['2', '1', '3'], [{'var_1': '2'}, {'var_1': '1'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['-var_3', '-var_2']))

    def test_find_GroupBy(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '3'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': '1', 'var_3': '4'}, True)
        self.mainloop.run()
        db.store('3', {'var_1': '3', 'var_2': '2', 'var_3': '4'}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1', '3'], [{'var_1': '1', 'grouped': 2}, {'var_1': '3', 'grouped': 1}], 2),
                db.find(reply=['var_1'], group_by='var_2'))
        self.assertEqual(
                (['1', '2'], [{'var_1': '1', 'grouped': 1}, {'var_1': '2', 'grouped': 2}], 2),
                db.find(reply=['var_1'], group_by='var_3'))

    def test_TermsAreLists(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B', multiple=True),
            'var_3': Property('var_3', 3, 'C', multiple=True, separator=';'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.store('1', {'var_1': '1', 'var_2': '1 2', 'var_3': '4;5'}, True)
        self.mainloop.run()
        db.store('2', {'var_1': '2', 'var_2': ' 2  3 ', 'var_3': ' 5 ; 6 '}, True)
        self.mainloop.run()

        self.assertEqual(
                (['1'], [{'var_1': '1'}], 1),
                db.find(request={'var_2': '1'}, reply=['var_1']))
        self.assertEqual(
                (['1', '2'], [{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': '2'}, reply=['var_1']))

        self.assertEqual(
                (['2'], [{'var_1': '2'}], 1),
                db.find(request={'var_3': '6'}, reply=['var_1']))
        self.assertEqual(
                (['1', '2'], [{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_3': '5'}, reply=['var_1']))

    def test_FlushThreshold(self):
        env.index_flush_threshold.value = 2
        env.index_flush_timeout.value = 0
        db = Index({'key': Property('key', 1, 'K')})

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
            db.store(uuid.uuid1(), {}, True)

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
        db = Index({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())
        self.mainloop.run()

        self.assertEqual(
                (['1', '2', '3'], [
                    {'guid': '1', 'key': 'a'},
                    {'guid': '2', 'key': 'b'},
                    {'guid': '3', 'key': 'c'},
                    ], 3),
                db.find(reply=['guid', 'key']))

    def test_LayoutVersion(self):
        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        assert exists('index/version')
        os.utime('index/index', (0, 0))
        index_db.shutdown()

        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertEqual(0, os.stat('index/index').st_mtime)
        index_db.shutdown()

        env.LAYOUT_VERSION += 1
        db = Index({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertNotEqual(0, os.stat('index/index').st_mtime)
        index_db.shutdown()

    def test_ReadFromWritingDB(self):
        env.index_flush_threshold.value = 10
        env.index_flush_timeout.value = 0
        db = Index({'key': Property('key', 1, 'K')})

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
                (['1', '2', '3'], [
                    {'key': '1'},
                    {'key': '2'},
                    {'key': '3'},
                    ], 3),
                db.find(reply=['key']))
        self.assertEqual(0, len(changed))


if __name__ == '__main__':
    tests.main()
