#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import time
from os.path import exists

import gobject

from __init__ import tests

from active_document import database, env, database_writer
from active_document.properties import Property


class Database(database.Database):

    docs = []

    def __init__(self, properties):
        Database._writer = None
        database.Database.__init__(self, properties, self.crawler)

    def crawler(self):
        for i in Database.docs:
            yield i['guid'], i


class DatabaseTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        env.threading.value = True
        Database.docs = []
        self.mainloop = gobject.MainLoop()

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, Property, 'key', 0, 'I')
        self.assertRaises(RuntimeError, Property, 'key', 0, 'K')
        self.assertRaises(RuntimeError, Property, 'key', 1, 'I')
        Property('key', 1, 'K')
        Property('guid', 0, 'I')

    def test_create(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid_1 = db.create({'key': 'value_1'})
        self.mainloop.run()

        assert guid_1
        entries, total = db.find(reply=['guid', 'key'])
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    ]),
                sorted(entries))

        guid_2 = db.create({'key': 'value_2'})
        self.mainloop.run()

        assert guid_2
        assert guid_1 != guid_2
        entries, total = db.find(reply=['guid', 'key'])
        self.assertEqual(2, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    {'guid': guid_2, 'key': 'value_2'},
                    ]),
                sorted(entries))

    def test_update(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = db.create({'key': 'value_1'})
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': guid, 'key': 'value_1'}], 1),
                db.find(reply=['guid', 'key']))

        db.update(guid, {'key': 'value_2'})
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': guid, 'key': 'value_2'}], 1),
                db.find(reply=['guid', 'key']))

    def test_update_AvoidGuidOverwrite(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = db.create({'key': 'value_1'})
        self.mainloop.run()

        db.update(guid, {'guid': 'fake', 'key': 'value_2'})
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': guid, 'key': 'value_2'}], 1),
                db.find(reply=['guid', 'key']))

    def test_delete(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        guid = db.create({'key': 'value'})
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': guid, 'key': 'value'}], 1),
                db.find(reply=['guid', 'key']))

        db.delete(guid)
        self.mainloop.run()

        self.assertEqual(
                ([], 0),
                db.find(reply=['guid', 'key']))

    def test_find(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(query='у', reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}], 1),
                db.find(query='у AND ю', reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}, {'var_1': '3'}], 2),
                db.find(query='var_3:ю', reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(query='var_3:ю OR var_2:у', reply=['var_1']))

    def test_find_WithProps(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': 'у'}, reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='var_1:0', request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '3'}], 1),
                db.find(query='var_3:ю', request={'var_2': 'б'}, reply=['var_1']))

    def test_find_WithAllBooleanProps(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A', boolean=True),
            'var_2': Property('var_2', 2, 'B', boolean=True),
            'var_3': Property('var_3', 3, 'C', boolean=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_WithBooleanProps(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A', boolean=True),
            'var_2': Property('var_2', 2, 'B', boolean=False),
            'var_3': Property('var_3', 3, 'C', boolean=True),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_ExactQuery(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'key': 'фу'})
        self.mainloop.run()
        db.create({'key': 'фу бар'})
        self.mainloop.run()
        db.create({'key': 'фу бар тест'})
        self.mainloop.run()

        self.assertEqual(
                ([{'key': 'фу'}, {'key': 'фу бар'}, {'key': 'фу бар тест'}], 3),
                db.find(query='key:фу', reply=['key']))
        self.assertEqual(
                ([{'key': 'фу бар'}, {'key': 'фу бар тест'}], 2),
                db.find(query='key:"фу бар"', reply=['key']))

        self.assertEqual(
                ([{'key': 'фу'}], 1),
                db.find(query='key:=фу', reply=['key']))
        self.assertEqual(
                ([{'key': 'фу бар'}], 1),
                db.find(query='key:="фу бар"', reply=['key']))
        self.assertEqual(
                ([{'key': 'фу бар тест'}], 1),
                db.find(query='key:="фу бар тест"', reply=['key']))

    def test_find_ReturnPortions(self):
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'key': '1'})
        self.mainloop.run()
        db.create({'key': '2'})
        self.mainloop.run()
        db.create({'key': '3'})
        self.mainloop.run()

        self.assertEqual(
                ([{'key': '1'}], 3),
                db.find(offset=0, limit=1, reply=['key']))
        self.assertEqual(
                ([{'key': '2'}], 3),
                db.find(offset=1, limit=1, reply=['key']))
        self.assertEqual(
                ([{'key': '3'}], 3),
                db.find(offset=2, limit=1, reply=['key']))
        self.assertEqual(
                ([], 3),
                db.find(offset=3, limit=1, reply=['key']))

    def test_find_OrderBy(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': '1', 'var_3': '5'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': '2', 'var_3': '5'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': '3', 'var_3': '4'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['var_2']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['+var_2']))
        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['-var_2']))

        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '1'}, {'var_1': '2'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '+var_2']))
        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(reply=['var_1'], order_by=['+var_3', '-var_2']))
        self.assertEqual(
                ([{'var_1': '2'}, {'var_1': '1'}, {'var_1': '3'}], 3),
                db.find(reply=['var_1'], order_by=['-var_3', '-var_2']))

    def test_find_GroupBy(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': '1', 'var_3': '3'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': '1', 'var_3': '4'})
        self.mainloop.run()
        db.create({'var_1': '3', 'var_2': '2', 'var_3': '4'})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1', 'grouped': 2}, {'var_1': '3', 'grouped': 1}], 2),
                db.find(reply=['var_1'], group_by='var_2'))
        self.assertEqual(
                ([{'var_1': '1', 'grouped': 1}, {'var_1': '2', 'grouped': 2}], 2),
                db.find(reply=['var_1'], group_by='var_3'))

    def test_TermsAreLists(self):
        db = Database({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B', multiple=True),
            'var_3': Property('var_3', 3, 'C', multiple=True, separator=';'),
            })
        db.connect('changed', lambda *args: self.mainloop.quit())

        db.create({'var_1': '1', 'var_2': '1 2', 'var_3': '4;5'})
        self.mainloop.run()
        db.create({'var_1': '2', 'var_2': ' 2  3 ', 'var_3': ' 5 ; 6 '})
        self.mainloop.run()

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_2': '1'}, reply=['var_1']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': '2'}, reply=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}], 1),
                db.find(request={'var_3': '6'}, reply=['var_1']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_3': '5'}, reply=['var_1']))

    def test_FlushThreshold(self):
        env.flush_threshold.value = 2
        env.flush_timeout.value = 0
        db = Database({'key': Property('key', 1, 'K')})

        changed = []
        db.connect('changed', lambda *args: changed.append(True))

        def cb():
            db.create({'key': '1'})
            db.create({'key': '2'})
            db.create({'key': '3'})
            db.create({'key': '4'})
            db.create({'key': '5'})
        gobject.idle_add(cb)

        gobject.timeout_add_seconds(2, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(2, len(changed))
        self.assertEqual(4, db.find()[-1])

    def test_FlushTimeout(self):
        env.flush_threshold.value = 0
        env.flush_timeout.value = 2
        db = Database({})

        changed = []
        db.connect('changed', lambda *args: changed.append(True))

        def create():
            db.create({})

        gobject.idle_add(create)
        gobject.timeout_add(1000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(0, len(changed))
        gobject.timeout_add(2000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(1, len(changed))
        gobject.idle_add(create)
        gobject.timeout_add(1000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(1, len(changed))
        gobject.timeout_add(2000, self.mainloop.quit)
        self.mainloop.run()

        self.assertEqual(2, len(changed))
        self.assertEqual(2, db.find()[-1])

    def test_Populate(self):
        Database.docs = [
                {'guid': '1', 'key': 'a'},
                {'guid': '2', 'key': 'b'},
                {'guid': '3', 'key': 'c'},
                ]

        env.flush_threshold.value = 3
        env.flush_timeout.value = 0
        db = Database({'key': Property('key', 1, 'K')})
        db.connect('changed', lambda *args: self.mainloop.quit())
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': 'a'},
                  {'guid': '2', 'key': 'b'},
                  {'guid': '3', 'key': 'c'},
                  ], 3),
                db.find(reply=['guid', 'key']))

    def test_LayoutVersion(self):
        db = Database({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        assert exists('Database/version')
        os.utime('Database/index', (0, 0))
        database_writer.shutdown()

        db = Database({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertEqual(0, os.stat('Database/index').st_mtime)
        database_writer.shutdown()

        env.LAYOUT_VERSION += 1
        db = Database({})
        db.connect('openned', lambda *args: self.mainloop.quit())
        self.mainloop.run()
        self.assertNotEqual(0, os.stat('Database/index').st_mtime)
        database_writer.shutdown()


if __name__ == '__main__':
    tests.main()
