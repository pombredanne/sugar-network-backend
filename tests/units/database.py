#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
from os.path import exists

import gobject

from __init__ import tests

from active_document import database


LAYOUT_VERSION = database._LAYOUT_VERSION


class Database(database.Database):

    docs = []

    def scan_cb(self):
        if not Database.docs:
            return
        doc = Database.docs.pop(0)
        return doc['guid'], doc


class DatabaseTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        database._LAYOUT_VERSION = LAYOUT_VERSION
        Database.docs = []
        Database.terms = {}
        self.mainloop = gobject.MainLoop()

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, database.Term, 'key', 0, 'I')
        self.assertRaises(RuntimeError, database.Term, 'key', 0, 'K')
        self.assertRaises(RuntimeError, database.Term, 'key', 1, 'I')
        database.Term('key', 1, 'K')
        database.Term('guid', 0, 'I')

    def test_create(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        guid_1 = db.create({'key': 'value_1'})
        assert guid_1
        entries, total = db.find(properties=['guid', 'key'])
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    ]),
                sorted(entries))

        guid_2 = db.create({'key': 'value_2'})
        assert guid_2
        assert guid_1 != guid_2
        entries, total = db.find(properties=['guid', 'key'])
        self.assertEqual(2, total)
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'key': 'value_1'},
                    {'guid': guid_2, 'key': 'value_2'},
                    ]),
                sorted(entries))

    def test_update(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        guid = db.create({'key': 'value_1'})
        self.assertEqual(
                ([{'guid': guid, 'key': 'value_1'}], 1),
                db.find(properties=['guid', 'key']))

        db.update(guid, {'key': 'value_2'})
        self.assertEqual(
                ([{'guid': guid, 'key': 'value_2'}], 1),
                db.find(properties=['guid', 'key']))

    def test_update_AvoidGuidOverwrite(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        guid = db.create({'key': 'value_1'})
        db.update(guid, {'guid': 'fake', 'key': 'value_2'})
        self.assertEqual(
                ([{'guid': guid, 'key': 'value_2'}], 1),
                db.find(properties=['guid', 'key']))

    def test_delete(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        guid = db.create({'key': 'value'})
        self.assertEqual(
                ([{'guid': guid, 'key': 'value'}], 1),
                db.find(properties=['guid', 'key']))

        db.delete(guid)
        self.assertEqual(
                ([], 0),
                db.find(properties=['guid', 'key']))

    def test_find(self):
        Database.terms = {
                'var_1': database.Term('var_1', 1, 'A'),
                'var_2': database.Term('var_2', 2, 'B'),
                'var_3': database.Term('var_3', 3, 'C'),
                }
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(query='у', properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}], 1),
                db.find(query='у AND ю', properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}, {'var_1': '3'}], 2),
                db.find(query='var_3:ю', properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(query='var_3:ю OR var_2:у', properties=['var_1']))

    def test_find_WithProps(self):
        Database.terms = {
                'var_1': database.Term('var_1', 1, 'A'),
                'var_2': database.Term('var_2', 2, 'B'),
                'var_3': database.Term('var_3', 3, 'C'),
                }
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.create({'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.create({'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': 'у'}, properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_2': 'у', 'var_3': 'г'}, properties=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find(query='var_1:0', request={'var_2': 'у', 'var_3': 'г'}, properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '3'}], 1),
                db.find(query='var_3:ю', request={'var_2': 'б'}, properties=['var_1']))

    def test_find_ExactQuery(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'key': 'фу'})
        db.create({'key': 'фу бар'})
        db.create({'key': 'фу бар тест'})

        self.assertEqual(
                ([{'key': 'фу'}, {'key': 'фу бар'}, {'key': 'фу бар тест'}], 3),
                db.find(query='key:фу', properties=['key']))
        self.assertEqual(
                ([{'key': 'фу бар'}, {'key': 'фу бар тест'}], 2),
                db.find(query='key:"фу бар"', properties=['key']))

        self.assertEqual(
                ([{'key': 'фу'}], 1),
                db.find(query='key:=фу', properties=['key']))
        self.assertEqual(
                ([{'key': 'фу бар'}], 1),
                db.find(query='key:="фу бар"', properties=['key']))
        self.assertEqual(
                ([{'key': 'фу бар тест'}], 1),
                db.find(query='key:="фу бар тест"', properties=['key']))

    def test_find_ReturnPortions(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'key': '1'})
        db.create({'key': '2'})
        db.create({'key': '3'})

        self.assertEqual(
                ([{'key': '1'}], 3),
                db.find(offset=0, limit=1, properties=['key']))
        self.assertEqual(
                ([{'key': '2'}], 3),
                db.find(offset=1, limit=1, properties=['key']))
        self.assertEqual(
                ([{'key': '3'}], 3),
                db.find(offset=2, limit=1, properties=['key']))
        self.assertEqual(
                ([], 3),
                db.find(offset=3, limit=1, properties=['key']))

    def test_find_OrderBy(self):
        Database.terms = {
                'var_1': database.Term('var_1', 1, 'A'),
                'var_2': database.Term('var_2', 2, 'B'),
                'var_3': database.Term('var_3', 3, 'C'),
                }
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'var_1': '1', 'var_2': '1', 'var_3': '5'})
        db.create({'var_1': '2', 'var_2': '2', 'var_3': '5'})
        db.create({'var_1': '3', 'var_2': '3', 'var_3': '4'})

        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(properties=['var_1'], order_by=['var_2']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}, {'var_1': '3'}], 3),
                db.find(properties=['var_1'], order_by=['+var_2']))
        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(properties=['var_1'], order_by=['-var_2']))

        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '1'}, {'var_1': '2'}], 3),
                db.find(properties=['var_1'], order_by=['+var_3', '+var_2']))
        self.assertEqual(
                ([{'var_1': '3'}, {'var_1': '2'}, {'var_1': '1'}], 3),
                db.find(properties=['var_1'], order_by=['+var_3', '-var_2']))
        self.assertEqual(
                ([{'var_1': '2'}, {'var_1': '1'}, {'var_1': '3'}], 3),
                db.find(properties=['var_1'], order_by=['-var_3', '-var_2']))

    def test_find_GroupBy(self):
        Database.terms = {
                'var_1': database.Term('var_1', 1, 'A'),
                'var_2': database.Term('var_2', 2, 'B'),
                'var_3': database.Term('var_3', 3, 'C'),
                }
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'var_1': '1', 'var_2': '1', 'var_3': '3'})
        db.create({'var_1': '2', 'var_2': '1', 'var_3': '4'})
        db.create({'var_1': '3', 'var_2': '2', 'var_3': '4'})

        self.assertEqual(
                ([{'var_1': '1', 'grouped': 2}, {'var_1': '3', 'grouped': 1}], 2),
                db.find(properties=['var_1'], group_by='var_2'))
        self.assertEqual(
                ([{'var_1': '1', 'grouped': 1}, {'var_1': '2', 'grouped': 2}], 2),
                db.find(properties=['var_1'], group_by='var_3'))

    def test_TermsAreLists(self):
        Database.terms = {
                'var_1': database.Term('var_1', 1, 'A'),
                'var_2': database.Term('var_2', 2, 'B', is_list=True),
                'var_3': database.Term('var_3', 3, 'C', is_list=True, list_separator=';'),
                }
        db = Database(flush_timeout=0, flush_threshold=0)

        db.create({'var_1': '1', 'var_2': '1 2', 'var_3': '4;5'})
        db.create({'var_1': '2', 'var_2': ' 2  3 ', 'var_3': ' 5 ; 6 '})

        self.assertEqual(
                ([{'var_1': '1'}], 1),
                db.find(request={'var_2': '1'}, properties=['var_1']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_2': '2'}, properties=['var_1']))

        self.assertEqual(
                ([{'var_1': '2'}], 1),
                db.find(request={'var_3': '6'}, properties=['var_1']))
        self.assertEqual(
                ([{'var_1': '1'}, {'var_1': '2'}], 2),
                db.find(request={'var_3': '5'}, properties=['var_1']))

    def test_FlushThreshold(self):
        Database.terms = {'key': database.Term('key', 1, 'K')}

        db = Database(flush_timeout=0, flush_threshold=2)

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
        self.assertEqual(5, db.find()[-1])

    def test_FlushTimeout(self):
        db = Database(flush_timeout=2, flush_threshold=0)

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
        Database.terms = {'key': database.Term('key', 1, 'K')}

        db = Database(flush_timeout=0, flush_threshold=3)
        db.connect('changed', lambda *args: self.mainloop.quit())
        self.mainloop.run()

        self.assertEqual(
                ([{'guid': '1', 'key': 'a'},
                  {'guid': '2', 'key': 'b'},
                  {'guid': '3', 'key': 'c'},
                  ], 3),
                db.find(properties=['guid', 'key']))

    def test_LayoutVersion(self):
        db = Database(flush_timeout=0, flush_threshold=0)
        assert exists('Database/version')
        os.utime('Database/index', (0, 0))
        db.close()

        db = Database(flush_timeout=0, flush_threshold=0)
        self.assertEqual(0, os.stat('Database/index').st_mtime)
        db.close()

        database._LAYOUT_VERSION += 1
        db = Database(flush_timeout=0, flush_threshold=0)
        self.assertNotEqual(0, os.stat('Database/index').st_mtime)
        db.close()


if __name__ == '__main__':
    tests.main()
