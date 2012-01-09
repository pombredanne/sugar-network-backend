#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import uuid
import time
from os.path import exists

from __init__ import tests

from active_document import index, env
from active_document.metadata import Metadata, IndexedProperty, GuidProperty
from active_document.metadata import CounterProperty


class IndexTest(tests.Test):

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 0, 'I')
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 0, 'K')
        self.assertRaises(RuntimeError, IndexedProperty, 'key', 1, 'I')
        IndexedProperty('key', 1, 'K')
        IndexedProperty('guid', 0, 'I')

    def test_Create(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        db.store('1', {'key': 'value_1'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'}], 1),
                db.find2(0, 10, reply=['key']))

        db.store('2', {'key': 'value_2'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'},
                  {'guid': '2', 'key': 'value_2'}], 2),
                db.find2(0, 10, reply=['key']))

    def test_update(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B'),
            })

        db.store('1', {'var_1': 'value_1', 'var_2': 'value_2'}, True)
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_1', 'var_2': 'value_2'}], 1),
                db.find2(0, 10, reply=['var_1', 'var_2']))

        db.store('1', {'var_1': 'value_3'}, False)
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_3', 'var_2': 'value_2'}], 1),
                db.find2(0, 10, reply=['var_1', 'var_2']))

    def test_delete(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        db.store('1', {'key': 'value'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value'}], 1),
                db.find2(0, 10, reply=['key']))

        db.delete('1')
        self.assertEqual(
                ([], 0),
                db.find2(0, 10, reply=['key']))

    def test_find(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db.find2(0, 10, query='у', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}], 1),
                db.find2(0, 10, query='у AND ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 2),
                db.find2(0, 10, query='var_3:ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 3),
                db.find2(0, 10, query='var_3:ю OR var_2:у', reply=['var_1'], order_by='guid'))

    def test_find_WithProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db.find2(0, 10, request={'var_2': 'у'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find2(0, 10, query='var_1:0', request={'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}], 1),
                db.find2(0, 10, query='var_3:ю', request={'var_2': 'б'}, reply=['var_1']))

    def test_find_WithAllBooleanProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', boolean=True, full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find2(0, 10, query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_WithBooleanProps(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': IndexedProperty('var_2', 2, 'B', boolean=False, full_text=True),
            'var_3': IndexedProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, query='г', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db.find2(0, 10, query='б', request={'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, reply=['var_1']))

    def test_find_ExactQuery(self):
        db = Index({'key': IndexedProperty('key', 1, 'K', full_text=True)})

        db.store('1', {'key': 'фу'}, True)
        db.store('2', {'key': 'фу бар'}, True)
        db.store('3', {'key': 'фу бар тест'}, True)

        self.assertEqual(
                ([{'guid': '1', 'key': 'фу'}, {'guid': '2', 'key': 'фу бар'}, {'guid': '3', 'key': 'фу бар тест'}], 3),
                db.find2(0, 10, query='key:фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': 'фу бар'}, {'guid': '3', 'key': 'фу бар тест'}], 2),
                db.find2(0, 10, query='key:"фу бар"', reply=['key']))

        self.assertEqual(
                ([{'guid': '1', 'key': 'фу'}], 1),
                db.find2(0, 10, query='key:=фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': 'фу бар'}], 1),
                db.find2(0, 10, query='key:="фу бар"', reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': 'фу бар тест'}], 1),
                db.find2(0, 10, query='key:="фу бар тест"', reply=['key']))

    def test_find_ExactQueryTerms(self):
        term = 'azAZ09_'

        db = Index({term: IndexedProperty(term, 1, 'T', full_text=True)})

        db.store('1', {term: 'test'}, True)
        db.store('2', {term: 'test fail'}, True)

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db.find2(0, 10, query='%s:=test' % term, reply=['guid']))

    def test_find_ReturnPortions(self):
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        db.store('1', {'key': '1'}, True)
        db.store('2', {'key': '2'}, True)
        db.store('3', {'key': '3'}, True)

        self.assertEqual(
                ([{'guid': '1', 'key': '1'}], 3),
                db.find2(offset=0, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': '2'}], 3),
                db.find2(offset=1, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': '3'}], 3),
                db.find2(offset=2, limit=1, reply=['key']))
        self.assertEqual(
                ([], 3),
                db.find2(offset=3, limit=1, reply=['key']))

    def test_find_OrderBy(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B'),
            'var_3': IndexedProperty('var_3', 3, 'C'),
            })

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '5'}, True)
        db.store('2', {'var_1': '2', 'var_2': '2', 'var_3': '5'}, True)
        db.store('3', {'var_1': '3', 'var_2': '3', 'var_3': '4'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}, {'guid': '3', 'var_1': '3'}], 3),
                db.find2(0, 10, reply=['var_1'], order_by='var_2'))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}, {'guid': '3', 'var_1': '3'}], 3),
                db.find2(0, 10, reply=['var_1'], order_by='+var_2'))
        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}, {'guid': '2', 'var_1': '2'}, {'guid': '1', 'var_1': '1'}], 3),
                db.find2(0, 10, reply=['var_1'], order_by='-var_2'))

    def test_TermsAreLists(self):
        db = Index({
            'var_1': IndexedProperty('var_1', 1, 'A'),
            'var_2': IndexedProperty('var_2', 2, 'B', multiple=True),
            'var_3': IndexedProperty('var_3', 3, 'C', multiple=True, separator=';'),
            })

        db.store('1', {'var_1': '1', 'var_2': '1 2', 'var_3': '4;5'}, True)
        db.store('2', {'var_1': '2', 'var_2': ' 2  3 ', 'var_3': ' 5 ; 6 '}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db.find2(0, 10, request={'var_2': '1'}, reply=['var_1']))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 2),
                db.find2(0, 10, request={'var_2': '2'}, reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}], 1),
                db.find2(0, 10, request={'var_3': '6'}, reply=['var_1']))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 2),
                db.find2(0, 10, request={'var_3': '5'}, reply=['var_1']))

    def test_FlushThreshold(self):
        env.index_flush_threshold.value = 2
        env.index_flush_timeout.value = 0
        db = Index({'key': IndexedProperty('key', 1, 'K')})

        db.store('1', {'key': '1'}, True)
        self.assertEqual(0, db.committed)

        db.store('2', {'key': '2'}, True)
        self.assertEqual(1, db.committed)

        db.store('3', {'key': '3'}, True)
        self.assertEqual(1, db.committed)

        db.store('4', {'key': '4'}, True)
        self.assertEqual(2, db.committed)

        db.store('5', {'key': '5'}, True)
        self.assertEqual(2, db.committed)

        self.assertEqual(5, db.find2(0, 10)[-1])

    def test_LayoutVersion(self):
        db = Index({})
        assert exists('index/version')
        os.utime('index/version', (0, 0))
        db.close()

        db = Index({})
        self.assertEqual(0, os.stat('index/version').st_mtime)
        db.close()

        env.LAYOUT_VERSION += 1
        db = Index({})
        self.assertNotEqual(0, os.stat('index/version').st_mtime)
        db.close()

    def test_CounterProperty(self):
        db = Index({'counter': CounterProperty('counter', 1)})

        self.assertRaises(RuntimeError, db.store, '1', {'counter': 'foo'}, True)
        self.assertEqual(
                ([], 0),
                db.find2(0, 10))

        db.store('1', {'counter': '-1'}, True)
        self.assertEqual(
                ([{'guid': '1', 'counter': '-1'}], 1),
                db.find2(0, 10))

        db.store('1', {'counter': '-1'}, False)
        self.assertEqual(
                ([{'guid': '1', 'counter': '-2'}], 1),
                db.find2(0, 10))

        db.store('1', {'counter': '4'}, False)
        self.assertEqual(
                ([{'guid': '1', 'counter': '2'}], 1),
                db.find2(0, 10))

    def test_Callbacks(self):
        db = Index({})

        pre_stored = []
        post_stored = []
        deleted = []

        db.store('1', {}, True,
                lambda *args: pre_stored.append(args),
                lambda *args: post_stored.append(args))
        self.assertEqual(1, len(pre_stored))
        self.assertEqual(1, len(post_stored))

        db.store('1', {}, False,
                lambda *args: pre_stored.append(args),
                lambda *args: post_stored.append(args))
        self.assertEqual(2, len(pre_stored))
        self.assertEqual(2, len(post_stored))

        db.delete('1', lambda *args: deleted.append(args))
        self.assertEqual(1, len(deleted))

    def test_mtime(self):
        db = Index({})
        self.assertEqual(0, db.mtime)
        db.close()

        db = Index({})
        db.store('1', {}, True)
        self.assertNotEqual(0, db.mtime)
        mtime = db.mtime
        db.close()

        time.sleep(1)

        db = Index({})
        self.assertEqual(mtime, db.mtime)
        db.close()

        db = Index({})
        db.store('2', {}, True)
        self.assertNotEqual(mtime, db.mtime)
        db.close()


class Index(index.IndexWriter):

    def __init__(self, props):
        metadata = Metadata()
        metadata.update(props)
        metadata.name = 'index'
        metadata['guid'] = GuidProperty()

        index.IndexWriter.__init__(self, metadata)
        self.committed = 0

    def commit(self):
        index.IndexWriter.commit(self)
        self.committed += 1

    def find2(self, offset, limit, request=None, query=None, reply=None,
            order_by=None):
        documents, total = index.IndexWriter.find(self,offset, limit, request,
                query, reply, order_by)
        result = []
        for guid, props in documents:
            props['guid'] = guid
            result.append(props)
        return result, total


if __name__ == '__main__':
    tests.main()
