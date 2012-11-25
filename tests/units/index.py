#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import uuid
import time
import shutil
import locale
from os.path import exists

from __init__ import tests

from active_document import index, env
from active_document.metadata import Metadata, ActiveProperty
from active_toolkit import coroutine


class IndexTest(tests.Test):

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, ActiveProperty, 'key', 0, 'I')
        self.assertRaises(RuntimeError, ActiveProperty, 'key', 0, 'K')
        self.assertRaises(RuntimeError, ActiveProperty, 'key', 1, 'I')
        ActiveProperty('key', 1, 'K')
        ActiveProperty('guid', 0, 'I')

    def test_Create(self):
        db = Index({'key': ActiveProperty('key', 1, 'K')})

        db.store('1', {'key': 'value_1'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'}], 1),
                db._find(reply=['key']))

        db.store('2', {'key': 'value_2'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'},
                  {'guid': '2', 'key': 'value_2'}], 2),
                db._find(reply=['key']))

    def test_update(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A'),
            'var_2': ActiveProperty('var_2', 2, 'B'),
            })

        db.store('1', {'var_1': 'value_1', 'var_2': 'value_2'}, True)
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_1', 'var_2': 'value_2'}], 1),
                db._find(reply=['var_1', 'var_2']))

        db.store('1', {'var_1': 'value_3', 'var_2': 'value_4'}, False)
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_3', 'var_2': 'value_4'}], 1),
                db._find(reply=['var_1', 'var_2']))

    def test_delete(self):
        db = Index({'key': ActiveProperty('key', 1, 'K')})

        db.store('1', {'key': 'value'}, True)
        self.assertEqual(
                ([{'guid': '1', 'key': 'value'}], 1),
                db._find(reply=['key']))

        db.delete('1')
        self.assertEqual(
                ([], 0),
                db._find(reply=['key']))

    def test_IndexByReprcast(self):
        db = Index({'key': ActiveProperty('key', 1, 'K', reprcast=lambda x: "foo" + x)})

        db.store('1', {'key': 'bar'}, True)

        self.assertEqual(
                [{'guid': '1', 'key': 'foobar'}],
                db._find(reply=['key'])[0])
        self.assertEqual(
                [{'guid': '1', 'key': 'foobar'}],
                db._find(key='bar', reply=['key'])[0])
        self.assertEqual(
                [],
                db._find(key='foobar', reply=['key'])[0])
        self.assertEqual(
                [],
                db._find(key='fake', reply=['key'])[0])

    def test_find(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A', full_text=True),
            'var_2': ActiveProperty('var_2', 2, 'B', full_text=True),
            'var_3': ActiveProperty('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db._find(query='у', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'}], 1),
                db._find(query='у AND ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 2),
                db._find(query='var_3:ю', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'},
                  {'guid': '3', 'var_1': '3'}], 3),
                db._find(query='var_3:ю OR var_2:у', reply=['var_1'], order_by='guid'))

    def test_find_NoneFilters(self):
        db = Index({
            'prop': ActiveProperty('prop', 1, 'P', full_text=True),
            })

        db.store('guid', {'prop': 'value'}, True)

        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(reply=['prop'])[0])
        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(prop=None, reply=['prop'])[0])
        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(guid=None, reply=['prop'])[0])

    def test_find_WithTypeCast(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A', typecast=bool),
            })

        db.store('1', {'var_1': True}, True)
        db.store('2', {'var_1': False}, True)

        self.assertEqual(
                [{'guid': '1'}],
                db._find(var_1=True, reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '2'}],
                db._find(var_1=False, reply=['guid'])[0])

    def test_find_WithProps(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A', full_text=True),
            'var_2': ActiveProperty('var_2', 2, 'B', full_text=True),
            'var_3': ActiveProperty('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'},
                  {'guid': '2', 'var_1': '2'}], 2),
                db._find(var_2='у', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db._find(var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db._find(query='var_1:0', var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '3', 'var_1': '3'}], 1),
                db._find(query='var_3:ю', var_2='б', reply=['var_1']))

    def test_find_WithAllBooleanProps(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': ActiveProperty('var_2', 2, 'B', boolean=True, full_text=True),
            'var_3': ActiveProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db._find(var_1='1', var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db._find(query='г', var_1='1', var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db._find(query='б', var_1='1', var_2='у', var_3='г', reply=['var_1']))

    def test_find_WithBooleanProps(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': ActiveProperty('var_2', 2, 'B', boolean=False, full_text=True),
            'var_3': ActiveProperty('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'}, True)
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'}, True)
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db._find(var_1='1', var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}], 1),
                db._find(query='г', var_1='1', var_2='у', var_3='г', reply=['var_1']))

        self.assertEqual(
                ([], 0),
                db._find(query='б', var_1='1', var_2='у', var_3='г', reply=['var_1']))

    def test_find_ExactQuery(self):
        db = Index({'key': ActiveProperty('key', 1, 'K', full_text=True)})

        db.store('1', {'key': 'фу'}, True)
        db.store('2', {'key': 'фу бар'}, True)
        db.store('3', {'key': 'фу бар тест'}, True)

        self.assertEqual(
                ([{'guid': '1', 'key': u'фу'}, {'guid': '2', 'key': u'фу бар'}, {'guid': '3', 'key': u'фу бар тест'}], 3),
                db._find(query='key:фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': u'фу бар'}, {'guid': '3', 'key': u'фу бар тест'}], 2),
                db._find(query='key:"фу бар"', reply=['key']))

        self.assertEqual(
                ([{'guid': '1', 'key': u'фу'}], 1),
                db._find(query='key:=фу', reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': u'фу бар'}], 1),
                db._find(query='key:="фу бар"', reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': u'фу бар тест'}], 1),
                db._find(query='key:="фу бар тест"', reply=['key']))

    def test_find_ExactQueryTerms(self):
        term = 'azAZ09_'

        db = Index({term: ActiveProperty(term, 1, 'T', full_text=True)})

        db.store('1', {term: 'test'}, True)
        db.store('2', {term: 'test fail'}, True)

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db._find(query='%s:=test' % term, reply=['guid']))

    def test_find_ReturnPortions(self):
        db = Index({'key': ActiveProperty('key', 1, 'K')})

        db.store('1', {'key': '1'}, True)
        db.store('2', {'key': '2'}, True)
        db.store('3', {'key': '3'}, True)

        self.assertEqual(
                ([{'guid': '1', 'key': '1'}], 3),
                db._find(offset=0, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '2', 'key': '2'}], 3),
                db._find(offset=1, limit=1, reply=['key']))
        self.assertEqual(
                ([{'guid': '3', 'key': '3'}], 3),
                db._find(offset=2, limit=1, reply=['key']))
        self.assertEqual(
                ([], 3),
                db._find(offset=3, limit=1, reply=['key']))

    def test_find_OrderBy(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A'),
            'var_2': ActiveProperty('var_2', 2, 'B'),
            })

        db.store('1', {'var_1': '1', 'var_2': '3'}, True)
        db.store('2', {'var_1': '2', 'var_2': '2'}, True)
        db.store('3', {'var_1': '3', 'var_2': '1'}, True)

        self.assertEqual(
                ([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}], 3),
                db._find(order_by='var_1'))
        self.assertEqual(
                ([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}], 3),
                db._find(order_by='+var_1'))
        self.assertEqual(
                ([{'guid': '3'}, {'guid': '2'}, {'guid': '1'}], 3),
                db._find(order_by='-var_1'))

        self.assertEqual(
                ([{'guid': '3'}, {'guid': '2'}, {'guid': '1'}], 3),
                db._find(order_by='var_2'))
        self.assertEqual(
                ([{'guid': '3'}, {'guid': '2'}, {'guid': '1'}], 3),
                db._find(order_by='+var_2'))
        self.assertEqual(
                ([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}], 3),
                db._find(order_by='-var_2'))

    def test_find_GroupBy(self):
        db = Index({
            'var_1': ActiveProperty('var_1', 1, 'A'),
            'var_2': ActiveProperty('var_2', 2, 'B'),
            'var_3': ActiveProperty('var_3', 3, 'C'),
            })

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '3'}, True)
        db.store('2', {'var_1': '2', 'var_2': '1', 'var_3': '4'}, True)
        db.store('3', {'var_1': '3', 'var_2': '2', 'var_3': '4'}, True)

        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '3', 'var_1': '3'}], 2),
                db._find(reply=['var_1'], group_by='var_2'))
        self.assertEqual(
                ([{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}], 2),
                db._find(reply=['var_1'], group_by='var_3'))

    def test_MultipleValues(self):
        db = Index({
            'prop': ActiveProperty('prop', prefix='B', typecast=[1, 2], full_text=True),
            })
        db.store('1', {'prop': [1, 2]}, True)
        db.store('2', {'prop': [2, 3]}, True)
        self.assertEqual(
                [{'guid': '1'}],
                db._find(prop=1, reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '1'}, {'guid': '2'}],
                db._find(prop=2, reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '1'}, {'guid': '2'}],
                db._find(query='2', reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '2'}],
                db._find(query='3', reply=['guid'])[0])
        db.close()

        db = Index({
            'prop': ActiveProperty('prop', prefix='B', typecast=[], full_text=True),
            })
        db.store('1', {'prop': ['a', 'b']}, True)
        db.store('2', {'prop': ['b', 'c']}, True)
        self.assertEqual(
                [{'guid': '1'}],
                db._find(prop='a', reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '1'}, {'guid': '2'}],
                db._find(prop='b', reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '1'}, {'guid': '2'}],
                db._find(query='b', reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '2'}],
                db._find(query='c', reply=['guid'])[0])
        db.close()

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
        # No index at start; checkpoint didn't happen
        db = Index({})
        self.assertEqual(0, db.mtime)
        db.store('1', {}, True)
        db.commit()
        db.close()

        # Index exists at start; checkpoint didn't happen
        db = Index({})
        self.assertEqual(0, db.mtime)
        db.store('2', {}, True)
        db.commit()
        self.assertEqual(0, db.mtime)
        db.close()

        # Index exists at start; mtime exists at start; checkpoint didn't happen
        self.touch('index/mtime')
        os.utime('index/mtime', (1, 1))
        db = Index({})
        self.assertEqual(1, db.mtime)
        db.store('3', {}, True)
        db.commit()
        self.assertEqual(1, db.mtime)
        db.close()

        # Index exists at start; checkpoint happened
        db = Index({})
        db.checkpoint()
        self.assertNotEqual(1, db.mtime)
        os.utime('index/mtime', (1, 1))
        self.assertEqual(1, db.mtime)
        db.store('4', {}, True)
        db.commit()
        self.assertNotEqual(1, db.mtime)
        db.close()

    def test_find_OrderByGUIDAllTime(self):
        db = Index({'prop': ActiveProperty('prop', 1, 'P')})

        db.store('3', {'prop': '1'}, True)
        db.store('2', {'prop': '1'}, True)
        db.store('1', {'prop': '3'}, True)

        self.assertEqual(
                ([{'guid': '1', 'prop': '3'}, {'guid': '2', 'prop': '1'}, {'guid': '3', 'prop': '1'}], 3),
                db._find(reply=['prop']))

        self.assertEqual(
                ([{'guid': '2', 'prop': '1'}, {'guid': '3', 'prop': '1'}, {'guid': '1', 'prop': '3'}], 3),
                db._find(reply=['prop'], order_by='prop'))

        self.assertEqual(
                ([{'guid': '1', 'prop': '3'}, {'guid': '2', 'prop': '1'}, {'guid': '3', 'prop': '1'}], 3),
                db._find(reply=['prop'], order_by='-prop'))

    def test_find_Region(self):
        term = 'azAZ09_'

        db = Index({term: ActiveProperty(term, 1, 'T', full_text=True)})

        db.store('1', {term: 'test'}, True)
        db.store('2', {term: 'test fail'}, True)

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db._find(query='%s:=test' % term, reply=['guid']))

    def test_find_WithListProps(self):
        db = Index({'prop': ActiveProperty('prop', None, 'A', full_text=True, typecast=[])})

        db.store('1', {'prop': ('a', )}, True)
        db.store('2', {'prop': ('a', 'aa')}, True)
        db.store('3', {'prop': ('aa', 'aaa')}, True)

        self.assertEqual(
                ([{'guid': '1'}, {'guid': '2'}], 2),
                db._find(prop='a', reply=['prop']))

        self.assertEqual(
                ([{'guid': '2'}, {'guid': '3'}], 2),
                db._find(prop='aa'))

        self.assertEqual(
                ([{'guid': '3'}], 1),
                db._find(prop='aaa'))

    def test_FlushThreshold(self):
        commits = []

        db = Index({}, lambda: commits.append(True))
        coroutine.dispatch()
        env.index_flush_threshold.value = 1
        db.store('1', {}, True)
        coroutine.dispatch()
        db.store('2', {}, True)
        coroutine.dispatch()
        db.store('3', {}, True)
        coroutine.dispatch()
        self.assertEqual(3, len(commits))
        db.close()

        del commits[:]
        db = Index({}, lambda: commits.append(True))
        coroutine.dispatch()
        env.index_flush_threshold.value = 2
        db.store('4', {}, True)
        coroutine.dispatch()
        db.store('5', {}, True)
        coroutine.dispatch()
        db.store('6', {}, True)
        coroutine.dispatch()
        db.store('7', {}, True)
        coroutine.dispatch()
        db.store('8', {}, True)
        coroutine.dispatch()
        self.assertEqual(2, len(commits))
        db.close()

    def test_FlushTimeout(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 1

        commits = []

        db = Index({}, lambda: commits.append(True))
        coroutine.dispatch()

        db.store('1', {}, True)
        coroutine.dispatch()
        self.assertEqual(0, len(commits))
        db.store('2', {}, True)
        coroutine.dispatch()
        self.assertEqual(0, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(1, len(commits))

        db.store('1', {}, True)
        coroutine.dispatch()
        self.assertEqual(1, len(commits))
        db.store('2', {}, True)
        coroutine.dispatch()
        self.assertEqual(1, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(2, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(2, len(commits))

    def test_DoNotMissImmediateCommitEvent(self):
        env.index_flush_threshold.value = 1
        commits = []
        db = Index({}, lambda: commits.append(True))

        db.store('1', {}, True)
        coroutine.dispatch()
        self.assertEqual(1, len(commits))

    def test_SortLocalizedProps(self):
        env.DEFAULT_LANG = 'default_lang'
        current_lang = locale.getdefaultlocale()[0].replace('_', '-')

        db = Index({'prop': ActiveProperty('prop', 1, 'A', localized=True)})

        db.store('0', {'prop': {'foo': '6'}}, True)
        db.store('1', {'prop': {current_lang: '1', 'default_lang': '4', 'foo': '9'}}, True)
        db.store('2', {'prop': {'default_lang': '2', 'foo': '8'}}, True)
        db.store('3', {'prop': {current_lang: '3', 'foo': '7'}}, True)

        self.assertEqual([
            {'guid': '1'},
            {'guid': '2'},
            {'guid': '3'},
            {'guid': '0'},
            ],
            db._find(order_by='prop')[0])

        self.assertEqual([
            {'guid': '0'},
            {'guid': '3'},
            {'guid': '2'},
            {'guid': '1'},
            ],
            db._find(order_by='-prop')[0])

    def test_find_MultipleFilter(self):
        db = Index({'prop': ActiveProperty('prop', 1, 'A')})

        db.store('1', {'prop': 'a'}, True)
        db.store('2', {'prop': 'b'}, True)
        db.store('3', {'prop': 'c'}, True)

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                db._find(prop=[], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    ]),
                db._find(prop='a', reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '2'},
                    ]),
                db._find(prop=['a', 'b'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                db._find(prop=['a', 'b', 'c'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '2'},
                    ]),
                db._find(prop=['b', 'foo', 'bar'], reply=['guid'])[0])

    def test_find_NotFilter(self):
        db = Index({'prop': ActiveProperty('prop', 1, 'A')})

        db.store('1', {'prop': 'a'}, True)
        db.store('2', {'prop': 'b'}, True)
        db.store('3', {'prop': 'c'}, True)

        self.assertEqual(
                sorted([
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                db._find(prop='!a', reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '3'},
                    ]),
                db._find(prop=['!a', '!b'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    ]),
                db._find(prop=['!a', '!b', '!c'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '3'},
                    ]),
                db._find(prop=['!b', 'c'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '3'},
                    ]),
                db._find(prop=['a', '!b', 'c'], reply=['guid'])[0])

    def test_find_AndNotFilter(self):
        db = Index({'prop': ActiveProperty('prop', 1, 'A')})

        db.store('1', {'prop': 'a'}, True)
        db.store('2', {'prop': 'b'}, True)
        db.store('3', {'prop': 'c'}, True)

        self.assertEqual(
                sorted([
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                db._find(prop='-a', reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '3'},
                    ]),
                db._find(prop=['-a', '-b'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    ]),
                db._find(prop=['-a', '-b', '-c'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '3'},
                    ]),
                db._find(prop=['-b', 'c'], reply=['guid'])[0])

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '3'},
                    ]),
                db._find(prop=['a', '-b', 'c'], reply=['guid'])[0])


class Index(index.IndexWriter):

    def __init__(self, props, *args):

        class Document(object):
            pass

        metadata = Metadata(Index)
        metadata.update(props)
        metadata['guid'] = ActiveProperty('guid',
                permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=0,
                prefix=env.GUID_PREFIX)

        index.IndexWriter.__init__(self, tests.tmpdir + '/index', metadata, *args)

    def _find(self, *args, **kwargs):
        if 'reply' not in kwargs:
            kwargs['reply'] = {}
        if 'order_by' not in kwargs:
            kwargs['order_by'] = 'guid'

        mset = self.find(env.Query(*args, **kwargs))
        result = []

        for hit in self.find(env.Query(*args, **kwargs)):
            props = {}
            for name in kwargs['reply']:
                prop = self.metadata[name]
                if prop.slot is not None:
                    props[name] = hit.document.get_value(prop.slot).decode('utf8')
            props['guid'] = hit.document.get_value(0).decode('utf8')
            result.append(props)

        return result, mset.get_matches_estimated()


if __name__ == '__main__':
    tests.main()
