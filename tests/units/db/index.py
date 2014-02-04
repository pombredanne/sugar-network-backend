#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import time
import shutil
import locale
from os.path import exists

from __init__ import tests

from sugar_network import toolkit
from sugar_network.db import index
from sugar_network.db.metadata import Metadata, Property, GUID_PREFIX, Boolean, Enum, List, Localized, Numeric
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit import coroutine, i18n


class IndexTest(tests.Test):

    def test_Term_AvoidCollisionsWithGuid(self):
        self.assertRaises(RuntimeError, Property, 'key', 0, 'I')
        self.assertRaises(RuntimeError, Property, 'key', 0, 'K')
        self.assertRaises(RuntimeError, Property, 'key', 1, 'I')
        Property('key', 1, 'K')
        Property('guid', 0, 'I')

    def test_Create(self):
        db = Index({'key': Property('key', 1, 'K')})

        self.assertEqual(
                ([], 0),
                db._find())

        db.store('1', {'key': 'value_1'})
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'}], 1),
                db._find(reply=['key']))

        db.store('2', {'key': 'value_2'})
        self.assertEqual(
                ([{'guid': '1', 'key': 'value_1'},
                  {'guid': '2', 'key': 'value_2'}], 2),
                db._find(reply=['key']))

    def test_update(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            })

        db.store('1', {'var_1': 'value_1', 'var_2': 'value_2'})
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_1', 'var_2': 'value_2'}], 1),
                db._find(reply=['var_1', 'var_2']))

        db.store('1', {'var_1': 'value_3', 'var_2': 'value_4'})
        self.assertEqual(
                ([{'guid': '1', 'var_1': 'value_3', 'var_2': 'value_4'}], 1),
                db._find(reply=['var_1', 'var_2']))

    def test_delete(self):
        db = Index({'key': Property('key', 1, 'K')})

        db.store('1', {'key': 'value'})
        self.assertEqual(
                ([{'guid': '1', 'key': 'value'}], 1),
                db._find(reply=['key']))

        db.delete('1')
        self.assertEqual(
                ([], 0),
                db._find(reply=['key']))

    def test_IndexCalculatedValue(self):

        class Property2(Property):

            def encode(self, value):
                yield "foo" + value

            def decode(self, value):
                return "foo" + value

        db = Index({'key': Property2('key', 1, 'K')})

        db.store('1', {'key': 'bar'})

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

    def test_IndexCalculatedValues(self):

        class Property2(Property):

            def encode(self, value):
                if value != 'fake':
                    yield 'foo'
                    yield 'bar'
                yield value

        db = Index({'key': Property2('key', 1, 'K')})
        db.store('1', {'key': 'value'})

        self.assertEqual(
                [{'guid': '1'}],
                db._find(key='foo')[0])
        self.assertEqual(
                [{'guid': '1'}],
                db._find(key='bar')[0])
        self.assertEqual(
                [{'guid': '1'}],
                db._find(key='value')[0])
        self.assertEqual(
                [],
                db._find(key='fake')[0])

    def test_find(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A', full_text=True),
            'var_2': Property('var_2', 2, 'B', full_text=True),
            'var_3': Property('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

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
            'prop': Property('prop', 1, 'P', full_text=True),
            })

        db.store('guid', {'prop': 'value'})

        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(reply=['prop'])[0])
        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(prop=None, reply=['prop'])[0])
        self.assertEqual(
                [{'guid': 'guid', 'prop': 'value'}],
                db._find(guid=None, reply=['prop'])[0])

    def test_find_DecodeArgs(self):
        db = Index({
            'var_1': Boolean('var_1', 1, 'A'),
            })

        db.store('1', {'var_1': True})
        db.store('2', {'var_1': False})

        self.assertEqual(
                [{'guid': '1'}],
                db._find(var_1=True, reply=['guid'])[0])
        self.assertEqual(
                [{'guid': '2'}],
                db._find(var_1=False, reply=['guid'])[0])

    def test_find_WithProps(self):
        db = Index({
            'var_1': Property('var_1', 1, 'A', full_text=True),
            'var_2': Property('var_2', 2, 'B', full_text=True),
            'var_3': Property('var_3', 3, 'C', full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

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
            'var_1': Property('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': Property('var_2', 2, 'B', boolean=True, full_text=True),
            'var_3': Property('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

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
            'var_1': Property('var_1', 1, 'A', boolean=True, full_text=True),
            'var_2': Property('var_2', 2, 'B', boolean=False, full_text=True),
            'var_3': Property('var_3', 3, 'C', boolean=True, full_text=True),
            })

        db.store('1', {'var_1': '1', 'var_2': 'у', 'var_3': 'г'})
        db.store('2', {'var_1': '2', 'var_2': 'у', 'var_3': 'ю'})
        db.store('3', {'var_1': '3', 'var_2': 'б', 'var_3': 'ю'})

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
        db = Index({'key': Property('key', 1, 'K', full_text=True)})

        db.store('1', {'key': 'фу'})
        db.store('2', {'key': 'фу бар'})
        db.store('3', {'key': 'фу бар тест'})

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

        db = Index({term: Property(term, 1, 'T', full_text=True)})

        db.store('1', {term: 'test'})
        db.store('2', {term: 'test fail'})

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db._find(query='%s:=test' % term, reply=['guid']))

    def test_find_ReturnPortions(self):
        db = Index({'key': Property('key', 1, 'K')})

        db.store('1', {'key': '1'})
        db.store('2', {'key': '2'})
        db.store('3', {'key': '3'})

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
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            })

        db.store('1', {'var_1': '1', 'var_2': '3'})
        db.store('2', {'var_1': '2', 'var_2': '2'})
        db.store('3', {'var_1': '3', 'var_2': '1'})

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
            'var_1': Property('var_1', 1, 'A'),
            'var_2': Property('var_2', 2, 'B'),
            'var_3': Property('var_3', 3, 'C'),
            'var_4': Property('var_4', 4, 'D'),
            })

        db.store('1', {'var_1': '1', 'var_2': '1', 'var_3': '3', 'var_4': '0'})
        db.store('2', {'var_1': '2', 'var_2': '1', 'var_3': '4', 'var_4': '0'})
        db.store('3', {'var_1': '3', 'var_2': '2', 'var_3': '4', 'var_4': '0'})

        self.assertEqual(
                [{'guid': '1', 'var_1': '1'}, {'guid': '3', 'var_1': '3'}],
                db._find(reply=['var_1'], group_by='var_2')[0])
        self.assertEqual(
                [{'guid': '1', 'var_1': '1'}, {'guid': '2', 'var_1': '2'}],
                db._find(reply=['var_1'], group_by='var_3')[0])
        self.assertEqual(
                [{'guid': '1'}],
                db._find(reply=['guid'], group_by='var_4', order_by='var_1')[0])
        self.assertEqual(
                [{'guid': '3'}],
                db._find(reply=['guid'], group_by='var_4', order_by='-var_1')[0])

    def test_MultipleValues(self):
        db = Index({
            'prop': List(name='prop', prefix='B', subtype=Enum([1, 2, 3]), full_text=True),
            })
        db.store('1', {'prop': [1, 2]})
        db.store('2', {'prop': [2, 3]})
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
            'prop': List(name='prop', prefix='B', full_text=True),
            })
        db.store('1', {'prop': ['a', 'b']})
        db.store('2', {'prop': ['b', 'c']})
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

        db.store('1', {},
                lambda *args: pre_stored.append(args),
                lambda *args: post_stored.append(args))
        self.assertEqual(1, len(pre_stored))
        self.assertEqual(1, len(post_stored))

        db.store('1', {},
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
        db.store('1', {})
        db.commit()
        db.close()

        # Index exists at start; commit did happen
        db = Index({})
        self.assertNotEqual(0, db.mtime)
        db.close()

        # Index exists at start; mtime is outdated
        os.utime('index/mtime', (1, 1))
        db = Index({})
        self.assertEqual(1, db.mtime)
        db.store('3', {})
        db.commit()
        self.assertNotEqual(1, db.mtime)
        db.close()

    def test_find_OrderByGUIDAllTime(self):
        db = Index({'prop': Property('prop', 1, 'P')})

        db.store('3', {'prop': '1'})
        db.store('2', {'prop': '1'})
        db.store('1', {'prop': '3'})

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

        db = Index({term: Property(term, 1, 'T', full_text=True)})

        db.store('1', {term: 'test'})
        db.store('2', {term: 'test fail'})

        self.assertEqual(
                ([{'guid': '1'}], 1),
                db._find(query='%s:=test' % term, reply=['guid']))

    def test_find_WithListProps(self):
        db = Index({'prop': List(name='prop', prefix='A', full_text=True)})

        db.store('1', {'prop': ('a', )})
        db.store('2', {'prop': ('a', 'aa')})
        db.store('3', {'prop': ('aa', 'aaa')})

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
        index.index_flush_threshold.value = 1
        db.store('1', {})
        coroutine.dispatch()
        db.store('2', {})
        coroutine.dispatch()
        db.store('3', {})
        coroutine.dispatch()
        self.assertEqual(3, len(commits))
        db.close()

        del commits[:]
        db = Index({}, lambda: commits.append(True))
        coroutine.dispatch()
        index.index_flush_threshold.value = 2
        db.store('4', {})
        coroutine.dispatch()
        db.store('5', {})
        coroutine.dispatch()
        db.store('6', {})
        coroutine.dispatch()
        db.store('7', {})
        coroutine.dispatch()
        db.store('8', {})
        coroutine.dispatch()
        self.assertEqual(2, len(commits))
        db.close()

    def test_FlushTimeout(self):
        index.index_flush_threshold.value = 0
        index.index_flush_timeout.value = 1

        commits = []

        db = Index({}, lambda: commits.append(True))
        coroutine.dispatch()

        db.store('1', {})
        coroutine.dispatch()
        self.assertEqual(0, len(commits))
        db.store('2', {})
        coroutine.dispatch()
        self.assertEqual(0, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(1, len(commits))

        db.store('1', {})
        coroutine.dispatch()
        self.assertEqual(1, len(commits))
        db.store('2', {})
        coroutine.dispatch()
        self.assertEqual(1, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(2, len(commits))

        coroutine.sleep(1.5)
        self.assertEqual(2, len(commits))

    def test_DoNotMissImmediateCommitEvent(self):
        index.index_flush_threshold.value = 1
        commits = []
        db = Index({}, lambda: commits.append(True))

        db.store('1', {})
        coroutine.dispatch()
        self.assertEqual(1, len(commits))

    def test_SortLocalizedProps(self):
        i18n._default_langs = ['default_lang']
        current_lang = locale.getdefaultlocale()[0].replace('_', '-')

        db = Index({'prop': Localized(name='prop', slot=1, prefix='A')})

        db.store('0', {'prop': {'foo': '5'}})
        db.store('1', {'prop': {current_lang: '4', 'default_lang': '1', 'foo': '3'}})
        db.store('2', {'prop': {'default_lang': '2', 'foo': '2'}})
        db.store('3', {'prop': {current_lang: '3', 'foo': '6'}})

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

    def test_SearchByLocalizedProps(self):
        db = Index({'prop': Localized(name='prop', slot=1, prefix='A', full_text=True)})

        db.store('1', {'prop': {'a': 'ё'}})
        db.store('2', {'prop': {'a': 'ё', 'b': 'ю'}})
        db.store('3', {'prop': {'a': 'ю', 'b': 'ё', 'c': 'я'}})

        self.assertEqual(
                sorted([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(prop='ё')[0]))
        self.assertEqual(
                sorted([{'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(prop='ю')[0]))
        self.assertEqual(
                sorted([{'guid': '3'}]),
                sorted(db._find(prop='я')[0]))

        self.assertEqual(
                sorted([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(query='ё')[0]))
        self.assertEqual(
                sorted([{'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(query='ю')[0]))
        self.assertEqual(
                sorted([{'guid': '3'}]),
                sorted(db._find(query='я')[0]))

        self.assertEqual(
                sorted([{'guid': '1'}, {'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(query='prop:ё')[0]))
        self.assertEqual(
                sorted([{'guid': '2'}, {'guid': '3'}]),
                sorted(db._find(query='prop:ю')[0]))
        self.assertEqual(
                sorted([{'guid': '3'}]),
                sorted(db._find(query='prop:я')[0]))

    def test_find_MultipleFilter(self):
        db = Index({'prop': Property('prop', 1, 'A')})

        db.store('1', {'prop': 'a'})
        db.store('2', {'prop': 'b'})
        db.store('3', {'prop': 'c'})

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

    def test_find_AndNotFilter(self):
        db = Index({'prop': Property('prop', 1, 'A')})

        db.store('1', {'prop': 'a'})
        db.store('2', {'prop': 'b'})
        db.store('3', {'prop': 'c'})

        self.assertEqual(
                sorted([
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                sorted(db._find(reply=['guid'], not_prop='a')[0]))

        self.assertEqual(
                sorted([
                    {'guid': '2'},
                    {'guid': '3'},
                    ]),
                sorted(db._find(reply=['guid'], **{'!prop': 'a'})[0]))

        self.assertEqual(
                sorted([
                    {'guid': '3'},
                    ]),
                sorted(db._find(reply=['guid'], **{'!prop': ['a', 'b']})[0]))

        self.assertEqual(
                sorted([
                    ]),
                sorted(db._find(reply=['guid'], **{'!prop': ['a', 'b', 'c']})[0]))

        self.assertEqual(
                sorted([
                    {'guid': '3'},
                    ]),
                sorted(db._find(prop='c', reply=['guid'], **{'!prop': 'b'})[0]))

        self.assertEqual(
                sorted([
                    {'guid': '1'},
                    {'guid': '3'},
                    ]),
                sorted(db._find(prop=['a', 'c'], reply=['guid'], **{'!prop': 'b'})[0]))

    def test_find_CustomEncode(self):
        db = Index({'trait': Numeric('trait', 1, 'A')})

        db.store('1', {'trait': 1})
        db.store('2', {'trait': 2})
        db.store('11', {'trait': 11})

        self.assertEqual([{'guid': '1'}], db._find(trait='1')[0])
        self.assertEqual([{'guid': '1'}], db._find(trait=1)[0])

        self.assertEqual([{'guid': '2'}], db._find(trait='2')[0])
        self.assertEqual([{'guid': '2'}], db._find(trait=2)[0])

        self.assertEqual([{'guid': '11'}], db._find(trait='11')[0])
        self.assertEqual([{'guid': '11'}], db._find(trait=11)[0])


class Index(index.IndexWriter):

    def __init__(self, props, *args):

        class Document(object):
            pass

        metadata = Metadata(Index)
        metadata.update(props)
        metadata['guid'] = Property('guid',
                acl=ACL.CREATE | ACL.READ, slot=0,
                prefix=GUID_PREFIX)

        index.IndexWriter.__init__(self, tests.tmpdir + '/index', metadata, *args)

    def _find(self, reply=None, **kwargs):
        mset = self.find(**kwargs)
        result = []

        for hit in mset:
            props = {}
            for name in (reply or []):
                prop = self.metadata[name]
                if prop.slot is not None:
                    props[name] = hit.document.get_value(prop.slot).decode('utf8')
            props['guid'] = hit.document.get_value(0).decode('utf8')
            result.append(props)

        return result, mset.get_matches_estimated()


if __name__ == '__main__':
    tests.main()
