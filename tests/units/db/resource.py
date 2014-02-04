#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import json
import sys
import stat
import time
import urllib2
import hashlib
from base64 import b64encode
from cStringIO import StringIO
from os.path import join, exists

import gobject

from __init__ import tests

from sugar_network import db
from sugar_network.db import storage, index
from sugar_network.db import directory as directory_
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, Sequence


class ResourceTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        this.broadcast = lambda x: x

    def test_ActiveProperty_Slotted(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def slotted(self, value):
                return value

            @db.stored_property()
            def not_slotted(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual(1, directory.metadata['slotted'].slot)

        directory.create({'slotted': 'slotted', 'not_slotted': 'not_slotted'})

        docs, total = directory.find(order_by='slotted')
        self.assertEqual(1, total)
        self.assertEqual(
                [('slotted', 'not_slotted')],
                [(i.slotted, i.not_slotted) for i in docs])

        self.assertRaises(RuntimeError, directory.find, order_by='not_slotted')

    def test_ActiveProperty_SlottedIUnique(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop_1(self, value):
                return value

            @db.indexed_property(slot=1)
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter)

    def test_ActiveProperty_Terms(self):

        class Document(db.Resource):

            @db.indexed_property(prefix='T')
            def term(self, value):
                return value

            @db.stored_property()
            def not_term(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual('T', directory.metadata['term'].prefix)

        guid = directory.create({'term': 'term', 'not_term': 'not_term'})

        docs, total = directory.find(term='term')
        self.assertEqual(1, total)
        self.assertEqual(
                [('term', 'not_term')],
                [(i.term, i.not_term) for i in docs])

        self.assertEqual(0, directory.find(query='not_term:not_term')[-1])
        self.assertEqual(1, directory.find(query='not_term:=not_term')[-1])

    def test_ActiveProperty_TermsUnique(self):

        class Document(db.Resource):

            @db.indexed_property(prefix='P')
            def prop_1(self, value):
                return value

            @db.indexed_property(prefix='P')
            def prop_2(self, value):
                return value

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter)

    def test_ActiveProperty_FullTextSearch(self):

        class Document(db.Resource):

            @db.indexed_property(full_text=False, slot=1)
            def no(self, value):
                return value

            @db.indexed_property(full_text=True, slot=2)
            def yes(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        self.assertEqual(False, directory.metadata['no'].full_text)
        self.assertEqual(True, directory.metadata['yes'].full_text)

        guid = directory.create({'no': 'foo', 'yes': 'bar'})

        self.assertEqual(0, directory.find(query='foo')[-1])
        self.assertEqual(1, directory.find(query='bar')[-1])

    def test_update(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop_1(self, value):
                return value

            @db.stored_property()
            def prop_2(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'prop_1': '1', 'prop_2': '2'})
        self.assertEqual(
                [('1', '2')],
                [(i.prop_1, i.prop_2) for i in directory.find()[0]])

        directory.update(guid, {'prop_1': '3', 'prop_2': '4'})
        self.assertEqual(
                [('3', '4')],
                [(i.prop_1, i.prop_2) for i in directory.find()[0]])

    def test_delete(self):

        class Document(db.Resource):

            @db.indexed_property(prefix='P')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid_1 = directory.create({'prop': '1'})
        guid_2 = directory.create({'prop': '2'})
        guid_3 = directory.create({'prop': '3'})

        self.assertEqual(
                ['1', '2', '3'],
                [i.prop for i in directory.find()[0]])

        directory.delete(guid_2)
        self.assertEqual(
                ['1', '3'],
                [i.prop for i in directory.find()[0]])

        directory.delete(guid_3)
        self.assertEqual(
                ['1'],
                [i.prop for i in directory.find()[0]])

        directory.delete(guid_1)
        self.assertEqual(
                [],
                [i.prop for i in directory.find()[0]])

    def test_populate(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(
                ('1/1/guid', '{"value": "1"}'),
                ('1/1/ctime', '{"value": 1}'),
                ('1/1/mtime', '{"value": 1}'),
                ('1/1/prop', '{"value": "prop-1"}'),
                ('1/1/seqno', '{"value": 0}'),

                ('2/2/guid', '{"value": "2"}'),
                ('2/2/ctime', '{"value": 2}'),
                ('2/2/mtime', '{"value": 2}'),
                ('2/2/prop', '{"value": "prop-2"}'),
                ('2/2/seqno', '{"value": 0}'),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertEqual(0, directory._index.mtime)
        for i in directory.populate():
            pass
        self.assertNotEqual(0, directory._index.mtime)

        doc = directory.get('1')
        self.assertEqual(1, doc['ctime'])
        self.assertEqual(1, doc['mtime'])
        self.assertEqual('prop-1', doc['prop'])

        doc = directory.get('2')
        self.assertEqual(2, doc['ctime'])
        self.assertEqual(2, doc['mtime'])
        self.assertEqual('prop-2', doc['prop'])

        self.assertEqual(
                [
                    (1, 1, 'prop-1'),
                    (2, 2, 'prop-2'),
                    ],
                [(i.ctime, i.mtime, i.prop) for i in directory.find()[0]])

    def test_populate_IgnoreBadDocuments(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.touch(
                ('1/1/guid', '{"value": "1"}'),
                ('1/1/ctime', '{"value": 1}'),
                ('1/1/mtime', '{"value": 1}'),
                ('1/1/prop', '{"value": "prop-1"}'),
                ('1/1/seqno', '{"value": 0}'),

                ('2/2/guid', '{"value": "2"}'),
                ('2/2/ctime', ''),
                ('2/2/mtime', '{"value": 2}'),
                ('2/2/prop', '{"value": "prop-2"}'),
                ('2/2/seqno', '{"value": 0}'),

                ('3/3/guid', ''),
                ('3/3/ctime', ''),
                ('3/3/mtime', ''),
                ('3/3/prop', ''),
                ('3/3/seqno', ''),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        populated = 0
        for i in directory.populate():
            populated += 1
        self.assertEqual(1, populated)
        self.assertEqual(
                sorted(['1']),
                sorted([i.guid for i in directory.find()[0]]))
        assert exists('1/1/guid')
        assert not exists('2/2/guid')
        assert not exists('3/3/guid')

    def test_create_with_guid(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({'guid': 'guid', 'prop': 'foo'})
        self.assertEqual(
                [('guid', 'foo')],
                [(i.guid, i.prop) for i in directory.find()[0]])

        directory.update(guid, {'prop': 'probe'})
        self.assertEqual(
                [('guid', 'probe')],
                [(i.guid, i.prop) for i in directory.find()[0]])

    def test_seqno(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid_1 = directory.create({'prop': 'value'})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(1, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

        guid_2 = directory.create({'prop': 'value'})
        seqno = directory.get(guid_2).get('seqno')
        self.assertEqual(2, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_2[:2], guid_2)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_2[:2], guid_2)))['seqno'],
                seqno)

        directory.update(guid_1, {'prop': 'new'})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(3, seqno)
        self.assertEqual(
                json.load(file('%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                1)
        self.assertEqual(
                json.load(file('%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

    def test_patch(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2)
            def prop2(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        guid = directory.create({'guid': '1', 'prop1': '1', 'prop2': '2'})
        doc = directory.get(guid)

        self.assertEqual({}, doc.patch({}))
        self.assertEqual({}, doc.patch({'prop1': '1', 'prop2': '2'}))
        self.assertEqual({'prop1': '1_'}, doc.patch({'prop1': '1_', 'prop2': '2'}))
        self.assertEqual({'prop1': '1_', 'prop2': '2_'}, doc.patch({'prop1': '1_', 'prop2': '2_'}))

    def test_patch_LocalizedProps(self):

        class Document(db.Resource):

            @db.indexed_property(db.Localized, slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        guid = directory.create({'guid': '1', 'prop': {'ru': 'ru'}})
        doc = directory.get(guid)

        self.assertEqual({}, doc.patch({'prop': {'ru': 'ru'}}))
        self.assertEqual({'prop': {'ru': 'ru_'}}, doc.patch({'prop': {'ru': 'ru_'}}))
        self.assertEqual({'prop': {'en': 'en'}}, doc.patch({'prop': {'en': 'en'}}))
        self.assertEqual({'prop': {'ru': 'ru', 'en': 'en'}}, doc.patch({'prop': {'ru': 'ru', 'en': 'en'}}))
        self.assertEqual({'prop': {'ru': 'ru_', 'en': 'en'}}, doc.patch({'prop': {'ru': 'ru_', 'en': 'en'}}))

    def test_diff(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))

        directory.create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        directory.create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        for i in os.listdir('3/3'):
            os.utime('3/3/%s' % i, (3, 3))

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'prop': {'value': '1', 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                }},
            {'guid': '3', 'diff': {
                'guid': {'value': '3', 'mtime': 3},
                'ctime': {'value': 3, 'mtime': 3},
                'prop': {'value': '3', 'mtime': 3},
                'mtime': {'value': 3, 'mtime': 3},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq)])
        self.assertEqual([[1, 3]], out_seq)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                }},
            ],
            [i for i in diff(directory, [[2, 2]], out_seq)])
        self.assertEqual([[2, 2]], out_seq)

        out_seq = Sequence()
        self.assertEqual([
            ],
            [i for i in diff(directory, [[4, 100]], out_seq)])
        self.assertEqual([], out_seq)
        directory.update('2', {'prop': '22'})
        self.assertEqual([
            {'guid': '2', 'diff': {
                'prop': {'value': '22', 'mtime': int(os.stat('2/2/prop').st_mtime)},
                }},
            ],
            [i for i in diff(directory, [[4, 100]], out_seq)])
        self.assertEqual([[4, 4]], out_seq)

    def test_diff_IgnoreCalcProps(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.CALC)
            def prop(self, value):
                return value

        directory = Directory('.', Document, IndexWriter)

        directory.create({'guid': 'guid', 'prop': '1', 'ctime': 1, 'mtime': 1})
        self.utime('.', 1)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': 'guid', 'diff': {
                'guid': {'value': 'guid', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq)])
        self.assertEqual([[1, 1]], out_seq)

        directory.update('guid', {'prop': '2'})
        out_seq = Sequence()
        self.assertEqual([
            ],
            [i for i in diff(directory, [[6, 100]], out_seq)])
        self.assertEqual([], out_seq)

    def test_diff_Exclude(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        directory.create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        directory.create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        directory.update('2', {'prop': '2_'})
        self.utime('.', 0)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 0},
                'ctime': {'value': 1, 'mtime': 0},
                'prop': {'value': '1', 'mtime': 0},
                'mtime': {'value': 1, 'mtime': 0},
                }},
            {'guid': '2', 'diff': {
                'prop': {'value': '2_', 'mtime': 0},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq, [[2, 3]])])

        self.assertEqual([[1, 1], [4, 4]], out_seq)

    def test_diff_Filter(self):

        class Document(db.Resource):

            @db.indexed_property(prefix='P')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'ctime': 1, 'mtime': 1, 'prop': '1'})
        directory.create({'guid': '2', 'ctime': 2, 'mtime': 2, 'prop': '2'})
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': '2', 'mtime': 2},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq, prop='2')])
        self.assertEqual([[2, 2]], out_seq)

    def test_diff_GroupBy(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, prefix='P')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'ctime': 1, 'mtime': 1, 'prop': '0'})
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))
        directory.create({'guid': '2', 'ctime': 2, 'mtime': 2, 'prop': '0'})
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': '0', 'mtime': 2},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq, group_by='prop')])
        self.assertEqual([[2, 2]], out_seq)

    def test_diff_Aggprops(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.create({'guid': '1', 'prop': {'1': {'prop': 1}}, 'ctime': 1, 'mtime': 1})
        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))

        directory.create({'guid': '2', 'prop': {'2': {'prop': 2}}, 'ctime': 2, 'mtime': 2})
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'prop': {'value': {'1': {'prop': 1}}, 'mtime': 1},
                }},
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2}}, 'mtime': 2},
                }},
            ],
            [i for i in diff(directory, [[0, None]], out_seq)])
        self.assertEqual([[1, 2]], out_seq)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '1', 'diff': {
                'guid': {'value': '1', 'mtime': 1},
                'ctime': {'value': 1, 'mtime': 1},
                'mtime': {'value': 1, 'mtime': 1},
                'prop': {'value': {'1': {'prop': 1}}, 'mtime': 1},
                }},
            ],
            [i for i in diff(directory, [[1, 1]], out_seq)])
        self.assertEqual([[1, 1]], out_seq)

        out_seq = Sequence()
        self.assertEqual([
            {'guid': '2', 'diff': {
                'guid': {'value': '2', 'mtime': 2},
                'ctime': {'value': 2, 'mtime': 2},
                'mtime': {'value': 2, 'mtime': 2},
                'prop': {'value': {'2': {'prop': 2}}, 'mtime': 2},
                }},
            ],
            [i for i in diff(directory, [[2, 2]], out_seq)])
        self.assertEqual([[2, 2]], out_seq)

        out_seq = Sequence()
        self.assertEqual([
            ],
            [i for i in diff(directory, [[3, None]], out_seq)])
        self.assertEqual([], out_seq)

        self.assertEqual({
            '1': {'seqno': 1, 'prop': 1},
            },
            directory.get('1')['prop'])
        self.assertEqual({
            '2': {'seqno': 2, 'prop': 2},
            },
            directory.get('2')['prop'])

        out_seq = Sequence()
        directory.update('2', {'prop': {'2': {}, '3': {'prop': 3}}})
        self.assertEqual([
            {'guid': '2', 'diff': {
                'prop': {'value': {'2': {}, '3': {'prop': 3}}, 'mtime': int(os.stat('2/2/prop').st_mtime)},
                }},
            ],
            [i for i in diff(directory, [[3, None]], out_seq)])
        self.assertEqual([[3, 3]], out_seq)

        self.assertEqual({
            '2': {'seqno': 3},
            '3': {'seqno': 3, 'prop': 3},
            },
            directory.get('2')['prop'])

        out_seq = Sequence()
        directory.update('1', {'prop': {'1': {'foo': 'bar'}}})
        self.assertEqual([
            {'guid': '1', 'diff': {
                'prop': {'value': {'1': {'foo': 'bar'}}, 'mtime': int(os.stat('1/1/prop').st_mtime)},
                }},
            ],
            [i for i in diff(directory, [[4, None]], out_seq)])
        self.assertEqual([[4, 4]], out_seq)

        self.assertEqual({
            '1': {'seqno': 4, 'foo': 'bar'},
            },
            directory.get('1')['prop'])

        out_seq = Sequence()
        directory.update('2', {'prop': {'2': {'restore': True}}})
        self.assertEqual([
            {'guid': '2', 'diff': {
                'prop': {'value': {'2': {'restore': True}}, 'mtime': int(os.stat('2/2/prop').st_mtime)},
                }},
            ],
            [i for i in diff(directory, [[5, None]], out_seq)])
        self.assertEqual([[5, 5]], out_seq)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True},
            '3': {'seqno': 3, 'prop': 3},
            },
            directory.get('2')['prop'])

        out_seq = Sequence()
        directory.update('2', {'ctime': 0})
        self.assertEqual([
            {'guid': '2', 'diff': {
                'ctime': {'value': 0, 'mtime': int(os.stat('2/2/prop').st_mtime)},
                }},
            ],
            [i for i in diff(directory, [[6, None]], out_seq)])
        self.assertEqual([[6, 6]], out_seq)

        self.assertEqual({
            '2': {'seqno': 5, 'restore': True},
            '3': {'seqno': 3, 'prop': 3},
            },
            directory.get('2')['prop'])

    def test_merge_New(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)

        directory1.create({'guid': '1', 'prop': '1', 'ctime': 1, 'mtime': 1})
        for i in os.listdir('document1/1/1'):
            os.utime('document1/1/1/%s' % i, (1, 1))

        directory1.create({'guid': '2', 'prop': '2', 'ctime': 2, 'mtime': 2})
        for i in os.listdir('document1/2/2'):
            os.utime('document1/2/2/%s' % i, (2, 2))

        directory1.create({'guid': '3', 'prop': '3', 'ctime': 3, 'mtime': 3})
        for i in os.listdir('document1/3/3'):
            os.utime('document1/3/3/%s' % i, (3, 3))

        directory2 = Directory('document2', Document, IndexWriter)
        for patch in diff(directory1, [[0, None]], Sequence()):
            directory2.merge(**patch)

        self.assertEqual(
                sorted([
                    (1, '1', 1, '1'),
                    (2, '2', 2, '2'),
                    (3, '3', 3, '3'),
                    ]),
                sorted([(i['ctime'], i['prop'], i['mtime'], i['guid']) for i in directory2.find()[0]]))

        doc = directory2.get('1')
        self.assertEqual(1, doc.get('seqno'))
        self.assertEqual(1, doc.meta('guid')['mtime'])
        self.assertEqual(1, doc.meta('ctime')['mtime'])
        self.assertEqual(1, doc.meta('prop')['mtime'])
        self.assertEqual(1, doc.meta('mtime')['mtime'])

        doc = directory2.get('2')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])

        doc = directory2.get('3')
        self.assertEqual(3, doc.get('seqno'))
        self.assertEqual(3, doc.meta('guid')['mtime'])
        self.assertEqual(3, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('prop')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])

    def test_merge_Update(self):

        class Document(db.Resource):

            @db.stored_property(default='')
            def prop(self, value):
                return value

        directory1 = Directory('document1', Document, IndexWriter)
        directory2 = Directory('document2', Document, IndexWriter)

        directory1.create({'guid': 'guid', 'ctime': 1, 'mtime': 1})
        directory1.update('guid', {'prop': '1'})
        for i in os.listdir('document1/gu/guid'):
            os.utime('document1/gu/guid/%s' % i, (1, 1))

        directory2.create({'guid': 'guid', 'ctime': 2, 'mtime': 2})
        directory2.update('guid', {'prop': '2'})

        for i in os.listdir('document2/gu/guid'):
            os.utime('document2/gu/guid/%s' % i, (2, 2))

        self.assertEqual(
                [(2, 2, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in directory2.find()[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        for patch in diff(directory1, [[0, None]], Sequence()):
            directory2.merge(**patch)

        self.assertEqual(
                [(2, 2, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in directory2.find()[0]])
        doc = directory2.get('guid')
        self.assertEqual(2, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(2, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        os.utime('document1/gu/guid/mtime', (3, 3))
        for patch in diff(directory1, [[0, None]], Sequence()):
            directory2.merge(**patch)

        self.assertEqual(
                [(2, 1, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in directory2.find()[0]])
        doc = directory2.get('guid')
        self.assertEqual(3, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(2, doc.meta('prop')['mtime'])
        self.assertEqual('2', doc.meta('prop')['value'])

        os.utime('document1/gu/guid/prop', (4, 4))
        for patch in diff(directory1, [[0, None]], Sequence()):
            directory2.merge(**patch)

        self.assertEqual(
                [(2, 1, 'guid')],
                [(i['ctime'], i['mtime'], i['guid']) for i in directory2.find()[0]])
        doc = directory2.get('guid')
        self.assertEqual(4, doc.get('seqno'))
        self.assertEqual(2, doc.meta('guid')['mtime'])
        self.assertEqual(2, doc.meta('ctime')['mtime'])
        self.assertEqual(3, doc.meta('mtime')['mtime'])
        self.assertEqual(4, doc.meta('prop')['mtime'])
        self.assertEqual('1', doc.meta('prop')['value'])

    def test_merge_Aggprops(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop(self, value):
                return value

        directory = Directory('document', Document, IndexWriter)

        directory.merge('1', {
            'guid': {'mtime': 1, 'value': '1'},
            'ctime': {'mtime': 1, 'value': 1},
            'mtime': {'mtime': 1, 'value': 1},
            'prop': {'mtime': 1, 'value': {'1': {}}},
            })
        self.assertEqual({
            '1': {'seqno': 1},
            },
            directory.get('1')['prop'])

        directory.merge('1', {
            'prop': {'mtime': 1, 'value': {'1': {'probe': False}}},
            })
        self.assertEqual({
            '1': {'seqno': 1},
            },
            directory.get('1')['prop'])

        directory.merge('1', {
            'prop': {'mtime': 2, 'value': {'1': {'probe': True}}},
            })
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            },
            directory.get('1')['prop'])

        directory.merge('1', {
            'prop': {'mtime': 3, 'value': {'2': {'foo': 'bar'}}},
            })
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            '2': {'seqno': 3, 'foo': 'bar'},
            },
            directory.get('1')['prop'])

        directory.merge('1', {
            'prop': {'mtime': 4, 'value': {'2': {}, '3': {'foo': 'bar'}}},
            })
        self.assertEqual({
            '1': {'seqno': 2, 'probe': True},
            '2': {'seqno': 4},
            '3': {'seqno': 4, 'foo': 'bar'},
            },
            directory.get('1')['prop'])

    def test_merge_CallSetters(self):

        class Document(db.Resource):

            @db.stored_property(db.Numeric)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + 1

        directory = Directory('document', Document, IndexWriter)

        directory.merge('1', {
            'guid': {'mtime': 1, 'value': '1'},
            'ctime': {'mtime': 1, 'value': 1},
            'mtime': {'mtime': 1, 'value': 1},
            'prop': {'mtime': 1, 'value': 1},
            })
        self.assertEqual(2, directory.get('1')['prop'])

    def test_wipe(self):

        class Document(db.Resource):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        guid = directory.create({'prop': '1'})
        self.assertEqual([guid], [i.guid for i in directory.find()[0]])
        directory.commit()
        assert exists('index/mtime')

        directory.wipe()
        self.assertEqual([], [i.guid for i in directory.find()[0]])
        assert not exists('index/mtime')


def diff(directory, in_seq, out_seq, exclude_seq=None, **kwargs):
    for guid, patch in directory.diff(Sequence(in_seq), Sequence(exclude_seq) if exclude_seq else None, **kwargs):
        diff = {}
        for prop, meta, seqno in patch:
            diff[prop] = meta
            out_seq.include(seqno, seqno)
        if diff:
            yield {'guid': guid, 'diff': diff}


if __name__ == '__main__':
    tests.main()
