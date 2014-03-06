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
from sugar_network.toolkit import http


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

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
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

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter, _SessionSeqno())

    def test_ActiveProperty_Terms(self):

        class Document(db.Resource):

            @db.indexed_property(prefix='T')
            def term(self, value):
                return value

            @db.stored_property()
            def not_term(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
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

        self.assertRaises(RuntimeError, Directory, tests.tmpdir, Document, IndexWriter, _SessionSeqno())

    def test_ActiveProperty_FullTextSearch(self):

        class Document(db.Resource):

            @db.indexed_property(full_text=False, slot=1)
            def no(self, value):
                return value

            @db.indexed_property(full_text=True, slot=2)
            def yes(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
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

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

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

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

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
                ('db/document/1/1/guid', '{"value": "1"}'),
                ('db/document/1/1/ctime', '{"value": 1}'),
                ('db/document/1/1/mtime', '{"value": 1}'),
                ('db/document/1/1/prop', '{"value": "prop-1"}'),
                ('db/document/1/1/seqno', '{"value": 0}'),

                ('db/document/2/2/guid', '{"value": "2"}'),
                ('db/document/2/2/ctime', '{"value": 2}'),
                ('db/document/2/2/mtime', '{"value": 2}'),
                ('db/document/2/2/prop', '{"value": "prop-2"}'),
                ('db/document/2/2/seqno', '{"value": 0}'),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

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
                ('db/document/1/1/guid', '{"value": "1"}'),
                ('db/document/1/1/ctime', '{"value": 1}'),
                ('db/document/1/1/mtime', '{"value": 1}'),
                ('db/document/1/1/prop', '{"value": "prop-1"}'),
                ('db/document/1/1/seqno', '{"value": 0}'),

                ('db/document/2/2/guid', '{"value": "2"}'),
                ('db/document/2/2/ctime', ''),
                ('db/document/2/2/mtime', '{"value": 2}'),
                ('db/document/2/2/prop', '{"value": "prop-2"}'),
                ('db/document/2/2/seqno', '{"value": 0}'),

                ('db/document/3/3/guid', ''),
                ('db/document/3/3/ctime', ''),
                ('db/document/3/3/mtime', ''),
                ('db/document/3/3/prop', ''),
                ('db/document/3/3/seqno', ''),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

        populated = 0
        for i in directory.populate():
            populated += 1
        self.assertEqual(1, populated)
        self.assertEqual(
                sorted(['1']),
                sorted([i.guid for i in directory.find()[0]]))
        assert exists('db/document/1/1/guid')
        assert not exists('db/document/2/2/guid')
        assert not exists('db/document/3/3/guid')

    def test_create_with_guid(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

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

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())

        guid_1 = directory.create({'prop': 'value'})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(1, seqno)
        self.assertEqual(
                json.load(file('db/document/%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('db/document/%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

        guid_2 = directory.create({'prop': 'value'})
        seqno = directory.get(guid_2).get('seqno')
        self.assertEqual(2, seqno)
        self.assertEqual(
                json.load(file('db/document/%s/%s/guid' % (guid_2[:2], guid_2)))['seqno'],
                seqno)
        self.assertEqual(
                json.load(file('db/document/%s/%s/prop' % (guid_2[:2], guid_2)))['seqno'],
                seqno)

        directory.update(guid_1, {'prop': 'new'})
        seqno = directory.get(guid_1).get('seqno')
        self.assertEqual(3, seqno)
        self.assertEqual(
                json.load(file('db/document/%s/%s/guid' % (guid_1[:2], guid_1)))['seqno'],
                1)
        self.assertEqual(
                json.load(file('db/document/%s/%s/prop' % (guid_1[:2], guid_1)))['seqno'],
                seqno)

    def test_format_patch(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2)
            def prop2(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
        guid = directory.create({'guid': '1', 'prop1': '1', 'prop2': '2'})
        doc = directory.get(guid)

        self.assertEqual({}, doc.format_patch({}))
        self.assertEqual({}, doc.format_patch({'prop1': '1', 'prop2': '2'}))
        self.assertEqual({'prop1': '1_'}, doc.format_patch({'prop1': '1_', 'prop2': '2'}))
        self.assertEqual({'prop1': '1_', 'prop2': '2_'}, doc.format_patch({'prop1': '1_', 'prop2': '2_'}))

    def test_format_patch_LocalizedProps(self):

        class Document(db.Resource):

            @db.indexed_property(db.Localized, slot=1)
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
        guid = directory.create({'guid': '1', 'prop': {'ru': 'ru'}})
        doc = directory.get(guid)

        self.assertEqual({}, doc.format_patch({'prop': {'ru': 'ru'}}))
        self.assertEqual({'prop': {'ru': 'ru_'}}, doc.format_patch({'prop': {'ru': 'ru_'}}))
        self.assertEqual({'prop': {'en': 'en'}}, doc.format_patch({'prop': {'en': 'en'}}))
        self.assertEqual({'prop': {'ru': 'ru', 'en': 'en'}}, doc.format_patch({'prop': {'ru': 'ru', 'en': 'en'}}))
        self.assertEqual({'prop': {'ru': 'ru_', 'en': 'en'}}, doc.format_patch({'prop': {'ru': 'ru_', 'en': 'en'}}))

    def test_wipe(self):

        class Document(db.Resource):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter, _SessionSeqno())
        guid = directory.create({'prop': '1'})
        self.assertEqual([guid], [i.guid for i in directory.find()[0]])
        directory.commit()
        assert exists('index/document')
        assert exists('db/document')

        directory.wipe()
        self.assertEqual([], [i.guid for i in directory.find()[0]])
        assert not exists('db/document')


class _SessionSeqno(object):

    def __init__(self):
        self._value = 0

    @property
    def value(self):
        return self._value

    def next(self):
        self._value += 1
        return self._value

    def commit(self):
        pass


if __name__ == '__main__':
    tests.main()
