#!/usr/bin/env python
# sugar-lint: disable

import time
import logging
import threading

import gevent
from gevent.event import Event

from __init__ import tests

from active_document import env
from active_document import index_queue, document
from active_document.document_class import active_property
from active_document.index_proxy import IndexProxy
from active_document.index import IndexReader, Total, IndexWriter
from active_document.storage import Storage


class IndexProxyTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class Document(document.Document):

            @active_property(slot=1, prefix='A')
            def term(self, value):
                return value

            @active_property(slot=2, prefix='B',
                    permissions=env.ACCESS_CREATE | env.ACCESS_READ)
            def not_term(self, value):
                return value

        Document.init(TestIndexProxy)
        self.metadata = Document.metadata

        self.override(index_queue, 'put', lambda *args: 1)
        self.override(index_queue, 'commit_seqno', lambda *args: 0)
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))

        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 0

    def test_Create(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'q', 'not_term': 'w', 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'term': 'a', 'not_term': 's', 'layers': ['public'], 'author': ['me']}),
            ], Total(2))

        proxy = TestIndexProxy(self.metadata)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    ]),
                proxy.find_())

        proxy.store('3', {'guid': '3', 'term': 'a', 'not_term': 's'}, True)
        proxy.store('4', {'guid': '4', 'term': 'z', 'not_term': 'x'}, True)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    {'guid': '3', 'term': 'a', 'not_term': 's'},
                    {'guid': '4', 'term': 'z', 'not_term': 'x'},
                    ]),
                proxy.find_())

        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '4', 'term': 'z', 'not_term': 'x',},
                    ]),
                proxy.find_(guid='4'))

        self.override(IndexReader, 'find', lambda *args: ([existing[0][1]], Total(1)))
        self.assertEqual(
                sorted([
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    {'guid': '3', 'term': 'a', 'not_term': 's'},
                    ]),
                proxy.find_(term='a'))

        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '4', 'term': 'z', 'not_term': 'x'},
                    ]),
                proxy.find_(term='z'))

        self.override(IndexReader, 'find', lambda *args: ([existing[0][0]], Total(1)))
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    ]),
                proxy.find_(term='q'))

        proxy.store('3', {'guid': '3', 'term': 'aa', 'not_term': 's'}, True)

        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '3', 'term': 'aa', 'not_term': 's'},
                    ]),
                proxy.find_(term='aa'))

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    {'guid': '3', 'term': 'aa', 'not_term': 's'},
                    {'guid': '4', 'term': 'z', 'not_term': 'x'},
                    ]),
                proxy.find_())

    def test_Create_FindForNotCreatedDB(self):
        proxy = TestIndexProxy(self.metadata)
        proxy.store('1', {'guid': '1', 'term': 'a', 'not_term': 's'}, True)

        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'a', 'not_term': 's'},
                    ]),
                proxy.find_())

    def test_Update(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'q', 'not_term': 'w', 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'term': 'a', 'not_term': 's', 'layers': ['public'], 'author': ['me']}),
            ], Total(2))
        self.override(IndexReader, 'find', lambda *args: existing)

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])

        proxy = TestIndexProxy(self.metadata)

        proxy.store('1', {'guid': '1', 'term': 'qq', 'not_term': 'ww'}, False)
        proxy.store('2', {'guid': '2', 'term': 'aa', 'not_term': 'ss'}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'qq', 'not_term': 'ww'},
                    {'guid': '2', 'term': 'aa', 'not_term': 'ss'},
                    ]),
                proxy.find_())

    def test_Update_Adds(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'q', 'not_term': 'w', 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'term': 'a', 'not_term': 's', 'layers': ['public'], 'author': ['me']}),
            ], Total(2))
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])

        proxy = TestIndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(term='foo'))

        proxy.store('1', {'guid': '1', 'term': 'foo', 'not_term': 'w'}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'foo', 'not_term': 'w'},
                    ]),
                proxy.find_(term='foo'))

        proxy.store('2', {'guid': '2', 'term': 'foo', 'not_term': 's'}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'foo', 'not_term': 'w'},
                    {'guid': '2', 'term': 'foo', 'not_term': 's'},
                    ]),
                proxy.find_(term='foo'))

    def test_Update_Deletes(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'orig', 'not_term': '', 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'term': 'orig', 'not_term': '', 'layers': ['public'], 'author': ['me']}),
            ], Total(2))
        self.override(IndexReader, 'find', lambda *args: existing)

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])

        proxy = TestIndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': ''},
                    {'guid': '2', 'term': 'orig', 'not_term': ''},
                    ]),
                proxy.find_(term='orig'))

        proxy.store('1', {'guid': '1', 'term': '', 'not_term': ''}, False)

        self.assertEqual(
                sorted([
                    {'guid': '2', 'term': 'orig', 'not_term': ''},
                    ]),
                proxy.find_(term='orig'))

        proxy.store('2', {'guid': '2', 'term': '', 'not_term': ''}, False)

        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(term='orig'))

    def test_get_cached(self):
        existing = ([
            ('1', {'guid': '1', 'term': 'orig', 'not_term': '', 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'term': 'orig', 'not_term': '', 'layers': ['public'], 'author': ['me']}),
            ], Total(2))
        self.override(IndexReader, 'find', lambda *args: existing)

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])

        proxy = TestIndexProxy(self.metadata)
        self.assertEqual({}, proxy.get_cached('1'))

        proxy.store('1', {'guid': '1', 'term': 'new', 'not_term': 'new'}, False)
        self.assertEqual({'guid': '1', 'term': 'new', 'not_term': 'new'}, proxy.get_cached('1'))

        proxy.store('3', {'guid': '3', 'term': 'z', 'not_term': 'x'}, True)
        self.assertEqual({'guid': '3', 'term': 'z', 'not_term': 'x'}, proxy.get_cached('3'))

    def test_FindByListProps(self):

        class Document(document.Document):

            @active_property(prefix='A', typecast=[])
            def prop(self, value):
                return value

        Document.init(TestIndexProxy)
        proxy = TestIndexProxy(Document.metadata)

        proxy.store('1', {'guid': '1', 'prop': ('a',)}, True)
        proxy.store('2', {'guid': '2', 'prop': ('a', 'aa')}, True)
        proxy.store('3', {'guid': '3', 'prop': ('aa', 'aaa')}, True)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    {'guid': '2', 'prop': ('a', 'aa')},
                    ]),
                proxy.find_(prop='a'))
        self.assertEqual(
                sorted([
                    {'guid': '2', 'prop': ('a', 'aa')},
                    {'guid': '3', 'prop': ('aa', 'aaa')},
                    ]),
                proxy.find_(prop='aa'))
        self.assertEqual(
                sorted([
                    {'guid': '3', 'prop': ('aa', 'aaa')},
                    ]),
                proxy.find_(prop='aaa'))

    def test_Update_AddsByListProps(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'prop': (), 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'prop': (), 'layers': ['public'], 'author': ['me']}),
            ], Total(2))

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])

        class Document(document.Document):

            @active_property(prefix='A', typecast=[])
            def prop(self, value):
                return value

        Document.init(TestIndexProxy)
        proxy = TestIndexProxy(Document.metadata)

        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(prop='a'))

        proxy.store('1', {'guid': '1', 'prop': ('a',)}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    ]),
                proxy.find_(prop='a'))

        proxy.store('2', {'guid': '2', 'prop': ('a',)}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    {'guid': '2', 'prop': ('a',)},
                    ]),
                proxy.find_(prop='a'))

    def test_Update_DeletesByListProps(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'prop': ('a',), 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'prop': ('a', 'aa'), 'layers': ['public'], 'author': ['me']}),
            ('3', {'guid': '3', 'prop': ('a', 'aa', 'aaa'), 'layers': ['public'], 'author': ['me']}),
            ], Total(3))
        self.override(IndexReader, 'find', lambda *args: existing)

        storage = Storage(self.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])
        storage.put(*existing[0][2])

        class Document(document.Document):

            @active_property(prefix='A', typecast=[])
            def prop(self, value):
                return value

        Document.init(TestIndexProxy)
        proxy = TestIndexProxy(Document.metadata)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    {'guid': '2', 'prop': ('a', 'aa')},
                    {'guid': '3', 'prop': ('a', 'aa', 'aaa')},
                    ]),
                proxy.find_(prop='a'))

        proxy.store('2', {'guid': '2', 'prop': ('aa',)}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    {'guid': '3', 'prop': ('a', 'aa', 'aaa')},
                    ]),
                proxy.find_(prop='a'))

        proxy.store('3', {'guid': '3', 'prop': ('aaa',)}, False)

        self.assertEqual(
                sorted([
                    {'guid': '1', 'prop': ('a',)},
                    ]),
                proxy.find_(prop='a'))

        proxy.store('1', {'guid': '1', 'prop': ()}, False)

        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(prop='a'))

    def test_SeamlessCache_Create(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'orig', 'not_term': 'a', 'layers': ['public'], 'author': ['me']}),
            ], Total(1))
        storage = Storage(self.metadata)
        storage.put(*existing[0][0])

        proxy = TestIndexProxy(self.metadata)

        self.override(index_queue, 'put', lambda *args: 2)
        proxy.store('2', {'guid': '2', 'term': 'orig', 'not_term': 'b'}, True)
        self.assertEqual(2, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 3)
        proxy.store('3', {'guid': '3', 'term': 'orig', 'not_term': 'c'}, True)
        self.assertEqual(3, len(proxy._pages))

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    {'guid': '2', 'term': 'orig', 'not_term': 'b'},
                    {'guid': '3', 'term': 'orig', 'not_term': 'c'},
                    ]),
                proxy.find_())
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    {'guid': '2', 'term': 'orig', 'not_term': 'b'},
                    {'guid': '3', 'term': 'orig', 'not_term': 'c'},
                    ]),
                proxy.find_(term='orig'))
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(term='new'))

        proxy.store('2', {'guid': '2', 'term': 'new', 'not_term': 'b'}, False)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    {'guid': '3', 'term': 'orig', 'not_term': 'c'},
                    ]),
                proxy.find_())
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    {'guid': '3', 'term': 'orig', 'not_term': 'c'},
                    ]),
                proxy.find_(term='orig'))
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    ]),
                proxy.find_(term='new'))

        proxy.store('3', {'guid': '3', 'term': 'new', 'not_term': 'c'}, False)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    {'guid': '3', 'term': 'new', 'not_term': 'c'},
                    ]),
                proxy.find_())
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'orig', 'not_term': 'a'},
                    ]),
                proxy.find_(term='orig'))
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    {'guid': '3', 'term': 'new', 'not_term': 'c'},
                    ]),
                proxy.find_(term='new'))

        proxy.store('1', {'guid': '1', 'term': 'new', 'not_term': 'a'}, False)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'new', 'not_term': 'a'},
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    {'guid': '3', 'term': 'new', 'not_term': 'c'},
                    ]),
                proxy.find_())
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(term='orig'))
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'new', 'not_term': 'a'},
                    {'guid': '2', 'term': 'new', 'not_term': 'b'},
                    {'guid': '3', 'term': 'new', 'not_term': 'c'},
                    ]),
                proxy.find_(term='new'))

    def test_SeamlessCache_WithRequest(self):
        existing = ([
            ('1', {'guid': '1', 'prop': ('a',), 'layers': ['public'], 'author': ['me']}),
            ('2', {'guid': '2', 'prop': ('a',), 'layers': ['public'], 'author': ['me']}),
            ('3', {'guid': '3', 'prop': ('a',), 'layers': ['public'], 'author': ['me']}),
            ], Total(3))
        self.override(IndexReader, 'find', lambda *args: existing)

        class Document(document.Document):

            @active_property(prefix='A', typecast=[])
            def prop(self, value):
                return value

        Document.init(TestIndexProxy)
        IndexWriter(Document.metadata).close()

        storage = Storage(Document.metadata)
        storage.put(*existing[0][0])
        storage.put(*existing[0][1])
        storage.put(*existing[0][2])

        proxy = TestIndexProxy(Document.metadata)

        self.override(index_queue, 'put', lambda *args: 2)
        proxy.store('1', {'guid': '1', 'prop': ('aa',)}, False)
        self.assertEqual(2, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 3)
        proxy.store('2', {'guid': '2', 'prop': ('aa',)}, False)
        self.assertEqual(3, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 4)
        proxy.store('3', {'guid': '3', 'prop': ('aa',)}, False)
        self.assertEqual(4, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 5)
        proxy.store('4', {'guid': '4', 'prop': ('a',)}, True)
        self.assertEqual(5, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 6)
        proxy.store('5', {'guid': '5', 'prop': ('a',)}, True)
        self.assertEqual(6, len(proxy._pages))

        self.override(index_queue, 'put', lambda *args: 7)
        proxy.store('6', {'guid': '6', 'prop': ('a',)}, True)
        self.assertEqual(7, len(proxy._pages))

        self.assertEqual(
                sorted([
                    {'guid': '4', 'prop': ('a',)},
                    {'guid': '5', 'prop': ('a',)},
                    {'guid': '6', 'prop': ('a',)},
                    ]),
                proxy.find_(prop=('a',)))

    def test_DropPages(self):
        IndexWriter(self.metadata).close()

        proxy = TestIndexProxy(self.metadata)

        self.override(index_queue, 'put', lambda *args: 2)
        proxy.store('1', {'guid': '1', 'term': 'q', 'not_term': 'w'}, True)
        self.override(index_queue, 'put', lambda *args: 3)
        proxy.store('2', {'guid': '2', 'term': 'a', 'not_term': 's'}, True)
        self.override(index_queue, 'put', lambda *args: 4)
        proxy.store('3', {'guid': '3', 'term': 'z', 'not_term': 'x'}, True)
        self.override(index_queue, 'put', lambda *args: 5)
        proxy.store('4', {'guid': '4', 'term': ' ', 'not_term': ' '}, True)
        self.assertEqual(5, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 0)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    {'guid': '3', 'term': 'z', 'not_term': 'x'},
                    {'guid': '4', 'term': ' ', 'not_term': ' '},
                    ]),
                proxy.find_())
        self.assertEqual(5, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 1)
        self.assertEqual(
                sorted([
                    {'guid': '2', 'term': 'a', 'not_term': 's'},
                    {'guid': '3', 'term': 'z', 'not_term': 'x'},
                    {'guid': '4', 'term': ' ', 'not_term': ' '},
                    ]),
                proxy.find_())
        self.assertEqual(4, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 3)
        self.assertEqual(
                sorted([
                    {'guid': '4', 'term': ' ', 'not_term': ' '},
                    ]),
                proxy.find_())
        self.assertEqual(2, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 4)
        self.assertEqual(
                sorted([
                    ]),
                proxy.find_())
        self.assertEqual(1, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 5)
        self.assertEqual(
                sorted([
                    ]),
                proxy.find_())
        self.assertEqual(0, len(proxy._pages))

    def test_NoCache(self):
        IndexWriter(self.metadata).close()

        existing = ([
            ('1', {'guid': '1', 'term': 'q', 'not_term': 'w', 'layers': ['public'], 'author': ['me']}),
            ], Total(1))

        proxy = TestIndexProxy(self.metadata)

        proxy.store('2', {'guid': '2', 'term': ' ', 'not_term': ' '}, True)

        self.override(IndexReader, 'find', lambda *args: existing)
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    {'guid': '2', 'term': ' ', 'not_term': ' '},
                    ]),
                proxy.find_())
        self.assertEqual(
                sorted([
                    {'guid': '1', 'term': 'q', 'not_term': 'w'},
                    ]),
                proxy.find_(no_cache=True))
        self.override(IndexReader, 'find', lambda *args: ([], Total(0)))
        self.assertEqual(
                sorted([
                    ]),
                proxy.find_(no_cache=True))

    def test_SetSeqnoOnInitialOpen(self):
        IndexWriter(self.metadata).close()

        proxy = TestIndexProxy(self.metadata)

        self.assertEqual(0, proxy._commit_seqno)
        self.override(index_queue, 'commit_seqno', lambda *args: 5)
        proxy.find_()
        self.assertEqual(5, proxy._commit_seqno)
        self.assertEqual({}, proxy.get_cached('fake'))
        self.assertEqual(5, proxy._commit_seqno)

    def test_NotFailOnEmptyCache(self):
        IndexWriter(self.metadata).close()
        proxy = TestIndexProxy(self.metadata)
        self.assertEqual(0, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 10)
        proxy.find_()
        self.assertEqual(0, len(proxy._pages))

        proxy.store('1', {'guid': '1', 'term': 'q', 'not_term': 'w'}, True)
        proxy.find_()
        self.assertEqual(1, len(proxy._pages))

    def test_get_cached_DropPages(self):
        proxy = TestIndexProxy(self.metadata)

        proxy.store('1', {'guid': '1', 'term': 'z', 'not_term': 'x'}, True)
        self.assertEqual(1, len(proxy._pages))

        self.override(index_queue, 'commit_seqno', lambda *args: 10)
        self.assertEqual({}, proxy.get_cached('0'))
        self.assertEqual(0, len(proxy._pages))


class TestIndexProxy(IndexProxy):

    def find_(self, *args, **kwargs):
        query = env.Query(*args, **kwargs)

        result = []
        for __, props in self.find(query)[0]:
            if 'layers' in props:
                props.pop('layers')
            if 'author' in props:
                props.pop('author')
            result.append(props)
        return sorted(result)

        return sorted([props for __, props in self.find(query)[0]])


if __name__ == '__main__':
    tests.main()
