#!/usr/bin/env python
# sugar-lint: disable

import time
import logging
import threading

import gevent
from gevent.event import Event

from __init__ import tests

from active_document import env
from active_document import index_queue, document, index_proxy
from active_document.document_class import active_property
from active_document.metadata import StoredProperty
from active_document.index_proxy import IndexProxy


class IndexProxyTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.Document = None

    def setup_document(self):

        class Document(document.Document):

            @active_property(slot=1, prefix='A')
            def term(self, value):
                return value

            @active_property(slot=2, prefix='B',
                    permissions=env.ACCESS_CREATE | env.ACCESS_READ)
            def not_term(self, value):
                return value

            @active_property(slot=3, prefix='C')
            def common(self, value):
                return value

        env.index_flush_threshold.value = 100
        env.index_flush_timeout.value = 0

        self.Document = Document
        Document.init(IndexProxy)
        self.metadata = Document.metadata
        index_queue.init([Document])

        self.doc_1 = Document(term='1_term', not_term='1_not_term', common='common')
        self.guid_1 = self.doc_1.guid
        self.doc_1.post()

        self.doc_2 = Document(term='2_term', not_term='2_not_term', common='common')
        self.guid_2 = self.doc_2.guid
        self.doc_2.post()

        def waiter():
            index_queue.wait_commit('document')

        wait_job = gevent.spawn(waiter)
        index_queue.commit('document')
        gevent.joinall([wait_job])

        self.committed = []

        def waiter():
            index_queue.wait_commit('document')
            self.committed.append(True)

        self.wait_job = gevent.spawn(waiter)

    def tearDown(self):
        if self.Document is not None:
            assert not self.committed
            self.wait_job.kill()
            self.Document.close()
        tests.Test.tearDown(self)

    def test_Create(self):
        self.setup_document()
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

        proxy.store('3', {'ctime': 1, 'mtime': 1, 'term': '3_term', 'not_term': '3_not_term', 'common': '', 'seqno': 0}, True)
        proxy.store('4', {'ctime': 1, 'mtime': 1, 'term': '4_term', 'not_term': '4_not_term', 'common': '', 'seqno': 0}, True)

        self.assertEqual(
                sorted([
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    {'guid': '3', 'term': '3_term', 'not_term': '3_not_term', 'common': ''},
                    {'guid': '4', 'term': '4_term', 'not_term': '4_not_term', 'common': ''},
                    ]),
                proxy._find(1, 4)[0])
        self.assertEqual(
                sorted([
                    {'guid': '3', 'term': '3_term', 'not_term': '3_not_term', 'common': ''},
                    ]),
                proxy._find(request={'term': '3_term'})[0])
        self.assertEqual(
                sorted([
                    {'guid': '4', 'term': '4_term', 'not_term': '4_not_term', 'common': ''},
                    ]),
                proxy._find(request={'guid': '4'})[0])

        proxy.store('3', {'ctime': 1, 'mtime': 1, 'term': '3_term_2', 'not_term': '3_not_term', 'common': '', 'seqno': 0}, True)

        self.assertEqual(
                sorted([
                    {'guid': '3', 'term': '3_term_2', 'not_term': '3_not_term', 'common': ''},
                    ]),
                proxy._find(request={'term': '3_term_2'})[0])

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    {'guid': '3', 'term': '3_term_2', 'not_term': '3_not_term', 'common': ''},
                    {'guid': '4', 'term': '4_term', 'not_term': '4_not_term', 'common': ''},
                    ]),
                proxy._find()[0])

    def test_Create_FindForNotCreatedDB(self):

        class Document2(document.Document):
            pass

        index_queue.close()
        Document2.init(IndexProxy)
        index_queue.init([Document2])

        proxy = IndexProxy(Document2.metadata)
        proxy.store('1', {'ctime': 1, 'mtime': 1, 'seqno': 0}, True)
        self.assertEqual(
                [{'guid': '1'}],
                proxy._find()[0])

    def test_Update(self):
        self.setup_document()
        proxy = IndexProxy(self.metadata)

        proxy._store(self.doc_1, {'term': '1_term_2'})
        proxy._store(self.doc_2, {'term': '2_term_2'})

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term_2', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term_2', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

    def test_Update_Adds(self):
        self.setup_document()
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        proxy._store(self.doc_1, {'term': 'foo'})
        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo', 'not_term': '1_not_term', 'common': 'common'},
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        proxy._store(self.doc_2, {'term': 'foo'})
        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': 'foo', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': 'foo', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

    def test_Update_Deletes(self):
        self.setup_document()
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                (sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]), 2),
                proxy._find(request={'common': 'common'}))

        proxy._store(self.doc_1, {'common': '1_common'})
        self.assertEqual(
                (sorted([
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]), 1),
                proxy._find(request={'common': 'common'}))

        proxy._store(self.doc_2, {'common': '2_common'})
        self.assertEqual(
                (sorted([
                    ]), 0),
                proxy._find(request={'common': 'common'}))

    def test_Get(self):
        self.setup_document()
        doc = self.Document(term='3', not_term='3', common='3')
        doc.post()

        doc_2 = self.Document(doc.guid)
        self.assertEqual('3', doc_2.term)
        self.assertEqual('3', doc_2.not_term)
        self.assertEqual('3', doc_2.common)

    def test_Document_merge(self):
        self.setup_document()
        ts = int(time.time())

        self.Document.merge(self.doc_1.guid, {
            'term': ('2', ts + 60),
            'not_term': ('2', ts + 60),
            'common': ('2', ts + 60),
            })
        self.Document.merge(self.doc_2.guid, {
            'term': ('3', ts + 60),
            'not_term': ('3', ts + 60),
            'common': ('3', ts + 60),
            })
        self.Document.merge('1', {
            'guid': ('1', 1),
            'term': ('1', 1),
            'not_term': ('1', 1),
            'common': ('1', 1),
            'ctime': (1, 1),
            })
        self.Document.merge('4', {
            'guid': ('4', ts + 60),
            'term': ('4', ts + 60),
            'not_term': ('4', ts + 60),
            'common': ('4', ts + 60),
            'ctime': (ts + 60, ts + 60),
            })

        self.assertEqual(
                [(self.doc_1.guid, '2', self.doc_1.ctime),
                    (self.doc_2.guid, '3', self.doc_2.ctime),
                    ],
                [(i.guid, i.term, i.ctime) for i in self.Document.find(0, 100)[0]])

        def waiter():
            index_queue.wait_commit('document')

        wait_job = gevent.spawn(waiter)
        index_queue.commit('document')
        gevent.joinall([wait_job])

        self.assertEqual(
                [('1', '1', 1),
                    (self.doc_1.guid, '2', self.doc_1.ctime),
                    (self.doc_2.guid, '3', self.doc_2.ctime),
                    ('4', '4', ts + 60),
                    ],
                [(i.guid, i.term, i.ctime) for i in self.Document.find(0, 100)[0]])

        del self.committed[:]

    def test_FindByListProps(self):

        class Document2(document.Document):

            @active_property(prefix='A', typecast=[])
            def prop(self, value):
                return value

        index_queue.close()
        Document2.init(IndexProxy)
        index_queue.init([Document2])
        proxy = IndexProxy(Document2.metadata)

        proxy.store('1', {'ctime': 0, 'mtime': 0, 'seqno': 0, 'prop': ('a',)}, True)
        proxy.store('2', {'ctime': 0, 'mtime': 0, 'seqno': 0, 'prop': ('a', 'aa')}, True)
        proxy.store('3', {'ctime': 0, 'mtime': 0, 'seqno': 0, 'prop': ('aa', 'aaa')}, True)

        self.assertEqual(
                ['1', '2'],
                [i['guid'] for i in proxy._find(request={'prop': 'a'})[0]])
        self.assertEqual(
                ['2', '3'],
                [i['guid'] for i in proxy._find(request={'prop': 'aa'})[0]])
        self.assertEqual(
                ['3'],
                [i['guid'] for i in proxy._find(request={'prop': 'aaa'})[0]])


class IndexProxy(index_proxy.IndexProxy):

    def _find(self, offset=0, limit=100, request=None):
        if request is None:
            request = {}
        documents, total = self.find(offset, limit, request,
                reply=['guid', 'term', 'not_term', 'common'])
        result = []
        for guid, props in documents:
            props['guid'] = guid
            if 'ctime' in props:
                del props['ctime']
            if 'mtime' in props:
                del props['mtime']
            if 'seqno' in props:
                del props['seqno']
            result.append(props)
        return sorted(result), total

    def _store(self, doc, update):
        props = {'ctime': doc.ctime,
                 'mtime': doc.mtime,
                 'term': doc.term,
                 'not_term': doc.not_term,
                 'common': doc.common,
                 'seqno': 0,
                 }
        props.update(update)
        self.store(doc.guid, props, False)


if __name__ == '__main__':
    tests.main()
