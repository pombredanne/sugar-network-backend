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
from active_document.metadata import StoredProperty


class IndexProxyTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class Document(document.Document):

            @document.active_property(slot=1, prefix='A')
            def term(self, value):
                return value

            @document.active_property(slot=2, prefix='B', construct_only=True)
            def not_term(self, value):
                return value

            @document.active_property(slot=3, prefix='C')
            def common(self, value):
                return value

        env.index_write_queue.value = 100
        env.index_flush_threshold.value = 100
        env.index_flush_timeout.value = 0

        Document.init()
        self.metadata = Document.metadata
        index_queue.init([Document])

        doc = Document(term='1_term', not_term='1_not_term', common='common')
        self.guid_1 = doc.guid
        doc.post()

        doc = Document(term='2_term', not_term='2_not_term', common='common')
        self.guid_2 = doc.guid
        doc.post()

        def waiter():
            index_queue.wait_commit('document')

        wait_job = gevent.spawn(waiter)
        index_queue.commit('document')
        gevent.joinall([wait_job])

        self.committed = []

        def waiter():
            index_queue.wait_commit('document')
            committed.append(True)

        gevent.spawn(waiter)

    def tearDown(self):
        assert not self.committed
        tests.Test.tearDown(self)
        index_queue.close()

    def test_Create(self):
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

        proxy.store('3', {'term': '3_term', 'not_term': '3_not_term'}, True)
        proxy.store('4', {'term': '4_term', 'not_term': '4_not_term'}, True)

        self.assertEqual(
                sorted([
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    {'guid': '3', 'term': '3_term', 'not_term': '3_not_term'},
                    {'guid': '4', 'term': '4_term', 'not_term': '4_not_term'},
                    ]),
                proxy._find(1, 4)[0])

        self.assertEqual(
                sorted([
                    {'guid': '3', 'term': '3_term', 'not_term': '3_not_term'},
                    ]),
                proxy._find(request={'term': '3_term'})[0])

        proxy.store('3', {'term': '3_term_2'}, False)

        self.assertEqual(
                sorted([
                    {'guid': '3', 'term': '3_term_2', 'not_term': '3_not_term'},
                    ]),
                proxy._find(request={'term': '3_term_2'})[0])

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    {'guid': '3', 'term': '3_term_2', 'not_term': '3_not_term'},
                    {'guid': '4', 'term': '4_term', 'not_term': '4_not_term'},
                    ]),
                proxy._find()[0])

    def test_Update(self):
        proxy = IndexProxy(self.metadata)

        proxy.store(self.guid_1, {'term': '1_term_2'}, False)
        proxy.store(self.guid_2, {'term': '2_term_2'}, False)

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': '1_term_2', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term_2', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

    def test_Update_Adds(self):
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                sorted([
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        proxy.store(self.guid_1, {'term': 'foo'}, False)
        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo'},
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        proxy.store(self.guid_2, {'term': 'foo'}, False)
        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo'},
                    {'guid': self.guid_2, 'term': 'foo'},
                    ]),
                proxy._find(request={'term': 'foo'})[0])

        self.assertEqual(
                sorted([
                    {'guid': self.guid_1, 'term': 'foo', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': 'foo', 'not_term': '2_not_term', 'common': 'common'},
                    ]),
                proxy._find()[0])

    def test_Update_Deletes(self):
        proxy = IndexProxy(self.metadata)

        self.assertEqual(
                (sorted([
                    {'guid': self.guid_1, 'term': '1_term', 'not_term': '1_not_term', 'common': 'common'},
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]), 2),
                proxy._find(request={'common': 'common'}))

        proxy.store(self.guid_1, {'common': '1_common'}, False)
        self.assertEqual(
                (sorted([
                    {'guid': self.guid_2, 'term': '2_term', 'not_term': '2_not_term', 'common': 'common'},
                    ]), 1),
                proxy._find(request={'common': 'common'}))

        proxy.store(self.guid_2, {'common': '2_common'}, False)
        self.assertEqual(
                (sorted([
                    ]), 0),
                proxy._find(request={'common': 'common'}))


class IndexProxy(index_proxy.IndexProxy):

    def _find(self, offset=0, limit=100, request=None):
        if request is None:
            request = {}
        documents, total = self.find(offset, limit, request,
                reply=['guid', 'term', 'not_term', 'common'])
        result = []
        for guid, props in documents:
            props['guid'] = guid
            result.append(props)
        return sorted(result), total


if __name__ == '__main__':
    tests.main()
