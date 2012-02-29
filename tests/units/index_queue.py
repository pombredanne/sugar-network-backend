#!/usr/bin/env python
# sugar-lint: disable

import time
import logging
import threading

import gevent
from gevent.event import Event

from __init__ import tests

from active_document import env, index_queue, document
from active_document.document_class import active_property
from active_document.index_proxy import IndexProxy
from active_document.index import IndexWriter


class IndexQueueTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class Document(document.Document):

            populate_timeout = 0

            @active_property(slot=1, prefix='P', full_text=True)
            def prop(self, value):
                return value

            @classmethod
            def populate(cls):
                time.sleep(cls.populate_timeout)
                return []

        Document.init(IndexProxy)
        self.Document = Document

    def test_put(self):
        index_queue.init([self.Document])

        doc_1 = self.Document(prop='value_1')
        doc_1.post()

        doc_2 = self.Document(prop='value_2')
        doc_2.post()

        index_queue.close()

        db = IndexWriter(self.Document.metadata)
        documents, total = db.find(0, 10, {})
        self.assertEqual(2, total.value)
        self.assertEqual(
                sorted([(doc_1.guid, 'value_1'), (doc_2.guid, 'value_2')]),
                sorted([(guid, props['prop']) for guid, props in documents]))

    def test_wait(self):
        event = Event()

        def waiter():
            index_queue.wait_commit('document')
            event.set()

        def put():
            gevent.sleep(1)
            self.Document(prop='value').post()
            event.wait()

        index_queue.init([self.Document])
        gevent.joinall([gevent.spawn(waiter), gevent.spawn(put)])
        index_queue.close()

    def test_PutWait(self):
        self.Document.populate_timeout = 1

        def put(value):
            self.Document(prop=value).post()

        index_queue.init([self.Document])

        jobs = []
        for i in range(3):
            jobs.append(gevent.spawn(put, str(i)))

        gevent.joinall(jobs)
        index_queue.close()

    def test_FlushTimeout(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 2

        index_queue.init([self.Document])

        committed = []

        def waiter():
            index_queue.wait_commit('document')
            committed.append(True)

        gevent.spawn(waiter)
        self.Document(prop='value').post()

        gevent.sleep(1)
        self.assertEqual(0, len(committed))

        gevent.sleep(3)
        self.assertEqual(1, len(committed))

        index_queue.close()


if __name__ == '__main__':
    tests.main()
