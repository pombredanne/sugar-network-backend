#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import shutil
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
            pass

        Document.init(IndexProxy)
        index_queue.start([Document])
        self.Document = Document

    def tearDown(self):
        index_queue.stop()
        tests.Test.tearDown(self)

    def test_put(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 0

        put = []
        index_queue.put('document', lambda *args: put.append(1))
        index_queue.put('document', lambda *args: put.append(2))
        index_queue.put('document', lambda *args: put.append(3))

        self.assertEqual([], put)
        self.assertEqual(0, index_queue.commit_seqno('document'))
        index_queue.commit_and_wait('document')
        self.assertEqual([1, 2, 3], put)
        self.assertEqual(1, index_queue.commit_seqno('document'))
        index_queue.commit_and_wait('document')
        self.assertEqual(1, index_queue.commit_seqno('document'))


if __name__ == '__main__':
    tests.main()
