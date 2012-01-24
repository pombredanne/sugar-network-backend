#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from active_document import document
from active_document.document_class import active_property
from active_document.sync import _Timeline, Sync, NodeSeqno
from active_document.metadata import CounterProperty, BlobProperty
from active_document.metadata import AggregatorProperty


class SyncTest(tests.Test):

    def test_Timeline_flush(self):
        tl = _Timeline('tl')
        self.assertEqual(
                [[1, None]],
                tl)
        tl.append([2, 3])
        tl.flush()

        tl = _Timeline('tl')
        self.assertEqual(
                [[1, None], [2, 3]],
                tl)

    def test_Timeline_exclude(self):
        tl = _Timeline('1')
        tl.exclude(1, 10)
        self.assertEqual(
                [[11, None]],
                tl)

        tl = _Timeline('2')
        tl.exclude(5, 10)
        self.assertEqual(
                [[1, 4], [11, None]],
                tl)

        tl.exclude(2)
        self.assertEqual(
                [[1, 1], [3, 4], [11, None]],
                tl)

        tl.exclude(1)
        self.assertEqual(
                [[3, 4], [11, None]],
                tl)

        tl.exclude(3)
        self.assertEqual(
                [[4, 4], [11, None]],
                tl)

        tl.exclude(1, 20)
        self.assertEqual(
                [[21, None]],
                tl)

        tl.exclude(21, None)
        self.assertEqual(
                [[22, None]],
                tl)

    def test_NodeSeqno(self):
        seqno = NodeSeqno(None)

        times = [0.0] * 10240
        for i in range(10240):
            times[i] = seqno.next()

        for i in range(10240 - 1):
            assert times[i] != times[i + 1]

    def test_Sync_Walkthrough(self):

        class Vote(AggregatorProperty):

            @property
            def value(self):
                pass

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(CounterProperty, slot=2)
            def counter(self, value):
                return value

            @active_property(Vote, counter='counter')
            def vote(self, value):
                return value

            @vote.setter
            def vote(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        Document.init()
        sync = Sync(Document)

        doc_1 = Document(prop='1', vote=True)
        doc_1.set_blob('blob', StringIO('1'))
        doc_1.post()
        doc_2 = Document(prop='2', vote=False)
        doc_2.set_blob('blob', StringIO('2'))
        doc_2.post()
        doc_3 = Document(prop='3', vote=True)
        doc_3.post()

        for i in range(3):
            syn, patch = sync.create_syn()
            self.assertEqual(
                    [[1, None]],
                    syn)
            self.assertEqual(
                    [
                        (doc_1.get('seqno', raw=True), doc_1.guid),
                        (doc_2.get('seqno', raw=True), doc_2.guid),
                        (doc_3.get('seqno', raw=True), doc_3.guid),
                        ],
                    [(seqno, guid) for seqno, guid, diff in patch])

        doc_4 = Document(prop='4', vote=False)
        doc_4.post()

        sync.process_ack([
            ([3, 3], [doc_3.get('seqno', raw=True), doc_3.get('seqno', raw=True)]),
            ])

        for i in range(3):
            syn, patch = sync.create_syn()
            self.assertEqual(
                    [[1, 2], [4, None]],
                    syn)
            self.assertEqual(
                    [
                        (doc_1.get('seqno', raw=True), doc_1.guid),
                        (doc_2.get('seqno', raw=True), doc_2.guid),
                        (doc_4.get('seqno', raw=True), doc_4.guid),
                        ],
                    [(seqno, guid) for seqno, guid, diff in patch])

        sync.process_ack([
            ([1, 2], [doc_1.get('seqno', raw=True), doc_2.get('seqno', raw=True)]),
            ])

        for i in range(3):
            syn, patch = sync.create_syn()
            self.assertEqual(
                    [[4, None]],
                    syn)
            self.assertEqual(
                    [
                        (doc_4.get('seqno', raw=True), doc_4.guid),
                        ],
                    [(seqno, guid, ) for seqno, guid, diff in patch])

        self.assertEqual('4', Document(doc_4.guid).prop)
        self.assertEqual(False, Document(doc_4.guid).vote)
        patch = [(4, doc_4.guid, {
            'prop': ('5', doc_4.mtime - 10),
            'vote': [(('None', True), doc_4.mtime + 10)],
            })]
        sync.merge(patch)
        self.assertEqual('4', Document(doc_4.guid).prop)
        self.assertEqual(True, Document(doc_4.guid).vote)

        for i in range(3):
            syn, patch = sync.create_syn()
            self.assertEqual(
                    [[5, None]],
                    syn)
            self.assertEqual(
                    [
                        (doc_4.get('seqno', raw=True), doc_4.guid),
                        ],
                    [(seqno, guid, ) for seqno, guid, diff in patch])


if __name__ == '__main__':
    tests.main()
