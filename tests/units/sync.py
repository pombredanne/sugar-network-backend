#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import shutil
from glob import glob
from os.path import exists, join, isfile
from cStringIO import StringIO

import gevent

from __init__ import tests

from active_document import env, sync, document, util, index_queue, sneakernet
from active_document.index import IndexWriter
from active_document.document_class import active_property
from active_document.metadata import BlobProperty


class SyncTest(tests.Test):

    def test_sync_Walkthrough(self):
        with Sync(sync.Node, 'node') as node:
            doc_1 = node.Document(prop='1')
            doc_1.set_blob('blob', StringIO('1'))
            doc_1.post()

            doc_2 = node.Document(prop='2')
            doc_2.set_blob('blob', StringIO('2'))
            doc_2.post()

            node.sync('sync')
            self.assertEqual(4, node.Document.metadata.last_seqno)
            self.assertEqual(
                    [('1', '1'), ('2', '2')],
                    node.props)
            self.assertEqual(
                    [[1, None]],
                    json.load(file('node/document/send.range')))
            self.assertEqual(
                    [[1, None]],
                    json.load(file('node/document/receive.range')))

        self.assertEqual(
                sorted([
                    ('node', None, ['request', 'diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Master, 'master') as master:
            doc = master.Document(prop='3')
            doc.post()

            doc = master.Document(prop='4')
            doc.post()
            doc.set_blob('blob', StringIO('4'))

            master.Document.commit()
            self.assertEqual(3, master.Document.metadata.last_seqno)

            master.sync('sync')
            self.assertEqual(5, master.Document.metadata.last_seqno)
            self.assertEqual(
                    [('1', None), ('2', None), ('3', None), ('4', '4')],
                    master.props)

        self.assertEqual(
                sorted([
                    ('master', 'node', ['ack']),
                    ('master', None, ['diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Node, 'node') as node:
            doc = node.Document(prop='5')
            doc.post()
            doc.set_blob('blob', StringIO('5'))

            node.sync('sync')
            self.assertEqual(6, node.Document.metadata.last_seqno)
            self.assertEqual(
                    [('1', '1'), ('2', '2'), ('3', None), ('4', None), ('5', '5')],
                    node.props)
            self.assertEqual(
                    [[5, None]],
                    json.load(file('node/document/send.range')))
            self.assertEqual(
                    [[6, None]],
                    json.load(file('node/document/receive.range')))

        self.assertEqual(
                sorted([
                    ('node', None, ['request', 'diff', 'syn']),
                    ('master', None, ['diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Node, 'more_node') as node:
            node.sync('sync')
            self.assertEqual(0, node.Document.metadata.last_seqno)
            self.assertEqual(
                    [('3', None), ('4', None), ('5', None)],
                    node.props)
            self.assertEqual(
                    [[1, None]],
                    json.load(file('more_node/document/send.range')))
            self.assertEqual(
                    [[4, None]],
                    json.load(file('more_node/document/receive.range')))

        self.assertEqual(
                sorted([
                    ('more_node', None, ['request']),
                    ('node', None, ['request', 'diff', 'syn']),
                    ('master', None, ['diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Node, 'one_more_node') as node:
            doc = node.Document(prop='6')
            doc.post()
            doc.set_blob('blob', StringIO('6'))

            node.sync('sync')
            self.assertEqual(2, node.Document.metadata.last_seqno)
            self.assertEqual(
                    [('3', None), ('4', None), ('5', None), ('6', '6')],
                    node.props)
            self.assertEqual(
                    [[1, None]],
                    json.load(file('one_more_node/document/send.range')))
            self.assertEqual(
                    [[4, None]],
                    json.load(file('one_more_node/document/receive.range')))

        self.assertEqual(
                sorted([
                    ('one_more_node', None, ['request', 'diff', 'syn']),
                    ('more_node', None, ['request']),
                    ('node', None, ['request', 'diff', 'syn']),
                    ('master', None, ['diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Master, 'master') as master:
            master.sync('sync')
            self.assertEqual(7, master.Document.metadata.last_seqno)
            self.assertEqual(
                    [('1', None), ('2', None), ('3', None), ('4', '4'), ('5', None), ('6', None)],
                    master.props)

        self.assertEqual(
                sorted([
                    ('master', 'one_more_node', ['ack']),
                    ('master', 'node', ['ack']),
                    ('master', None, ['diff', 'diff', 'diff', 'diff', 'syn']),
                    ]),
                sorted(load_packets()))

        with Sync(sync.Node, 'more_node') as node:
            node.sync('sync')
            self.assertEqual(
                    [('1', None), ('2', None), ('3', None), ('4', None), ('5', None), ('6', None)],
                    node.props)
            self.assertEqual(
                    [[1, None]],
                    json.load(file('more_node/document/send.range')))
            self.assertEqual(
                    [[8, None]],
                    json.load(file('more_node/document/receive.range')))

    def test_id(self):
        node = sync.Node([])
        assert exists('id')
        self.assertNotEqual('', file('id').read().strip())

        self.touch(('id', 'foo'))
        node = sync.Node([])
        self.assertEqual('foo', file('id').read())


class Sync(object):

    def __init__(self, cls, id):

        class Document(document.Document):

            @active_property(slot=1)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        env.data_root.value = id

        if not exists('sync'):
            os.makedirs('sync')
        if not exists(id):
            os.makedirs(id)
        with file(id + '/id', 'w') as f:
            f.write(id)

        Document.init(IndexWriter)
        self._sync = cls([Document])
        self.Document = Document

    def sync(self, *args):
        self._sync.sync(*args)
        self.Document.commit()

    @property
    def props(self):
        result = []
        docs, total = self.Document.find(0, 100, order_by='prop')
        for i in docs:
            blob = i.get_blob('blob')
            if blob is not None:
                blob = blob.read()
            result.append((i.prop, blob))
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._sync.close()


def load_packets():
    packets = []
    for i in glob('sync/*.packet.gz'):
        with sneakernet._InPacket(i) as packet:
            packets.append((
                packet.header.get('sender'),
                packet.header.get('to'),
                [i.get('type') for i in packet.read_rows()] + [i['type'] for i in packet.syns],
                ))
    return packets


if __name__ == '__main__':
    tests.main()
