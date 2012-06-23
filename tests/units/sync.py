#!/usr/bin/env python
# sugar-lint: disable

import os
import json
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network.node.sneakernet import InPacket, OutPacket
from sugar_network.node.sync import Master, Node
from sugar_network.node import sneakernet


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Master_MisaddressedPacket(self):
        master = Master('master')
        master.volume = {}
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('push')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', sender='node')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', receiver='master')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', sender='node', receiver='fake')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', sender='master', receiver='master')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', sender='node', receiver='master')
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('pull', sender='node', receiver='master')
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('fake', sender='node', receiver='master')
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

    def test_Master_PushPacket(self):
        master = Master('master')
        master.volume = {'document': Directory()}
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('push',
                sender='node',
                receiver='master',
                sequence='sequence')
        packet.push_messages(document='document', items=[
            {'guid': 1, 'diff': 'diff-1'},
            {'guid': 2, 'diff': 'diff-2'},
            {'guid': 3, 'diff': 'diff-3'},
            ])
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual([
            (1, 'diff-1'),
            (2, 'diff-2'),
            (3, 'diff-3'),
            ],
            master.volume['document'].merged)
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet['sender'])
        self.assertEqual('node', packet['receiver'])
        self.assertEqual('sequence', packet['push_sequence'])
        self.assertEqual({'document': [[1, 3]]}, packet['pull_sequence'])

    def test_Master_PullPacket(self):
        master = Master('master')
        master.volume = {'document': Directory()}
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('pull',
                sender='node',
                receiver='master',
                sequence={'document': 'sequence'})
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual(['sequence'], master.volume['document'].pulled)
        packet = InPacket(stream=reply)
        self.assertEqual('master', packet['sender'])
        self.assertEqual(None, packet['receiver'])
        self.assertEqual({'document': [[1, 1]]}, packet['sequence'])

    def test_Master_LimittedPull(self):
        master = Master('master')
        master.volume = {'document': Directory(diff=['0' * 1024] * 10)}
        response = ad.Response()

        def rewind():
            request = ad.Request()
            packet = OutPacket('pull',
                    sender='node',
                    receiver='master',
                    sequence={'document': 'sequence'})
            request.content_stream, request.content_length = packet.pop_content()
            master.volume['document'].pulled = []
            master.volume['document'].seqno = 0
            return request

        request = rewind()
        reply = master.sync(request, response, accept_length=1024)
        self.assertEqual([], master.volume['document'].pulled)
        packet = InPacket(stream=reply)
        self.assertEqual({}, packet['sequence'])
        self.assertEqual([
            ],
            [i for i in packet])

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 2)
        self.assertEqual(['sequence'], master.volume['document'].pulled)
        packet = InPacket(stream=reply)
        self.assertEqual({'document': [[1, 1]]}, packet['sequence'])
        self.assertEqual([
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            ],
            [i for i in packet])

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 3)
        self.assertEqual(['sequence'] * 2, master.volume['document'].pulled)
        packet = InPacket(stream=reply)
        self.assertEqual({'document': [[1, 2]]}, packet['sequence'])
        self.assertEqual([
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            {'type': 'messages', 'document': 'document', 'guid': 2, 'diff': '0' * 1024},
            ],
            [i for i in packet])

    def test_Node_Export(self):
        node = Node('node', 'master')
        node.volume = {'document': Directory()}
        os.makedirs('sync')

        node.sync('sync')
        self.assertEqual(['push-1.packet'], os.listdir('sync'))

        packet = InPacket('sync/push-1.packet')
        self.assertEqual('node', packet['sender'])
        self.assertEqual('master', packet['receiver'])
        self.assertEqual({'document': [[1, 1]]}, packet['sequence'])
        self.assertEqual(
                [{'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'}],
                [i for i in packet])

    def test_Node_Import(self):
        self.touch(('push.sequence', json.dumps({'document': [[1, None]]})))
        self.touch(('pull.sequence', json.dumps({'document': [[1, None]]})))
        node = Node('node', 'master')
        node.volume = {'document': Directory()}
        os.makedirs('sync')

        ack = OutPacket('ack', root='sync',
                sender='master',
                receiver='node',
                push_sequence={'document': [[1, 2]]},
                pull_sequence={'document': [[3, 4]]})
        ack.close()

        other_node_ack = OutPacket('ack', root='sync',
                sender='master',
                receiver='other',
                push_sequence={'document': [[5, 6]]},
                pull_sequence={'document': [[7, 8]]})
        other_node_ack.close()

        our_push = OutPacket('push', root='sync',
                sender='node',
                receiver='master',
                sequence={'document': [[9, 10]]})
        our_push.push_messages(document='document', items=[
            {'guid': 1, 'diff': 'diff-1'},
            ])
        our_push.close()

        master_push = OutPacket('push', root='sync',
                sender='master',
                sequence={'document': [[11, 12]]})
        master_push.push_messages(document='document', items=[
            {'guid': 2, 'diff': 'diff-2'},
            ])
        master_push.close()

        other_node_push = OutPacket('push', root='sync',
                sender='other',
                receiver='master',
                sequence={'document': [[13, 14]]})
        other_node_push.push_messages(document='document', items=[
            {'guid': 3, 'diff': 'diff-3'},
            ])
        other_node_push.close()

        node.sync('sync')

        assert not exists(ack.path)
        assert exists(other_node_ack.path)
        assert not exists(our_push.path)
        assert exists(master_push.path)
        assert exists(other_node_push.path)

        self.assertEqual(
                {'document': [[3, None]]},
                json.load(file('push.sequence')))
        self.assertEqual(
                {'document': [[1, 2], [5, 10], [13, None]]},
                json.load(file('pull.sequence')))
        self.assertEqual(
                sorted([
                    (2, 'diff-2'),
                    (3, 'diff-3'),
                    ]),
                sorted(node.volume['document'].merged))

    def test_Node_LimittedExport(self):
        pass


class Directory(object):

    def __init__(self, diff=None):
        self.seqno = 0
        self.merged = []
        self.pulled = []
        self._diff = diff or ['diff']

    def diff(self, seq):
        for diff in self._diff:
            self.seqno += 1
            yield [[self.seqno, self.seqno]], self.seqno, diff
            self.pulled.append(seq)

    def merge(self, *args):
        self.merged.append(args)
        self.seqno += 1
        return self.seqno

    def commit(self):
        pass


if __name__ == '__main__':
    tests.main()
