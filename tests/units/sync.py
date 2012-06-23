#!/usr/bin/env python
# sugar-lint: disable

import os
import json
from os.path import exists, join

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

        packet = OutPacket('push', sender='node', receiver='master', sequence={})
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('pull', sender='node', receiver='master', sequence={})
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
        self.assertEqual('master', packet.header['sender'])
        self.assertEqual('node', packet.header['receiver'])
        self.assertEqual('sequence', packet.header['push_sequence'])
        self.assertEqual({'document': [[1, 3]]}, packet.header['pull_sequence'])

    def test_Master_PullPacket(self):
        master = Master('master')
        master.volume = {'document': Directory()}
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('pull',
                sender='node',
                receiver='master',
                sequence={'document': [[1, None]]})
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual([
            {'type': 'push', 'sender': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Master_LimittedPull(self):
        master = Master('master')
        master.volume = {'document': Directory(diff=['0' * 1024] * 10)}
        response = ad.Response()

        def rewind():
            request = ad.Request()
            packet = OutPacket('pull',
                    sender='node',
                    receiver='master',
                    sequence={'document': [[1, None]]})
            request.content_stream, request.content_length = packet.pop_content()
            return request

        request = rewind()
        reply = master.sync(request, response, accept_length=1024)
        self.assertEqual([
            {'type': 'push', 'sender': 'master', 'sequence': {}},
            ],
            self.read_packet(InPacket(stream=reply)))

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 2)
        self.assertEqual([
            {'type': 'push', 'sender': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            ],
            self.read_packet(InPacket(stream=reply)))

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 3)
        self.assertEqual([
            {'type': 'push', 'sender': 'master', 'sequence': {'document': [[1, 2]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            {'type': 'messages', 'document': 'document', 'guid': 2, 'diff': '0' * 1024},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Node_Export(self):
        node = Node('node', 'master')
        node.volume = {'document': Directory()}
        os.makedirs('sync')

        node.sync('sync')
        self.assertEqual([
            {'type': 'push', 'sender': 'node', 'receiver': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'},
            ],
            self.read_packets('sync'))

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
        node = Node('node', 'master')
        node.volume = {'document': Directory(diff=['0' * 100] * 5)}
        os.makedirs('sync')

        sequence = node.sync('sync', accept_length=100)
        self.assertEqual({'document': [[1, None]]}, sequence)
        self.assertEqual([], self.read_packets('sync'))

        sequence = node.sync('sync', accept_length=100, sequence=sequence)
        self.assertEqual({'document': [[1, None]]}, sequence)
        self.assertEqual([], self.read_packets('sync'))

        sequence = node.sync('sync', accept_length=200, sequence=sequence)
        self.assertEqual({'document': [[2, None]]}, sequence)
        self.assertEqual([
            {'type': 'push', 'sender': 'node', 'receiver': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

        sequence = node.sync('sync', accept_length=300, sequence=sequence)
        self.assertEqual({'document': [[4, None]]}, sequence)
        self.assertEqual([
            {'type': 'push', 'sender': 'node', 'receiver': 'master', 'sequence': {'document': [[2, 3]]}},
            {'type': 'messages', 'document': 'document', 'guid': 2, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 3, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

        sequence = node.sync('sync', sequence=sequence)
        self.assertEqual(None, sequence)
        self.assertEqual([
            {'type': 'push', 'sender': 'node', 'receiver': 'master', 'sequence': {'document': [[4, 8]]}},
            {'type': 'messages', 'document': 'document', 'guid': 4, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 5, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 6, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 7, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 8, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

    def read_packets(self, path):
        result = []
        for filename in os.listdir(path):
            with InPacket(join(path, filename)) as packet:
                result.extend(self.read_packet(packet))
        return result

    def read_packet(self, packet):
        result = [packet.header]
        result.extend([i for i in packet])
        return result


class Directory(object):

    def __init__(self, diff=None):
        self.seqno = 0
        self.merged = []
        self._diff = diff or ['diff']

    def diff(self, seq):
        seqno = seq[0][0]
        for diff in self._diff:
            yield [[seqno, seqno]], seqno, diff
            seqno += 1

    def merge(self, *args):
        self.merged.append(args)
        self.seqno += 1
        return self.seqno

    def commit(self):
        pass


if __name__ == '__main__':
    tests.main()
