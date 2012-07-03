#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import hashlib
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.sneakernet import InPacket, OutPacket
from sugar_network.node.sync import SyncCommands
from sugar_network.local.sync import NodeMount, _DEFAULT_MASTER
from sugar_network.toolkit import sneakernet


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.uuid = 0
        self.override(ad, 'uuid', self.next_uuid)

    def next_uuid(self):
        self.uuid += 1
        return str(self.uuid)

    def test_Master_MisaddressedPacket(self):
        master = SyncCommands('master')
        master.volume = Volume({})
        response = ad.Response()

        packet = OutPacket('push')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node', dst='fake')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='master', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

        packet = OutPacket('push', src='node', dst='master', sequence={})
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('pull', src='node', dst='master', sequence={})
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        master.sync(request, response)

        packet = OutPacket('fake', src='node', dst='master')
        request = ad.Request()
        request.content_stream, request.content_length = packet.pop_content()
        self.assertRaises(RuntimeError, master.sync, request, response)

    def test_Master_PushPacket(self):
        master = SyncCommands('master')
        master.volume = Volume({'document': Directory()})
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('push',
                src='node',
                dst='master',
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
        self.assertEqual('master', packet.header['src'])
        self.assertEqual('node', packet.header['dst'])
        self.assertEqual('sequence', packet.header['push_sequence'])
        self.assertEqual({'document': [[1, 3]]}, packet.header['pull_sequence'])

    def test_Master_PullPacket(self):
        master = SyncCommands('master')
        master.volume = Volume({'document': Directory()})
        request = ad.Request()
        response = ad.Response()

        packet = OutPacket('pull',
                src='node',
                dst='master',
                sequence={'document': [[1, None]]})
        request.content_stream, request.content_length = packet.pop_content()

        reply = master.sync(request, response)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Master_LimittedPull(self):
        master = SyncCommands('master')
        master.volume = Volume({'document': Directory(diff=['0' * 1024] * 10)})
        response = ad.Response()

        def rewind():
            request = ad.Request()
            packet = OutPacket('pull',
                    src='node',
                    dst='master',
                    sequence={'document': [[1, None]]})
            request.content_stream, request.content_length = packet.pop_content()
            return request

        request = rewind()
        reply = master.sync(request, response, accept_length=1024)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {}},
            ],
            self.read_packet(InPacket(stream=reply)))

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 2)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            ],
            self.read_packet(InPacket(stream=reply)))

        request = rewind()
        reply = master.sync(request, response, accept_length=1024 * 3)
        self.assertEqual([
            {'type': 'push', 'src': 'master', 'sequence': {'document': [[1, 2]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 1024},
            {'type': 'messages', 'document': 'document', 'guid': 2, 'diff': '0' * 1024},
            ],
            self.read_packet(InPacket(stream=reply)))

    def test_Node_Export(self):
        node = NodeMount(Volume({'document': Directory()}), None)

        node.sync('sync')
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': '1', 'sequence': {'document': [[1, None]]}},
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': '1', 'sequence': {'document': [[1, 1]]}},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'},
            ],
            self.read_packets('sync'))

        assert exists('push.sequence')
        self.assertEqual({'document': [[1, None]]}, json.load(file('push.sequence')))

        assert exists('pull.sequence')
        self.assertEqual({'document': [[1, None]]}, json.load(file('pull.sequence')))

    def test_Node_Export_NoPullForExistingSession(self):
        self.touch(('push.sequence', json.dumps({'document': [[1, None]]})))
        self.touch(('pull.sequence', json.dumps({'document': [[1, None]]})))
        node = NodeMount(Volume({'document': Directory()}), None)

        node.sync('sync', session='session')
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': 'session'},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': 'diff'},
            ],
            self.read_packets('sync'))

    def test_Node_Import(self):
        self.touch(('push.sequence', json.dumps({'document': [[1, None]]})))
        self.touch(('pull.sequence', json.dumps({'document': [[1, None]]})))
        node = NodeMount(Volume({'document': Directory()}), None)

        ack = OutPacket('ack', root='sync',
                src=_DEFAULT_MASTER,
                dst=tests.UID,
                push_sequence={'document': [[1, 2]]},
                pull_sequence={'document': [[3, 4]]})
        ack.close()

        other_node_ack = OutPacket('ack', root='sync',
                src=_DEFAULT_MASTER,
                dst='other',
                push_sequence={'document': [[5, 6]]},
                pull_sequence={'document': [[7, 8]]})
        other_node_ack.close()

        our_push = OutPacket('push', root='sync',
                src=tests.UID,
                dst=_DEFAULT_MASTER,
                sequence={'document': [[9, 10]]},
                session='stale')
        our_push.push_messages(document='document', items=[
            {'guid': 1, 'diff': 'diff-1'},
            ])
        our_push.close()

        master_push = OutPacket('push', root='sync',
                src=_DEFAULT_MASTER,
                sequence={'document': [[11, 12]]})
        master_push.push_messages(document='document', items=[
            {'guid': 2, 'diff': 'diff-2'},
            ])
        master_push.close()

        other_node_push = OutPacket('push', root='sync',
                src='other',
                dst=_DEFAULT_MASTER,
                sequence={'document': [[13, 14]]})
        other_node_push.push_messages(document='document', items=[
            {'guid': 3, 'diff': 'diff-3'},
            ])
        other_node_push.close()

        node.sync('sync', session='new')

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

    def test_Node_Import_DoNotDeletePacketsFromCurrentSession(self):
        node = NodeMount(Volume({'document': Directory()}), None)

        existing_push = OutPacket('push', root='sync',
                src=tests.UID,
                dst=_DEFAULT_MASTER,
                sequence={},
                session='the same')
        existing_push.close()

        self.assertEqual(1, len([i for i in sneakernet.walk('sync')]))
        node.sync('sync', session='the same')
        files = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(2, len(files))
        assert exists(existing_push.path)

        node.sync('sync', session='new one')
        new_fiels = [i.path for i in sneakernet.walk('sync')]
        self.assertEqual(1, len(new_fiels))
        assert not (set(new_fiels) & set(files))

    def test_Node_LimittedExport(self):
        node = NodeMount(Volume({'document': Directory(diff=['0' * 100] * 5)}), None)

        kwargs = node.sync('sync', accept_length=100, session=0)
        self.assertEqual(0, kwargs['session'])
        self.assertEqual({'document': [[1, None]]}, kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=100, push_sequence=kwargs['push_sequence'], session=0)
        self.assertEqual({'document': [[1, None]]}, kwargs['push_sequence'])
        self.assertEqual([], self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=200, push_sequence=kwargs['push_sequence'], session=1)
        self.assertEqual({'document': [[2, None]]}, kwargs['push_sequence'])
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': 1},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', accept_length=300, push_sequence=kwargs['push_sequence'], session=2)
        self.assertEqual({'document': [[4, None]]}, kwargs['push_sequence'])
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[2, 3]]}, 'session': 2},
            {'type': 'messages', 'document': 'document', 'guid': 2, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 3, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

        kwargs = node.sync('sync', push_sequence=kwargs['push_sequence'], session=3)
        self.assertEqual(None, kwargs)
        self.assertEqual([
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[4, 8]]}, 'session': 3},
            {'type': 'messages', 'document': 'document', 'guid': 4, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 5, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 6, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 7, 'diff': '0' * 100},
            {'type': 'messages', 'document': 'document', 'guid': 8, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

    def test_Node_sync_session(self):
        node = NodeMount(Volume({'document': Directory(diff=['0' * 100])}), None)
        node.publisher = lambda x: events.append(x)

        self.override(os, 'statvfs', lambda x: Statvfs(50))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync'},
            {'event': 'sync_continue'},
            ],
            events)
        records = self.read_packets('sync')
        self.assertEqual(1, len(records))
        self.assertEqual('pull', records[0]['type'])
        session = records[0]['session']

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync'},
            {'event': 'sync_continue'},
            ],
            events)
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': {'document': [[1, None]]}},
            ],
            self.read_packets('sync'))

        self.override(os, 'statvfs', lambda x: Statvfs(150))

        events = []
        node.sync_session(['sync'])
        self.assertEqual([
            {'path': 'sync', 'event': 'sync'},
            {'event': 'sync_complete'},
            ],
            events)
        self.assertEqual([
            {'type': 'pull', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'session': session, 'sequence': {'document': [[1, None]]}},
            {'type': 'push', 'src': tests.UID, 'dst': _DEFAULT_MASTER, 'sequence': {'document': [[1, 1]]}, 'session': session},
            {'type': 'messages', 'document': 'document', 'guid': 1, 'diff': '0' * 100},
            ],
            self.read_packets('sync'))

    def read_packets(self, path):
        result = []
        for dirname in ('pull', 'push', 'ack'):
            if not exists(join(path, dirname)):
                continue
            for filename in sorted(os.listdir(join(path, dirname))):
                with InPacket(join(path, dirname, filename)) as packet:
                    result.extend(self.read_packet(packet))
        return result

    def read_packet(self, packet):
        result = [packet.header]
        result.extend([i for i in packet])
        return result


class Volume(dict):

    def __init__(self, default):
        dict.__init__(self, default)
        self.root = '.'

    def connect(self, *args):
        pass


class Directory(object):

    def __init__(self, diff=None):
        self.seqno = 0
        self.merged = []
        self._diff = diff or ['diff']
        self.document_class = None

    def diff(self, seq, limit=None):
        seqno = seq[0][0]
        sequence = []

        def patch(seqno):
            for diff in self._diff:
                yield {'guid': seqno}, diff
                if sequence:
                    sequence[-1] = seqno
                else:
                    sequence[:] = [seqno, seqno]
                seqno += 1

        return sequence, patch(seqno)

    def merge(self, *args):
        self.merged.append(args)
        self.seqno += 1
        return self.seqno

    def commit(self):
        pass


class Statvfs(object):

    f_bfree = 0
    f_frsize = 1

    def __init__(self, f_bfree):
        self.f_bfree = f_bfree


if __name__ == '__main__':
    tests.main()
