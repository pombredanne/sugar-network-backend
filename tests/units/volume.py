#!/usr/bin/env python
# sugar-lint: disable

import json

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit.sneakernet import InPacket, OutBufferPacket, DiskFull
from sugar_network.resources.volume import Volume, Resource, Commands
from active_toolkit import coroutine


class VolumeTest(tests.Test):

    def test_diff_Partial(self):

        class Document(ad.Document):

            @ad.active_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])

        volume['document'].create(guid='1', seqno=1, prop='*' * 1024)
        volume['document'].create(guid='2', seqno=2, prop='*' * 1024)
        volume['document'].create(guid='3', seqno=3, prop='*' * 1024)

        in_seq = Sequence([[1, None]])
        try:
            packet = OutBufferPacket(filename='packet', limit=1024 - 512)
            volume.diff(in_seq, packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([
            ],
            read_packet(packet))
        self.assertEqual([[1, None]], in_seq)

        in_seq = Sequence([[1, None]])
        try:
            packet = OutBufferPacket(filename='packet', limit=1024 + 512)
            volume.diff(in_seq, packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '1'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[1, 1]]},
            ],
            read_packet(packet))
        self.assertEqual([[2, None]], in_seq)

        in_seq = Sequence([[1, None]])
        packet = OutBufferPacket(filename='packet', limit=None)
        volume.diff(in_seq, packet)
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '1'},
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '2'},
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '3'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[1, 3]]},
            ],
            read_packet(packet))
        self.assertEqual([[4, None]], in_seq)

    def test_diff_CollapsedCommit(self):

        class Document(ad.Document):

            @ad.active_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])

        volume['document'].create(guid='2', seqno=2, prop='*' * 1024)
        volume['document'].create(guid='4', seqno=4, prop='*' * 1024)
        volume['document'].create(guid='6', seqno=6, prop='*' * 1024)
        volume['document'].create(guid='8', seqno=8, prop='*' * 1024)

        in_seq = Sequence([[1, None]])
        try:
            packet = OutBufferPacket(filename='packet', limit=1024 * 2)
            volume.diff(in_seq, packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '2'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[2, 2]]},
            ],
            read_packet(packet))
        self.assertEqual([[1, 1], [3, None]], in_seq)

        try:
            packet = OutBufferPacket(filename='packet', limit=1024 * 2)
            volume.diff(in_seq, packet)
            assert False
        except DiskFull:
            pass
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '4'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[4, 4]]},
            ],
            read_packet(packet))
        self.assertEqual([[1, 1], [3, 3], [5, None]], in_seq)

        packet = OutBufferPacket(filename='packet')
        volume.diff(in_seq, packet)
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '6'},
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document', 'guid': '8'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[1, 1], [3, 3], [5, 8]]},
            ],
            read_packet(packet))

    def test_diff_TheSameInSeqForAllDocuments(self):

        class Document1(ad.Document):
            pass

        class Document2(ad.Document):
            pass

        class Document3(ad.Document):
            pass

        volume = Volume('db', [Document1, Document2, Document3])

        volume['document1'].create(guid='3', seqno=3)
        volume['document2'].create(guid='2', seqno=2)
        volume['document3'].create(guid='1', seqno=1)

        in_seq = Sequence([[1, None]])
        packet = OutBufferPacket(filename='packet')
        volume.diff(in_seq, packet)
        self.assertEqual([
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document1', 'guid': '3'},
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document2', 'guid': '2'},
            {'filename': 'packet', 'content_type': 'records', 'cmd': 'sn_push', 'document': 'document3', 'guid': '1'},
            {'filename': 'packet', 'cmd': 'sn_commit', 'sequence': [[1, 3]]},
            ],
            read_packet(packet))

    def test_SimulateDeleteEvents(self):

        class Document(Resource):
            pass

        events = []
        volume = Volume('db', [Document])
        volume.connect(lambda event: events.append(event))

        volume['document'].create(guid='guid')
        del events[:]
        volume['document'].update('guid', layer=['deleted'])

        self.assertEqual([
            {'event': 'delete', 'document': 'document', 'seqno': 2, 'guid': 'guid'},
            ],
            events)

    def test_Subscribe(self):

        class Document(Resource):

            @ad.active_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        cp = TestCommands(volume)
        events = []

        def read_events():
            for event in cp.subscribe(ad.Response()):
                if not event.strip():
                    continue
                assert event.startswith('data: ')
                assert event.endswith('\n\n')
                event = json.loads(event[6:])
                if 'props' in event:
                    event.pop('props')
                events.append(event)

        job = coroutine.spawn(read_events)
        coroutine.dispatch()
        volume['document'].create(guid='guid', prop='value1')
        coroutine.dispatch()
        volume['document'].update('guid', prop='value2')
        coroutine.dispatch()
        volume['document'].delete('guid')
        coroutine.dispatch()
        volume['document'].commit()
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': 'guid', 'seqno': 1, 'document': 'document', 'event': 'create'},
            {'guid': 'guid', 'seqno': 2, 'document': 'document', 'event': 'update'},
            {'guid': 'guid', 'event': 'delete', 'document': u'document'},
            ],
            events)

    def test_SubscribeToOnlyCommits(self):

        class Document(Resource):

            @ad.active_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        cp = TestCommands(volume)
        events = []

        def read_events():
            for event in cp.subscribe(ad.Response(), only_commits=True):
                if not event.strip():
                    continue
                assert event.startswith('data: ')
                assert event.endswith('\n\n')
                event = json.loads(event[6:])
                if 'props' in event:
                    event.pop('props')
                events.append(event)

        job = coroutine.spawn(read_events)
        coroutine.dispatch()
        volume['document'].create(guid='guid', prop='value1')
        coroutine.dispatch()
        volume['document'].update('guid', prop='value2')
        coroutine.dispatch()
        volume['document'].delete('guid')
        coroutine.dispatch()
        volume['document'].commit()
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'seqno': 1, 'document': 'document', 'event': 'commit'},
            {'seqno': 2, 'document': 'document', 'event': 'commit'},
            {'seqno': 2, 'document': 'document', 'event': 'commit'},
            ],
            events)


class TestCommands(ad.VolumeCommands, Commands):

    def __init__(self, volume):
        ad.VolumeCommands.__init__(self, volume)
        Commands.__init__(self)

    def connect(self, callback, condition=None, **kwargs):
        self.volume.connect(callback, condition)


def read_packet(packet):
    result = []
    for i in InPacket(stream=packet.pop()):
        if 'diff' in i:
            i.pop('diff')
        result.append(i)
    return result


if __name__ == '__main__':
    tests.main()
