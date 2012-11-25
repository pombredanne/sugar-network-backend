#!/usr/bin/env python
# sugar-lint: disable

import json
import cPickle as pickle
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network import node, sugar
from sugar_network.toolkit.collection import Sequence
from sugar_network.toolkit.sneakernet import InPacket, OutBufferPacket, DiskFull
from sugar_network.resources.volume import Volume, Resource, Commands, VolumeCommands
from sugar_network.resources.user import User
from sugar_network.toolkit.router import Request
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
            {'event': 'delete', 'document': 'document', 'guid': 'guid'},
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
            for event in cp.subscribe(Request(), ad.Response()):
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
            {'event': 'handshake'},
            {'guid': 'guid', 'document': 'document', 'event': 'create'},
            {'guid': 'guid', 'document': 'document', 'event': 'update'},
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
            for event in cp.subscribe(Request(), ad.Response(), only_commits=True):
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
            {'event': 'handshake'},
            {'document': 'document', 'event': 'commit'},
            {'document': 'document', 'event': 'commit'},
            {'document': 'document', 'event': 'commit'},
            ],
            events)

    def test_MixinBlobUrls(self):
        volume = Volume('db')
        cp = TestCommands(volume)

        guid1 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        guid2 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid2, 'icon', url='http://foo/bar')
        guid3 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid3, 'icon', url='/foo/bar')
        guid4 = call(cp, method='POST', document='artifact', principal='principal', content={
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })

        # No GUID in reply
        self.assertEqual(
                {'icon': 'http://localhost/static/images/missing.png'},
                call(cp, method='GET', document='context', guid=guid1, reply=['icon']))
        self.assertEqual(
                {'icon': 'http://foo/bar'},
                call(cp, method='GET', document='context', guid=guid2, reply=['icon']))
        self.assertEqual(
                {'icon': 'http://localhost/foo/bar'},
                call(cp, method='GET', document='context', guid=guid3, reply=['icon']))
        self.assertEqual(
                {'data': 'http://localhost/artifact/%s/data' % guid4},
                call(cp, method='GET', document='artifact', guid=guid4, reply=['data']))
        self.assertRaises(RuntimeError, call, cp, method='GET', document='context', reply=['icon'])

        # GUID in reply
        self.assertEqual(
                {'guid': guid1, 'icon': 'http://localhost/static/images/missing.png', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid1, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid2, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid3, 'icon': 'http://localhost/foo/bar', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid3, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid4, 'data': 'http://localhost/artifact/%s/data' % guid4, 'layer': ['public']},
                call(cp, method='GET', document='artifact', guid=guid4, reply=['guid', 'data', 'layer']))
        self.assertEqual(
                sorted([
                    {'guid': guid1, 'icon': 'http://localhost/static/images/missing.png', 'layer': ['public']},
                    {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                    {'guid': guid3, 'icon': 'http://localhost/foo/bar', 'layer': ['public']},
                    ]),
                sorted(call(cp, method='GET', document='context', reply=['guid', 'icon', 'layer'])['result']))

        self.assertEqual([
            {'guid': guid4, 'data': 'http://localhost/artifact/%s/data' % guid4, 'layer': ['public']},
            ],
            call(cp, method='GET', document='artifact', reply=['guid', 'data', 'layer'])['result'])

        node.static_url.value = 'static_url'
        self.assertEqual(
                sorted([
                    {'guid': guid1, 'icon': 'static_url/static/images/missing.png', 'layer': ['public']},
                    {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                    {'guid': guid3, 'icon': 'static_url/foo/bar', 'layer': ['public']},
                    ]),
                sorted(call(cp, method='GET', document='context', reply=['guid', 'icon', 'layer'])['result']))

    def test_Populate(self):
        self.touch(
                ('db/context/1/1/guid', pickle.dumps({"value": "1"})),
                ('db/context/1/1/ctime', pickle.dumps({"value": 1})),
                ('db/context/1/1/mtime', pickle.dumps({"value": 1})),
                ('db/context/1/1/seqno', pickle.dumps({"value": 0})),
                ('db/context/1/1/type', pickle.dumps({"value": "activity"})),
                ('db/context/1/1/title', pickle.dumps({"value": {}})),
                ('db/context/1/1/summary', pickle.dumps({"value": {}})),
                ('db/context/1/1/description', pickle.dumps({"value": {}})),
                )

        volume = Volume('db', lazy_open=True)
        cp = TestCommands(volume)
        assert not exists('db/context/index')

        self.assertEqual(
                [],
                call(cp, method='GET', document='context')['result'])
        coroutine.dispatch()
        self.assertEqual(
                [{'guid': '1'}],
                call(cp, method='GET', document='context')['result'])
        assert exists('db/context/index')

    def test_DefaultAuthor(self):

        class Document(Resource):
            pass

        volume = Volume('db', [User, Document])
        cp = TestCommands(volume)

        guid = call(cp, method='POST', document='document', content={}, principal='user')
        self.assertEqual(
                [{'name': 'user', 'role': 2}],
                call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual(
                {'user': {'role': 2, 'order': 0}},
                volume['document'].get(guid)['author'])

        volume['user'].create(guid='user', color='', pubkey='', name='User')

        guid = call(cp, method='POST', document='document', content={}, principal='user')
        self.assertEqual(
                [{'guid': 'user', 'name': 'User', 'role': 3}],
                call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual(
                {'user': {'name': 'User', 'role': 3, 'order': 0}},
                volume['document'].get(guid)['author'])

    def test_PreserveAuthorsOrder(self):

        class Document(Resource):
            pass

        volume = Volume('db', [User, Document])
        cp = TestCommands(volume)

        volume['user'].create(guid='user1', color='', pubkey='', name='User1')
        volume['user'].create(guid='user2', color='', pubkey='', name='User2')
        volume['user'].create(guid='user3', color='', pubkey='', name='User3')

        guid = call(cp, method='POST', document='document', content={}, principal='user1')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user2', role=0)
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user2', principal='user1')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user2', principal='user1')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user3', principal='user1')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            'user3': {'name': 'User3', 'role': 1, 'order': 4},
            },
            volume['document'].get(guid)['author'])

    def test_AddUser(self):

        class Document(Resource):
            pass

        volume = Volume('db', [User, Document])
        cp = TestCommands(volume)

        volume['user'].create(guid='user1', color='', pubkey='', name='User1')
        volume['user'].create(guid='user2', color='', pubkey='', name='User2')

        guid = call(cp, method='POST', document='document', content={}, principal='user1')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='User3', role=3)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='User4', role=4)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            {'name': 'User4', 'role': 0},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            'User4': {'role': 0, 'order': 3},
            },
            volume['document'].get(guid)['author'])

    def test_UpdateAuthor(self):

        class Document(Resource):
            pass

        volume = Volume('db', [User, Document])
        cp = TestCommands(volume)

        volume['user'].create(guid='user1', color='', pubkey='', name='User1')
        guid = call(cp, method='POST', document='document', content={}, principal='user1')

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='User2', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'name': 'User2', 'role': 0},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user1', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 0},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='User2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 2},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 2, 'order': 1},
            },
            volume['document'].get(guid)['author'])

    def test_DelUser(self):

        class Document(Resource):
            pass

        volume = Volume('db', [User, Document])
        cp = TestCommands(volume)

        volume['user'].create(guid='user1', color='', pubkey='', name='User1')
        volume['user'].create(guid='user2', color='', pubkey='', name='User2')
        guid = call(cp, method='POST', document='document', content={}, principal='user1')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='user2')
        call(cp, method='PUT', document='document', guid=guid, cmd='useradd', user='User3')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        # Do not remove yourself
        self.assertRaises(RuntimeError, call, cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user1', principal='user1')
        self.assertRaises(RuntimeError, call, cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user2', principal='user2')

        call(cp, method='PUT', document='document', guid=guid, cmd='userdel', user='user1', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        call(cp, method='PUT', document='document', guid=guid, cmd='userdel', user='User3', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            call(cp, method='GET', document='document', guid=guid, prop='author'))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            },
            volume['document'].get(guid)['author'])


class TestCommands(VolumeCommands, Commands):

    def __init__(self, volume):
        VolumeCommands.__init__(self, volume)
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


def call(cp, principal=None, content=None, **kwargs):
    request = Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': 'localhost'}
    request.commands = cp
    return cp.call(request, ad.Response())


if __name__ == '__main__':
    tests.main()
