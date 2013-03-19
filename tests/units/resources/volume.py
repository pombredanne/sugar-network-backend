#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
from os.path import exists

from __init__ import tests

from sugar_network import db, node
from sugar_network.resources.volume import Volume, Resource, Commands
from sugar_network.resources.user import User
from sugar_network.toolkit import coroutine, sugar, util


class VolumeTest(tests.Test):

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

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        cp = TestCommands(volume)
        events = []

        def read_events():
            for event in cp.subscribe(event='!commit'):
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
            {'guid': 'guid', 'document': 'document', 'event': 'create'},
            {'guid': 'guid', 'document': 'document', 'event': 'update'},
            {'guid': 'guid', 'event': 'delete', 'document': u'document'},
            ],
            events)

    def test_SubscribeWithPong(self):
        volume = Volume('db', [])
        cp = TestCommands(volume)

        for event in cp.subscribe(ping=True):
            break
        self.assertEqual('data: {"event": "pong"}\n\n', event)

    def __test_SubscribeCondition(self):

        class Document(Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = Volume('db', [Document])
        cp = TestCommands(volume)
        events = []

        def read_events():
            for event in cp.subscribe(db.Request(), db.Response(), only_commits=True):
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
            {'document': 'document', 'event': 'commit'},
            {'document': 'document', 'event': 'commit'},
            {'document': 'document', 'event': 'commit'},
            ],
            events)

    def test_Populate(self):
        self.touch(
                ('db/context/1/1/guid', json.dumps({"value": "1"})),
                ('db/context/1/1/ctime', json.dumps({"value": 1})),
                ('db/context/1/1/mtime', json.dumps({"value": 1})),
                ('db/context/1/1/seqno', json.dumps({"value": 0})),
                ('db/context/1/1/type', json.dumps({"value": "activity"})),
                ('db/context/1/1/title', json.dumps({"value": {}})),
                ('db/context/1/1/summary', json.dumps({"value": {}})),
                ('db/context/1/1/description', json.dumps({"value": {}})),
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


class TestCommands(db.VolumeCommands, Commands):

    def __init__(self, volume):
        db.VolumeCommands.__init__(self, volume)
        Commands.__init__(self)
        self.volume.connect(self.broadcast)


def call(cp, principal=None, content=None, **kwargs):
    request = db.Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': 'localhost'}
    request.commands = cp
    return cp.call(request, db.Response())


if __name__ == '__main__':
    tests.main()
