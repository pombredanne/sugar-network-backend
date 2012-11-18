#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

import active_document as ad
from sugar_network.toolkit.router import Request
from sugar_network.resources.volume import Volume, VolumeCommands
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.artifact import Artifact
from sugar_network.client.mounts import _ProxyCommands


class ProxyCommandsTest(tests.Test):

    def test_FindsAddGuidToReply(self):

        class Commands(ad.CommandsProcessor, _ProxyCommands):

            result = []

            def __init__(self, volume):
                ad.CommandsProcessor.__init__(self)
                _ProxyCommands.__init__(self, volume)

            def proxy_call(self, request, response):
                return {'result': [{'guid': 'fake', 'type': 'activity'}], 'reply': request.get('reply')}

        volume = Volume('db', [Context])
        commands = Commands(volume)

        self.assertEqual(
                [],
                commands.call(Request(method='GET', document='context', reply=[]))['reply'])
        self.assertEqual(
                ['foo', 'bar'],
                commands.call(Request(method='GET', document='context', reply=['foo', 'bar']))['reply'])
        self.assertEqual(
                ['guid', 'type'],
                commands.call(Request(method='GET', document='context', reply=['guid', 'type']))['reply'])
        self.assertEqual(
                ['favorite', 'guid', 'type'],
                commands.call(Request(method='GET', document='context', reply=['favorite']))['reply'])
        self.assertEqual(
                ['clone', 'guid', 'type'],
                commands.call(Request(method='GET', document='context', reply=['clone']))['reply'])

        self.assertEqual(
                [],
                commands.call(Request(method='GET', document='artifact', reply=[]))['reply'])
        self.assertEqual(
                ['foo', 'bar'],
                commands.call(Request(method='GET', document='artifact', reply=['foo', 'bar']))['reply'])
        self.assertEqual(
                ['guid'],
                commands.call(Request(method='GET', document='artifact', reply=['guid']))['reply'])
        self.assertEqual(
                ['favorite', 'guid'],
                commands.call(Request(method='GET', document='artifact', reply=['favorite']))['reply'])
        self.assertEqual(
                ['clone', 'guid'],
                commands.call(Request(method='GET', document='artifact', reply=['clone']))['reply'])

    def test_FindAbsents(self):

        class Commands(ad.CommandsProcessor, _ProxyCommands):

            result = []

            def __init__(self, volume):
                ad.CommandsProcessor.__init__(self)
                _ProxyCommands.__init__(self, volume)

            def proxy_call(self, request, response):
                return {'result': Commands.result}

        volume = Volume('db', [Context])
        commands = Commands(volume)

        Commands.result = [{'guid': 'fake', 'type': 'activity'}]
        self.assertEqual(
                [{'guid': 'fake', 'favorite': False, 'type': 'activity'}],
                commands.call(Request(method='GET', document='context', reply=['favorite']))['result'])
        Commands.result = [{'guid': 'fake', 'type': 'activity'}]
        self.assertEqual(
                [{'guid': 'fake', 'clone': 0, 'type': 'activity'}],
                commands.call(Request(method='GET', document='context', reply=['clone']))['result'])
        Commands.result = [{'guid': 'fake', 'type': 'activity'}]
        self.assertEqual(
                [{'guid': 'fake', 'favorite': False, 'clone': 0, 'type': 'activity'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = [{'guid': 'fake'}]
        self.assertEqual(
                [{'guid': 'fake', 'favorite': False}],
                commands.call(Request(method='GET', document='artifact', reply=['favorite']))['result'])
        Commands.result = [{'guid': 'fake'}]
        self.assertEqual(
                [{'guid': 'fake', 'clone': 0}],
                commands.call(Request(method='GET', document='artifact', reply=['clone']))['result'])
        Commands.result = [{'guid': 'fake'}]
        self.assertEqual(
                [{'guid': 'fake', 'favorite': False, 'clone': 0}],
                commands.call(Request(method='GET', document='artifact', reply=['favorite', 'clone']))['result'])

    def test_Activities(self):

        class Commands(ad.CommandsProcessor, _ProxyCommands):

            result = []

            def __init__(self, volume):
                ad.CommandsProcessor.__init__(self)
                _ProxyCommands.__init__(self, volume)

            def proxy_call(self, request, response):
                return Commands.result

        volume = Volume('db', [Context])
        commands = Commands(volume)

        context = volume['context'].create(
                type='activity',
                title='local',
                summary='summary',
                description='description',
                )

        Commands.result = {'result': [{'guid': context, 'type': 'activity'}]}
        self.assertEqual(
                [{'guid': context, 'favorite': False, 'clone': 0, 'type': 'activity'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': context, 'type': 'activity'}
        self.assertEqual(
                {'guid': context, 'favorite': False, 'clone': 0, 'type': 'activity'},
                commands.call(Request(method='GET', document='context', guid=context, reply=['favorite', 'clone'])))

        volume['context'].update(context, favorite=True, clone=2)

        Commands.result = {'result': [{'guid': context, 'type': 'activity'}]}
        self.assertEqual(
                [{'guid': context, 'favorite': True, 'clone': 2, 'type': 'activity'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': context, 'type': 'activity'}
        self.assertEqual(
                {'guid': context, 'favorite': True, 'clone': 2, 'type': 'activity'},
                commands.call(Request(method='GET', document='context', guid=context, reply=['favorite', 'clone'])))

    def test_Content(self):

        class Commands(ad.CommandsProcessor, _ProxyCommands):

            result = []

            def __init__(self, volume):
                ad.CommandsProcessor.__init__(self)
                _ProxyCommands.__init__(self, volume)

            def proxy_call(self, request, response):
                return Commands.result

        volume = Volume('db', [Context])
        commands = Commands(volume)

        Commands.result = {'result': [{'guid': 'guid', 'type': 'content'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': False, 'clone': 0, 'type': 'content'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid', 'type': 'content'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': False, 'clone': 0, 'type': 'content'},
                commands.call(Request(method='GET', document='context', guid='guid', reply=['favorite', 'clone'])))

        self.touch(('datastore/gu/guid/metadata/keep', '0'))

        Commands.result = {'result': [{'guid': 'guid', 'type': 'content'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': False, 'clone': 2, 'type': 'content'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid', 'type': 'content'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': False, 'clone': 2, 'type': 'content'},
                commands.call(Request(method='GET', document='context', guid='guid', reply=['favorite', 'clone'])))

        self.touch(('datastore/gu/guid/metadata/keep', '1'))

        Commands.result = {'result': [{'guid': 'guid', 'type': 'content'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': True, 'clone': 2, 'type': 'content'}],
                commands.call(Request(method='GET', document='context', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid', 'type': 'content'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': True, 'clone': 2, 'type': 'content'},
                commands.call(Request(method='GET', document='context', guid='guid', reply=['favorite', 'clone'])))

    def test_Artifacts(self):

        class Commands(ad.CommandsProcessor, _ProxyCommands):

            result = []

            def proxy_call(self, request, response):
                return Commands.result

        commands = Commands(None)

        Commands.result = {'result': [{'guid': 'guid'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': False, 'clone': 0}],
                commands.call(Request(method='GET', document='artifact', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': False, 'clone': 0},
                commands.call(Request(method='GET', document='artifact', guid='guid', reply=['favorite', 'clone'])))

        self.touch(('datastore/gu/guid/metadata/keep', '0'))

        Commands.result = {'result': [{'guid': 'guid'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': False, 'clone': 2}],
                commands.call(Request(method='GET', document='artifact', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': False, 'clone': 2},
                commands.call(Request(method='GET', document='artifact', guid='guid', reply=['favorite', 'clone'])))

        self.touch(('datastore/gu/guid/metadata/keep', '1'))

        Commands.result = {'result': [{'guid': 'guid'}]}
        self.assertEqual(
                [{'guid': 'guid', 'favorite': True, 'clone': 2}],
                commands.call(Request(method='GET', document='artifact', reply=['favorite', 'clone']))['result'])

        Commands.result = {'guid': 'guid'}
        self.assertEqual(
                {'guid': 'guid', 'favorite': True, 'clone': 2},
                commands.call(Request(method='GET', document='artifact', guid='guid', reply=['favorite', 'clone'])))


if __name__ == '__main__':
    tests.main()
