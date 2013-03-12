#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests, src_root

from sugar_network import client
from sugar_network.client import IPCClient
from sugar_network.client.commands import ClientCommands
from sugar_network.toolkit.router import IPCRouter
from sugar_network.resources.volume import Volume
from sugar_network.zerosugar import clones
from sugar_network.toolkit import coroutine


class OfflineCommandsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.home_volume = Volume('db')
        commands = ClientCommands(self.home_volume, offline=True)
        server = coroutine.WSGIServer(('localhost', client.ipc_port.value), IPCRouter(commands))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()

    def test_SetUser(self):
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                [{'name': tests.UID, 'role': 2}],
                ipc.get(['context', guid, 'author']))

    def test_HandleDeletes(self):
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        guid_path = 'db/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)

        ipc.delete(['context', guid])
        self.assertRaises(RuntimeError, ipc.get, ['context', guid])
        assert not exists(guid_path)

    def test_whoami(self):
        ipc = IPCClient()

        self.assertEqual(
                {'guid': tests.UID, 'roles': [], 'route': 'proxy'},
                ipc.get(cmd='whoami'))

    def test_clone(self):
        ipc = IPCClient()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, ipc.put, ['context', context], 1, cmd='clone')

    def test_favorite(self):
        ipc = IPCClient()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                {'favorite': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], True, cmd='favorite')

        self.assertEqual(
                {'favorite': True, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], False, cmd='favorite')

        self.assertEqual(
                {'favorite': False, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

    def test_subscribe(self):
        ipc = IPCClient()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                if 'props' in event:
                    event.pop('props')
                events.append(event)
        job = coroutine.spawn(read_events)
        coroutine.dispatch()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch()
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch()
        ipc.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'document': 'context', 'event': 'create'},
            {'guid': guid, 'document': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'document': 'context'},
            ],
            events)

    def test_BLOBs(self):
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.request('PUT', ['context', guid, 'preview'], 'image')

        self.assertEqual(
                'image',
                ipc.request('GET', ['context', guid, 'preview']).content)
        self.assertEqual(
                {'preview': 'http://localhost:5555/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://localhost:5555/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://localhost:5555/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://localhost:5555/static/images/missing.png'}],
                ipc.get(['context'], reply=['icon'])['result'])

    def test_Feeds(self):
        self.touch(('Activities/activity-1/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]))
        self.touch(('Activities/activity-2/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'requires = dep1; dep2 = 1; dep3 < 2; dep4 >= 3',
            ]))

        ipc = IPCClient()
        monitor = coroutine.spawn(clones.monitor, self.home_volume['context'], ['Activities'])
        coroutine.dispatch()

        self.assertEqual([
            {
                'version': '1',
                'arch': '*-*',
                'commands': {
                    'activity': {
                        'exec': 'false',
                        },
                    },
                'stability': 'stable',
                'guid': tests.tmpdir + '/Activities/activity-1',
                'requires': {},
                },
            {
                'version': '2',
                'arch': '*-*',
                'commands': {
                    'activity': {
                        'exec': 'true',
                        },
                    },
                'stability': 'stable',
                'guid': tests.tmpdir + '/Activities/activity-2',
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                },
            ],
            ipc.get(['context', 'bundle_id', 'versions']))


if __name__ == '__main__':
    tests.main()
