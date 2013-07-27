#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests, src_root

from sugar_network import client, model
from sugar_network.client import IPCConnection, clones
from sugar_network.client.routes import ClientRoutes
from sugar_network.db import Volume
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import coroutine, http


class OfflineRoutes(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.home_volume = Volume('db', model.RESOURCES)
        commands = ClientRoutes(self.home_volume)
        server = coroutine.WSGIServer(('127.0.0.1', client.ipc_port.value), Router(commands))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()

    def test_NoAuthors(self):
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {},
                self.home_volume['context'].get(guid)['author'])
        self.assertEqual(
                [],
                ipc.get(['context', guid, 'author']))

    def test_HandleDeletes(self):
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        guid_path = 'db/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)

        ipc.delete(['context', guid])
        self.assertRaises(http.NotFound, ipc.get, ['context', guid])
        assert not exists(guid_path)

    def test_whoami(self):
        ipc = IPCConnection()

        self.assertEqual(
                {'guid': tests.UID, 'roles': []},
                ipc.get(cmd='whoami'))

    def test_clone(self):
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, ipc.put, ['context', context], 1, cmd='clone')

    def test_favorite(self):
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                {'favorite': False},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], True, cmd='favorite')

        self.assertEqual(
                {'favorite': True},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], False, cmd='favorite')

        self.assertEqual(
                {'favorite': False},
                ipc.get(['context', context], reply=['favorite']))

    def test_subscribe(self):
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
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
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)

    def test_BLOBs(self):
        ipc = IPCConnection()

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
                {'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://127.0.0.1:5555/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://127.0.0.1:5555/static/images/missing.png'}],
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

        ipc = IPCConnection()
        monitor = coroutine.spawn(clones.monitor, self.home_volume['context'], ['Activities'])
        coroutine.dispatch()

        self.assertEqual({
            'name': 'TestActivity',
            'implementations': [
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
            },
            ipc.get(['context', 'bundle_id'], cmd='feed'))

    def test_LocalAPIShouldDuplicateNodeButWith503Response(self):
        ipc = IPCConnection()
        self.assertRaises(http.ServiceUnavailable, ipc.get, ['context', 'foo'], cmd='feed')
        self.assertRaises(http.ServiceUnavailable, ipc.get, ['packages', 'foo', 'bar'])


if __name__ == '__main__':
    tests.main()
