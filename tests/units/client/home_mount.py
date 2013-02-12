#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
from os.path import exists, abspath

from __init__ import tests

from sugar_network.toolkit import coroutine
from sugar_network.resources.user import User
from sugar_network.resources.artifact import Artifact
from sugar_network import client as local
from sugar_network.zerosugar import clones
from sugar_network.client import IPCClient


class HomeMountTest(tests.Test):

    def test_create(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertNotEqual(None, guid)

        res = local.get(['context', guid], reply=['guid', 'title', 'favorite', 'clone', 'position'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])
        self.assertEqual(False, res['favorite'])
        self.assertEqual(0, res['clone'])
        self.assertEqual([-1, -1], res['position'])

    def test_update(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        local.put(['context', guid], {
            'title': 'title_2',
            })

        context = local.get(['context', guid], reply=['title'])
        self.assertEqual('title_2', context['title'])

    def test_find(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})

        guid_1 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_1',
            'summary': 'summary',
            'description': 'description',
            })
        guid_2 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_2',
            'summary': 'summary',
            'description': 'description',
            })
        guid_3 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_3',
            'summary': 'summary',
            'description': 'description',
            })

        cursor = local.get(['context'], reply=['guid', 'title'])
        self.assertEqual(3, cursor['total'])
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1'),
                    (guid_2, 'title_2'),
                    (guid_3, 'title_3'),
                    ]),
                sorted([(i['guid'], i['title']) for i in cursor['result']]))

    def test_upload_blob(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        self.assertEqual('blob', local.request('GET', ['context', guid, 'preview']).content)

        self.touch(('file2', 'blob2'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        self.assertEqual('blob2', local.request('GET', ['context', guid, 'preview']).content)
        assert not exists('file2')

    def test_GetBLOBs(self):
        self.start_server()
        client = IPCClient(params={'mountpoint': '~'})

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'icon-blob'))
        client.put(['context', guid, 'icon'], cmd='upload_blob', path=abspath('file'))

        self.assertEqual(
                'icon-blob',
                client.request('GET', ['context', guid, 'icon']).content)
        blob_url = 'http://localhost:%s/context/%s/icon?mountpoint=~' % (local.ipc_port.value, guid)
        self.assertEqual(
                [{'guid': guid, 'icon': blob_url}],
                client.get(['context'], reply=['guid', 'icon'])['result'])
        self.assertEqual(
                {'icon': blob_url},
                client.get(['context', guid], reply=['icon']))
        self.assertEqual(
                'icon-blob',
                urllib2.urlopen(blob_url).read())

    def test_GetAbsentBLOBs(self):
        self.start_server([User, Artifact])
        client = IPCClient(params={'mountpoint': '~'})

        guid = client.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, client.get, ['artifact', guid, 'data'])
        blob_url = 'http://localhost:%s/artifact/%s/data?mountpoint=~' % (local.ipc_port.value, guid)
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                client.get(['artifact'], reply=['guid', 'data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                client.get(['artifact', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_Subscription(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})
        events = []

        def read_events():
            for event in local.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
        job = coroutine.spawn(read_events)

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch()
        local.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch()
        local.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'event': 'handshake'},
            {'guid': guid, 'document': 'context', 'event': 'create', 'mountpoint': '~'},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '~'},
            {'guid': guid, 'event': 'delete', 'document': 'context', 'mountpoint': '~'},
            ],
            events)

    def test_Subscription_NotifyOnlineMount(self):
        self.start_server()
        local = IPCClient(params={'mountpoint': '~'})
        events = []

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        def read_events():
            for event in local.subscribe():
                events.append(event)
        job = coroutine.spawn(read_events)
        coroutine.sleep(.1)

        self.mounts.volume['context'].update(guid, {'title': 'title_2'})
        self.mounts.volume['context'].update(guid, {'favorite': True})
        self.mounts.volume['context'].update(guid, {'clone': 2})
        coroutine.sleep(.1)
        job.kill()

        self.assertEqual([
            {'event': 'handshake'},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '~', 'props': {'title': 'title_2'}},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '/', 'props': {'favorite': True}},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '~', 'props': {'favorite': True}},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '/', 'props': {'clone': 2}},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '~', 'props': {'clone': 2}},
            ],
            events)

    def test_Feed(self):
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

        self.start_server()
        client = IPCClient(params={'mountpoint': '~'})

        monitor = coroutine.spawn(clones.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

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
            client.get(['context', 'bundle_id', 'versions']))


if __name__ == '__main__':
    tests.main()
