#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
from os.path import exists, abspath

from __init__ import tests

from active_toolkit import sockets, coroutine
from sugar_network.resources.report import Report
from sugar_network import local
from sugar_network.local import activities
from sugar_network import IPCClient


class HomeMountTest(tests.Test):

    def test_create(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertNotEqual(None, guid)

        res = local.get(['context', guid], reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])
        self.assertEqual(False, res['keep'])
        self.assertEqual(0, res['keep_impl'])
        self.assertEqual([-1, -1], res['position'])

    def test_update(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        local.put(['context', guid], {
            'title': 'title_2',
            'keep': True,
            'position': (2, 3),
            })

        context = local.get(['context', guid], reply=['title', 'keep', 'position'])
        self.assertEqual('title_2', context['title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual([2, 3], context['position'])

    def test_find(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

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

        cursor = local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(3, cursor['total'])
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', False, 0, [-1, -1]),
                    (guid_2, 'title_2', False, 0, [-1, -1]),
                    (guid_3, 'title_3', False, 0, [-1, -1]),
                    ]),
                sorted([(i['guid'], i['title'], i['keep'], i['keep_impl'], i['position']) for i in cursor['result']]))

    def test_upload_blob(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        self.assertEqual('blob', local.get(['context', guid, 'preview']).content)

        self.touch(('file2', 'blob2'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        self.assertEqual('blob2', local.get(['context', guid, 'preview']).content)
        assert not exists('file2')

    def test_GetBLOBs(self):
        self.start_server()
        client = IPCClient(mountpoint='~')

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
                client.get(['context', guid, 'icon']).content)
        blob_url = 'http://localhost:%s/context/%s/icon?mountpoint=~' % (local.ipc_port.value, guid)
        self.assertEqual(
                [{'guid': guid, 'icon': blob_url}],
                client.get(['context'], reply=['icon'])['result'])
        self.assertEqual(
                {'icon': blob_url},
                client.get(['context', guid], reply=['icon']))
        self.assertEqual(
                'icon-blob',
                urllib2.urlopen(blob_url).read())

    def test_GetAbsentBLOBs(self):
        self.start_server([Report])
        client = IPCClient(mountpoint='~')

        guid = client.post(['report'], {
            'context': 'context',
            'implementation': 'implementation',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, client.get, ['report', guid, 'data'])
        blob_url = 'http://localhost:%s/report/%s/data?mountpoint=~' % (local.ipc_port.value, guid)
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                client.get(['report'], reply=['data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                client.get(['report', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_Subscription(self):
        self.start_server()
        local = IPCClient(mountpoint='~')
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
            {'guid': guid, 'seqno': 1, 'document': 'context', 'event': 'create'},
            {'guid': guid, 'seqno': 2, 'document': 'context', 'event': 'update', 'mountpoint': '~'},
            {'guid': guid, 'event': 'delete', 'document': 'context', 'mountpoint': '~'},
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
        client = IPCClient(mountpoint='~')

        monitor = coroutine.spawn(activities.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        self.assertEqual({
            '1': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'false',
                            },
                        },
                    'stability': 'stable',
                    'guid': tests.tmpdir + '/Activities/activity-1',
                    'requires': {},
                    },
                },
            '2': {
                '*-*': {
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
                },
            },
            client.get(['context', 'bundle_id', 'versions']))


if __name__ == '__main__':
    tests.main()
