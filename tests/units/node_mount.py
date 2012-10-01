#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
import zipfile
from os.path import exists, abspath, join

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.local.mounts import HomeMount
from sugar_network.local.mountset import Mountset
from sugar_network.local import activities
from sugar_network.toolkit import mounts_monitor
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network import local, sugar
from sugar_network.resources.volume import Volume
from sugar_network.resources.report import Report
from sugar_network.local.ipc_client import Router as IPCRouter
from sugar_network import IPCClient


class NodeMountTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.events_job = None
        local.server_mode.value = True

    def tearDown(self):
        if self.events_job is not None:
            self.events_job.kill()
        tests.Test.tearDown(self)

    def start_server(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        local.mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context, Report])
        self.mounts = Mountset(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', local.ipc_port.value), IPCRouter(self.mounts))
        coroutine.spawn(self.server.serve_forever)
        self.mounts.open()
        mounts_monitor.start(tests.tmpdir)
        self.mounts.opened.wait()

        return self.mounts

    def test_GetKeep(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        context = remote.get(['context', guid], reply=['keep', 'keep_impl'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(0, context['keep_impl'])
        self.assertEqual([
            {'guid': guid, 'keep': False, 'keep_impl': False},
            ],
            remote.get(['context'], reply=['guid', 'keep', 'keep_impl'])['result'])

        mounts.volume['context'].create(guid=guid, type='activity',
                title='local', summary='summary',
                description='description', keep=True, keep_impl=2,
                user=[sugar.uid()])
        coroutine.sleep(1)

        context = remote.get(['context', guid], reply=['keep', 'keep_impl'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(2, context['keep_impl'])
        self.assertEqual([
            {'guid': guid, 'keep': True, 'keep_impl': 2},
            ],
            remote.get(['context'], reply=['guid', 'keep', 'keep_impl'])['result'])

    def test_SetKeep(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        mounts['~'] = HomeMount(mounts.volume)
        local = IPCClient(mountpoint='~')
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid_1 = remote.post(['context'], {
            'type': 'activity',
            'title': 'remote',
            'summary': 'summary',
            'description': 'description',
            })
        guid_2 = remote.post(['context'], {
            'type': 'activity',
            'title': 'remote-2',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, local.get, ['context', guid_1])
        self.assertRaises(RuntimeError, local.get, ['context', guid_2])

        remote.put(['context', guid_1], {'keep': True})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

        remote.put(['context', guid_1], {'keep': False})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': False, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

        local.put(['context', guid_1], {'title': 'local'})

        self.assertEqual(
                {'title': 'local'},
                local.get(['context', guid_1], reply=['title']))

        remote.put(['context', guid_1], {'keep': True})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'local', 'keep': True, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

    def test_SetKeepImpl(self):
        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.context',
                'sugar_network.resources.implementation',
                ]

        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        mounts['~'] = HomeMount(mounts.volume)
        local = IPCClient(mountpoint='~')
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')
        coroutine.spawn(activities.monitor, mounts.volume['context'], ['Activities'])

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'remote',
            'summary': 'summary',
            'description': 'description',
            })
        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })
        remote.put(['context', context, 'versions'], {
            '1': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'echo',
                            },
                        },
                    'stability': 'stable',
                    'guid': impl,
                    'size': 0,
                    'extract': 'TestActivitry',
                    },
                },
            })
        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()
        remote.put(['implementation', impl, 'data'], cmd='upload_blob', path=abspath('bundle'))

        remote.put(['context', context], {'keep_impl': 1})
        coroutine.sleep(1)

        cursor = local.get(['context'], reply=['guid', 'keep_impl', 'title'])['result']
        self.assertEqual([
            {'guid': context, 'title': 'remote', 'keep_impl': 2},
            ],
            cursor)
        assert exists('Activities/TestActivitry/activity/activity.info')

    def test_Events(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        events = []
        got_event = coroutine.Event()

        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
                got_event.set()
        job = coroutine.spawn(read_events)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'create', 'guid': guid, 'seqno': 1},
            ],
            events)
        del events[:]

        remote.put(['context', guid], {'title': 'new-title'})
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 2},
            ],
            events)
        del events[:]

        guid_path = 'mnt/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)
        remote.delete(['context', guid])
        assert not exists(guid_path)
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'delete', 'guid': guid},
            ],
            events)
        del events[:]

    def test_upload_blob(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        client.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        self.assertEqual('blob', client.request('GET', ['context', guid, 'preview']).content)

        self.touch(('file2', 'blob2'))
        client.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        self.assertEqual('blob2', client.request('GET', ['context', guid, 'preview']).content)
        assert not exists('file2')

    def test_GetBLOBs(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

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
                client.request('GEt', ['context', guid, 'icon']).content)
        blob_url = 'http://localhost:%s/context/%s/icon?mountpoint=%s' % (local.ipc_port.value, guid, tests.tmpdir + '/mnt')
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
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = client.post(['report'], {
            'context': 'context',
            'implementation': 'implementation',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, client.get, ['report', guid, 'data'])
        blob_url = 'http://localhost:%s/report/%s/data?mountpoint=%s' % (local.ipc_port.value, guid, tests.tmpdir + '/mnt')
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                client.get(['report'], reply=['data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                client.get(['report', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_get_blob_ExtractImplementations(self):
        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.implementation',
                ]

        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = remote.post(['implementation'], {
            'context': 'context',
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })

        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()
        remote.put(['implementation', guid, 'data'], cmd='upload_blob', path=abspath('bundle'))

        blob = remote.get(['implementation', guid, 'data'], cmd='get_blob')
        self.assertEqual(abspath('cache/implementation/%s/%s/data' % (guid[:2], guid)), blob['path'])
        self.assertEqual('probe', file(join(blob['path'], 'probe')).read())


if __name__ == '__main__':
    tests.main()
