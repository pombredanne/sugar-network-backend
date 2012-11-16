#!/usr/bin/env python
# sugar-lint: disable

import os
import socket
import shutil
import zipfile
from os.path import exists

import requests

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.client.mountset import Mountset
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.implementation import Implementation
from sugar_network.toolkit import http, mountpoints
from sugar_network import client as local, sugar, node
from sugar_network.resources.volume import Volume
from sugar_network.client.mounts import HomeMount, RemoteMount
from sugar_network.toolkit.router import IPCRouter
from sugar_network import IPCClient, Client
from sugar_network.zerosugar import injector, clones
from sugar_network.client import journal


class MountsetTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.events_job = None
        self.events = []
        self.mounted = coroutine.Event()

    def tearDown(self):
        if self.events_job is not None:
            self.events_job.kill()
        tests.Test.tearDown(self)

    def mountset(self):
        local.mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context, Implementation, Artifact])
        mounts = Mountset(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', local.ipc_port.value), IPCRouter(mounts))
        coroutine.spawn(self.server.serve_forever)
        mounts.open()
        mounts.opened.wait()

        def read_events():
            for event in IPCClient().subscribe():
                if 'props' in event:
                    event.pop('props')
                self.events.append(event)
                self.mounted.set()

        coroutine.dispatch()
        self.events_job = coroutine.spawn(read_events)
        coroutine.sleep(.5)
        mountpoints.populate(tests.tmpdir)
        coroutine.spawn(mountpoints.monitor, tests.tmpdir)
        coroutine.dispatch()

        return mounts

    def test_Populate(self):
        os.makedirs('1/.sugar-network')
        os.makedirs('2/.sugar-network')

        mounts = self.mountset()
        mounts[tests.tmpdir + '/1'].mounted.wait()
        mounts[tests.tmpdir + '/2'].mounted.wait()

        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/1', 'event': 'mount', 'private': True, 'name': '1'},
            {'mountpoint': tests.tmpdir + '/2', 'event': 'mount', 'private': True, 'name': '2'},
            ],
            self.events)

        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1', 'private': True},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2', 'private': True},
                    ]),
                sorted(IPCClient().get(cmd='mounts')))

    def test_Mount(self):
        mounts = self.mountset()

        os.makedirs('tmp/1/.sugar-network')
        shutil.move('tmp/1', '.')
        self.mounted.wait()
        self.mounted.clear()

        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/1', 'event': 'mount', 'private': True, 'name': '1'},
            ],
            self.events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1', 'private': True},
                    ]),
                sorted(IPCClient().get(cmd='mounts')))

        os.makedirs('tmp/2/.sugar-network')
        shutil.move('tmp/2', '.')
        self.mounted.wait()
        self.mounted.clear()

        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/1', 'event': 'mount', 'private': True, 'name': '1'},
            {'mountpoint': tests.tmpdir + '/2', 'event': 'mount', 'private': True, 'name': '2'},
            ],
            self.events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1', 'private': True},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2', 'private': True},
                    ]),
                sorted(IPCClient().get(cmd='mounts')))

    def test_Unmount(self):
        os.makedirs('1/.sugar-network')
        os.makedirs('2/.sugar-network')

        mounts = self.mountset()
        client = IPCClient()

        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1', 'private': True},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2', 'private': True},
                    ]),
                sorted(client.get(cmd='mounts')))

        self.mounted.clear()
        del self.events[:]
        shutil.rmtree('1')
        self.mounted.wait()
        self.mounted.clear()

        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/1', 'event': 'unmount', 'private': True, 'name': '1'},
            ],
            self.events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2', 'private': True},
                    ]),
                sorted(client.get(cmd='mounts')))

    def test_MountNode(self):
        local.server_mode.value = True
        mounts = self.mountset()

        self.touch('tmp/mnt/.sugar-network')
        self.touch(('tmp/mnt/node', 'node'))
        shutil.move('tmp/mnt', '.')
        self.mounted.wait()
        self.mounted.clear()

        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'event': 'mount', 'private': False, 'name': 'mnt'},
            ],
            self.events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/mnt', 'name': 'mnt', 'private': False},
                    ]),
                sorted(IPCClient().get(cmd='mounts')))

        client = Client('http://localhost:%s' % node.port.value)
        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                'title',
                client.get(['context', guid, 'title']))

    def test_launch_ResumeJobject(self):
        mounts = self.mountset()
        mounts['~'] = HomeMount(mounts.volume)
        coroutine.dispatch()
        del self.events[:]
        self.override(injector, 'launch', lambda *args, **kwargs: [{'args': args, 'kwargs': kwargs}])

        self.assertRaises(RuntimeError, mounts.launch, '~', 'fake', 'app', [])

        mounts.launch('~', 'context', 'app', [])
        coroutine.sleep(1)
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': None}},
            ],
            self.events)
        del self.events[:]

        self.override(journal, 'exists', lambda *args: True)
        mounts.launch('~', 'context', 'app', [], object_id='object_id')
        coroutine.sleep(1)
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'object_id'}},
            ],
            self.events)
        del self.events[:]

    def test_Hub(self):
        mounts = self.mountset()
        client = IPCClient()
        url = 'http://localhost:%s' % local.ipc_port.value

        response = requests.request('GET', url + '/hub', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual('/hub/', response.headers['Location'])

        local.hub_root.value = '.'
        index_html = '<html><body>index</body></html>'
        self.touch(('index.html', index_html))

        response = requests.request('GET', url + '/hub', allow_redirects=True)
        self.assertEqual(index_html, response.content)

        response = requests.request('GET', url + '/hub/', allow_redirects=False)
        self.assertEqual(index_html, response.content)

    def test_clone_Activities(self):
        self.start_ipc_and_restful_server()
        client = IPCClient()
        coroutine.spawn(clones.monitor, self.mounts.volume['context'], ['Activities'])

        context = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'stability': 'stable',
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
        client.request('PUT', ['implementation', impl, 'data'], file('bundle', 'rb').read())

        assert not exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 0, 'type': ['activity']},
                client.get(['context', context], reply=['clone']))
        self.assertRaises(RuntimeError, client.get, ['context', context], mountpoint='~')

        client.put(['context', context], 2, cmd='clone')
        coroutine.sleep(1)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                client.get(['context', context], reply=['clone']))
        self.assertEqual(
                {'clone': 2},
                client.get(['context', context], reply=['clone'], mountpoint='~'))

        client.put(['context', context], 2, cmd='clone')
        coroutine.sleep(1)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                client.get(['context', context], reply=['clone']))
        self.assertEqual(
                {'clone': 2},
                client.get(['context', context], reply=['clone'], mountpoint='~'))

        client.put(['context', context], 1, cmd='clone', force=1)
        coroutine.sleep(1)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                client.get(['context', context], reply=['clone']))
        self.assertEqual(
                {'clone': 2},
                client.get(['context', context], reply=['clone'], mountpoint='~'))

        client.put(['context', context], 0, cmd='clone')
        coroutine.sleep(1)

        assert not exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 0, 'type': ['activity']},
                client.get(['context', context], reply=['clone']))
        self.assertEqual(
                {'clone': 0},
                client.get(['context', context], reply=['clone'], mountpoint='~'))

    def test_clone_Content(self):
        updates = []
        self.override(journal.Commands, '__init__', lambda *args: None)
        self.override(journal.Commands, 'journal_update', lambda self, guid, preview=None, **kwargs: updates.append((guid, kwargs)))
        self.override(journal.Commands, 'journal_delete', lambda self, guid: updates.append((guid,)))

        self.start_ipc_and_restful_server()
        client = IPCClient()

        context = client.post(['context'], {
            'type': 'content',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        client.request('PUT', ['implementation', impl, 'data'], 'version_1')

        self.assertEqual({'clone': 0, 'type': ['content']}, client.get(['context', context], reply=['clone']))

        client.put(['context', context], 2, cmd='clone')
        self.touch('datastore/%s/%s/metadata/uid' % (context[:2], context))

        self.assertEqual([
            (context, {'activity_id': impl, 'data': 'version_1', 'description': 'description', 'title': 'title', 'mime_type': 'application/octet-stream'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                client.get(['context', context], reply=['clone']))
        del updates[:]

        client.request('PUT', ['implementation', impl, 'data'], 'version_2',
                headers={'Content-Type': 'foo/bar'})
        client.put(['context', context], 2, cmd='clone')

        self.assertEqual(
                [],
                updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                client.get(['context', context], reply=['clone']))

        client.put(['context', context], 1, cmd='clone', force=1)

        self.assertEqual([
            (context, {'activity_id': impl, 'data': 'version_2', 'description': 'description', 'title': 'title', 'mime_type': 'foo/bar'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                client.get(['context', context], reply=['clone']))
        del updates[:]

        client.put(['context', context], 0, cmd='clone')
        shutil.rmtree('datastore/%s/%s' % (context[:2], context))

        self.assertEqual([
            (context,),
            ],
            updates)
        self.assertEqual(
                {'clone': 0, 'type': ['content']},
                client.get(['context', context], reply=['clone']))
        del updates[:]

    def test_clone_Artifacts(self):
        updates = []
        self.override(journal.Commands, '__init__', lambda *args: None)
        self.override(journal.Commands, 'journal_update', lambda self, guid, preview=None, **kwargs: updates.append((guid, kwargs)))
        self.override(journal.Commands, 'journal_delete', lambda self, guid: updates.append((guid,)))

        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        client = IPCClient()

        artifact = client.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })
        client.request('PUT', ['artifact', artifact, 'data'], 'data')

        self.assertEqual({'clone': 0}, client.get(['artifact', artifact], reply=['clone']))

        client.put(['artifact', artifact], 2, cmd='clone')
        self.touch('datastore/%s/%s/metadata/uid' % (artifact[:2], artifact))

        self.assertEqual([
            (artifact, {'data': 'data', 'description': 'description', 'title': 'title', 'activity': 'context'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2},
                client.get(['artifact', artifact], reply=['clone']))
        del updates[:]

        client.put(['artifact', artifact], 2, cmd='clone')

        self.assertEqual(
                [],
                updates)
        self.assertEqual(
                {'clone': 2},
                client.get(['artifact', artifact], reply=['clone']))

        client.request('PUT', ['artifact', artifact, 'data'], 'data_2')
        client.put(['artifact', artifact], 1, cmd='clone', force=1)

        self.assertEqual([
            (artifact, {'data': 'data_2', 'description': 'description', 'title': 'title', 'activity': 'context'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2},
                client.get(['artifact', artifact], reply=['clone']))
        del updates[:]

        client.put(['artifact', artifact], 0, cmd='clone')
        shutil.rmtree('datastore/%s/%s' % (artifact[:2], artifact))

        self.assertEqual([
            (artifact,),
            ],
            updates)
        self.assertEqual(
                {'clone': 0},
                client.get(['artifact', artifact], reply=['clone']))
        del updates[:]

    def test_favorite_Activities(self):
        self.start_ipc_and_restful_server()
        client = IPCClient()
        coroutine.spawn(clones.monitor, self.mounts.volume['context'], ['Activities'])

        context = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                {'favorite': 0, 'type': ['activity']},
                client.get(['context', context], reply=['favorite']))
        self.assertRaises(RuntimeError, client.get, ['context', context], mountpoint='~')

        client.put(['context', context], True, cmd='favorite')
        coroutine.sleep(1)

        self.assertEqual(
                {'favorite': True, 'type': ['activity']},
                client.get(['context', context], reply=['favorite']))
        self.assertEqual(
                {'favorite': True},
                client.get(['context', context], reply=['favorite'], mountpoint='~'))

        client.put(['context', context], False, cmd='favorite')

        self.assertEqual(
                {'favorite': False, 'type': ['activity']},
                client.get(['context', context], reply=['favorite']))
        self.assertEqual(
                {'favorite': False},
                client.get(['context', context], reply=['favorite'], mountpoint='~'))

    def test_whoami(self):
        self.start_ipc_and_restful_server()
        client = IPCClient()
        remote = Client(local.api_url.value)

        self.assertEqual(
                {'guid': tests.UID, 'roles': [], 'route': 'proxy'},
                client.get([], cmd='whoami'))
        self.assertEqual(
                {'guid': tests.UID, 'roles': [], 'route': 'direct'},
                remote.get([], cmd='whoami'))


if __name__ == '__main__':
    tests.main()
