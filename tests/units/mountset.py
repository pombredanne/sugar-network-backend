#!/usr/bin/env python
# sugar-lint: disable

import os
import socket
import shutil
from os.path import exists

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.local.mountset import Mountset
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.implementation import Implementation
from sugar_network.toolkit import http, mounts_monitor
from sugar_network import local, sugar, node
from sugar_network.resources.volume import Volume
from sugar_network.local.mounts import HomeMount, RemoteMount
from sugar_network.local.ipc_client import Router as IPCRouter
from sugar_network import IPCClient, Client
from sugar_network.zerosugar import injector
from sugar_network.local import journal


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
        mounts_monitor.start(tests.tmpdir)
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
        coroutine.dispatch()
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': None}},
            ],
            self.events)
        del self.events[:]

        self.override(journal, 'exists', lambda *args: True)
        mounts.launch('~', 'context', 'app', [], object_id='object_id')
        coroutine.dispatch()
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'object_id'}},
            ],
            self.events)
        del self.events[:]

        self.override(journal, 'exists', lambda *args: False)
        self.assertRaises(ad.NotFound, mounts.launch, '~', 'context', 'app', [], object_id='object_id')

    def test_launch_ResumeArtifact(self):
        mounts = self.mountset()
        mounts['~'] = HomeMount(mounts.volume)

        mounts.volume['artifact'].create({
            'guid': 'artifact',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })
        coroutine.dispatch()
        del self.events[:]

        self.override(injector, 'launch', lambda *args, **kwargs: [{'args': args, 'kwargs': kwargs}])
        updates = []
        self.override(journal, 'update', lambda *args, **kwargs: updates.append((args, kwargs)))
        self.override(journal, 'exists', lambda *args: False)

        mounts.launch('~', 'context', 'app', [], object_id='artifact')
        coroutine.dispatch()
        self.assertEqual([
            (('artifact',), {
                'title': 'title',
                'description': 'description',
                'preview': 'http://localhost:5101/artifact/artifact/preview',
                'data': 'http://localhost:5101/artifact/artifact/data',
                }),
            ],
            updates)
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'artifact'}},
            ],
            self.events)
        del self.events[:]

    def test_launch_ResumeContext(self):
        mounts = self.mountset()
        mounts['~'] = HomeMount(mounts.volume)

        mounts.volume['context'].create({
            'guid': 'context',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        mounts.volume['implementation'].create({
            'guid': 'impl1',
            'context': 'context',
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })
        mounts.volume['implementation'].create({
            'guid': 'impl2',
            'context': 'context',
            'license': 'GPLv3+',
            'version': '2',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })
        coroutine.dispatch()
        del self.events[:]

        self.override(injector, 'launch', lambda *args, **kwargs: [{'args': args, 'kwargs': kwargs}])
        updates = []
        self.override(journal, 'update', lambda *args, **kwargs: updates.append((args, kwargs)))
        self.override(journal, 'exists', lambda *args: False)

        mounts.launch('~', 'context', 'app', [], context='context')
        coroutine.dispatch()
        self.assertEqual([
            (('impl2',), {
                'title': 'title',
                'description': 'description',
                'preview': 'http://localhost:5101/context/context/preview',
                'data': 'http://localhost:5101/implementation/impl2/data',
                }),
            ],
            updates)
        updates = []
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'impl2'}},
            ],
            self.events)
        del self.events[:]

        mounts.launch('~', 'context', 'app', [], context='context', object_id='impl1')
        coroutine.dispatch()
        self.assertEqual([
            (('impl1',), {
                'title': 'title',
                'description': 'description',
                'preview': 'http://localhost:5101/context/context/preview',
                'data': 'http://localhost:5101/implementation/impl1/data',
                }),
            ],
            updates)
        updates = []
        self.assertEqual([
            {'event': 'launch', 'args': ['~', 'app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'impl1'}},
            ],
            self.events)
        del self.events[:]


if __name__ == '__main__':
    tests.main()
