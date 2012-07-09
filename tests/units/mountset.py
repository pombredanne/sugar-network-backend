#!/usr/bin/env python
# sugar-lint: disable

import os
import socket
import shutil
from os.path import exists

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.local import activities_crawler
from sugar_network.local.mountset import Mountset
from sugar_network.local.bus import IPCServer
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.toolkit import http
from sugar_network import local, Client, ServerError, sugar, node
from sugar_network.resources.volume import Volume
from sugar_network.local.mounts import HomeMount, RemoteMount


class MountsetTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

    def tearDown(self):
        tests.Test.tearDown(self)

    def mountset(self):
        local.mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context])
        mounts = Mountset(volume)
        Client.connection = mounts
        self.mounted = coroutine.Event()

        def events_cb(event):
            print event
            if event['event'] in ('mount', 'unmount'):
                self.mount_events.append((event['event'], event['mountpoint']))
                self.mounted.set()

        self.mount_events = []
        Client.connect(events_cb)

        mounts.open()
        mounts.opened.wait()
        # Let `open()` start processing spawned jobs
        coroutine.dispatch()

        return mounts

    def test_Populate(self):
        os.makedirs('1/sugar-network')
        os.makedirs('2/sugar-network')

        self.mountset()

        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual([
            ('mount', tests.tmpdir + '/1'),
            ('mount', tests.tmpdir + '/2'),
            ],
            self.mount_events)

        Client(tests.tmpdir + '/1').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()
        Client(tests.tmpdir + '/2').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()

        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1'},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2'},
                    ]),
                sorted(Client.mounts()))

    def test_Mount(self):
        self.mountset()

        os.makedirs('tmp/1/sugar-network')
        shutil.move('tmp/1', '.')

        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('mount', tests.tmpdir + '/1')],
                self.mount_events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1'},
                    ]),
                sorted(Client.mounts()))
        Client(tests.tmpdir + '/1').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()

        os.makedirs('tmp/2/sugar-network')
        shutil.move('tmp/2', '.')

        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('mount', tests.tmpdir + '/2')],
                self.mount_events[1:])
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1'},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2'},
                    ]),
                sorted(Client.mounts()))
        Client(tests.tmpdir + '/2').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()

    def test_Unmount(self):
        os.makedirs('1/sugar-network')

        self.mountset()
        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('mount', tests.tmpdir + '/1')],
                self.mount_events)
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1'},
                    ]),
                sorted(Client.mounts()))
        Client(tests.tmpdir + '/1').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()
        self.assertEqual(1, Client(tests.tmpdir + '/1').Context.cursor().total)

        os.makedirs('tmp/2/sugar-network')
        shutil.move('tmp/2', '.')

        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('mount', tests.tmpdir + '/2')],
                self.mount_events[1:])
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/1', 'name': '1'},
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2'},
                    ]),
                sorted(Client.mounts()))
        Client(tests.tmpdir + '/2').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()
        self.assertEqual(1, Client(tests.tmpdir + '/2').Context.cursor().total)

        shutil.rmtree('1')
        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('unmount', tests.tmpdir + '/1')],
                self.mount_events[2:])
        self.assertEqual(
                sorted([
                    {'mountpoint': tests.tmpdir + '/2', 'name': '2'},
                    ]),
                sorted(Client.mounts()))
        self.assertRaises(RuntimeError, Client(tests.tmpdir + '/1').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post)
        self.assertEqual(0, Client(tests.tmpdir + '/1').Context.cursor().total)

        shutil.rmtree('2')
        self.mounted.wait()
        self.mounted.clear()
        self.assertEqual(
                [('unmount', tests.tmpdir + '/2')],
                self.mount_events[3:])
        self.assertEqual(
                sorted([
                    ]),
                sorted(Client.mounts()))
        self.assertRaises(RuntimeError, Client(tests.tmpdir + '/2').Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post)
        self.assertEqual(0, Client(tests.tmpdir + '/2').Context.cursor().total)

    def test_MountNode(self):
        local.server_mode.value = True
        self.mountset()

        os.makedirs('tmp/mnt/sugar-network')
        shutil.move('tmp/mnt', '.')

        self.mounted.wait()
        self.mounted.clear()
        client = Client(tests.tmpdir + '/mnt')

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        local.api_url.value = 'http://localhost:%s' % node.port.value
        self.assertEqual(
                {'guid': guid, 'title': {'en-US': 'title'}},
                http.request('GET', ['context', guid], params={'reply': 'guid,title'}))

    def test_RootRemount(self):
        mountset = self.mountset()
        client = Client('/')

        mountset['/'] = RemoteMount(mountset.home_volume)
        self.assertEqual(['/'], [i['mountpoint'] for i in mountset.mounts()])
        assert not mountset['/'].mounted
        self.assertEqual(0, client.Context.cursor().total)
        self.assertRaises(RuntimeError, client.Context(type='activity', title='', summary='', description='').post)

        pid = self.fork(self.restful_server)
        coroutine.sleep(1)

        self.assertEqual([], self.mount_events)
        self.assertRaises(RuntimeError, client.Context(type='activity', title='', summary='', description='').post)
        self.mounted.wait()
        self.mounted.clear()
        assert mountset['/'].mounted
        self.assertEqual([('mount', '/')], self.mount_events)
        del self.mount_events[:]

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description',
                ).post()
        self.assertEqual(
                [guid],
                [i['guid'] for i in client.Context.cursor()])

        self.waitpid(pid)

        self.mounted.wait()
        self.mounted.clear()
        assert not mountset['/'].mounted
        self.assertEqual([('unmount', '/')], self.mount_events)
        del self.mount_events[:]
        self.assertEqual(0, client.Context.cursor().total)
        self.assertRaises(RuntimeError, client.Context(type='activity', title='', summary='', description='').post)

        pid = self.fork(self.restful_server)
        coroutine.sleep(1)

        self.assertEqual([], self.mount_events)
        self.assertRaises(RuntimeError, client.Context(type='activity', title='', summary='', description='').post)
        self.mounted.wait()
        self.mounted.clear()
        assert mountset['/'].mounted
        self.assertEqual([('mount', '/')], self.mount_events)
        del self.mount_events[:]


if __name__ == '__main__':
    tests.main()
