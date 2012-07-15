#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
import signal
from cStringIO import StringIO
from contextlib import contextmanager

from __init__ import tests

import active_document as ad
from sugar_network import Client
from sugar_network.local import local_root

from sugar_network.toolkit.sneakernet import InPacket, OutPacket
from active_toolkit import util, coroutine


class SyncTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        local_root.value = 'node'
        self.touch(('master/db/master', 'master'))
        os.makedirs('mnt')

        self.master_pid = self.popen([
            'sugar-network-server', '--port=8100', '--subscribe-port=8101',
            '--data-root=master/db', '--index-flush-threshold=1024',
            '--index-flush-timeout=3', '--only-commit-events',
            '--tmpdir=tmp', '-DDDF', 'start',
            ])
        self.node_pid = self.popen([
            'sugar-network-service', '--port=8200', '--subscribe-port=8201',
            '--activity-dirs=node/Activities', '--local-root=node',
            '--mounts-root=mnt', '--server-mode', '--tmpdir=tmp',
            '--api-url=http://localhost:8100', '-DDDF', 'start',
            ])

        coroutine.sleep(1)
        with Client('/') as client:
            if not client.connected:
                self.wait_for_events({'event': 'mount', 'mountpoint': '/'})

    def tearDown(self):
        self.waitpid(self.master_pid, signal.SIGINT)
        self.waitpid(self.node_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_Sneakernet(self):
        # Create initial data on master
        with Client('/') as client:
            context = client.Context(type='activity', title='title_1', summary='summary', description='description')
            guid_1 = context.post()
            self.touch(('preview_1', 'preview_1'))
            context.upload_blob('preview', 'preview_1')

            context = client.Context(type='activity', title='title_2', summary='summary', description='description')
            guid_2 = context.post()
            self.touch(('preview_2', 'preview_2'))
            context.upload_blob('preview', 'preview_2')

        # Clone initial dump
        pid = self.popen('V=1 sugar-network-sync mnt_1 http://localhost:8100', shell=True)
        self.waitpid(pid, 0)

        # Start node and import cloned data
        self.touch('mnt_1/.sugar-network')
        self.touch(('mnt_1/node', 'node'))
        self.touch(('mnt_1/master', 'master'))
        os.rename('mnt_1', 'mnt/mnt_1')
        self.wait_for_events({'event': 'sync_complete'})
        mountpoint = tests.tmpdir + '/mnt/mnt_1'

        # Create data on node
        with Client(mountpoint) as client:
            if not client.connected:
                self.wait_for_events({'event': 'mount', 'mountpoint': mountpoint})

            context = client.Context(type='activity', title='title_3', summary='summary', description='description')
            guid_3 = context.post()
            self.touch(('preview_3', 'preview_3'))
            context.upload_blob('preview', 'preview_3')

            context = client.Context(type='activity', title='title_4', summary='summary', description='description')
            guid_4 = context.post()
            self.touch(('preview_4', 'preview_4'))
            context.upload_blob('preview', 'preview_4')

        # Create node push packets with newly create data
        self.touch('mnt_2/.sugar-network-sync')
        os.rename('mnt_2', 'mnt/mnt_2')
        self.wait_for_events({'event': 'sync_complete'})

        # Upload node data to master
        shutil.copytree('mnt/mnt_2', 'mnt_3')
        pid = self.popen('V=1 sugar-network-sync mnt_3', shell=True)
        self.waitpid(pid, 0)

        # Process master's reply
        os.rename('mnt_3', 'mnt/mnt_3')
        self.wait_for_events({'event': 'sync_complete'})

        with Client(mountpoint) as client:
            self.assertEqual(
                    sorted([
                        (guid_1, 'title_1', 'preview_1'),
                        (guid_2, 'title_2', 'preview_2'),
                        (guid_3, 'title_3', 'preview_3'),
                        (guid_4, 'title_4', 'preview_4'),
                        ]),
                    sorted([(i['guid'], i['title'], i.get_blob('preview').read()) for i in client.Context.cursor(reply=['guid', 'title'])]))

        with Client('/') as client:
            self.assertEqual(
                    sorted([
                        (guid_1, 'title_1', 'preview_1'),
                        (guid_2, 'title_2', 'preview_2'),
                        (guid_3, 'title_3', 'preview_3'),
                        (guid_4, 'title_4', 'preview_4'),
                        ]),
                    sorted([(i['guid'], i['title'], i.get_blob('preview').read()) for i in client.Context.cursor(reply=['guid', 'title'])]))

    def wait_for_events(self, *events):
        events = list(events)
        connected = coroutine.Event()

        def wait_connect(event):
            for i in events[:]:
                for k, v in i.items():
                    if event.get(k) != v:
                        break
                else:
                    events.remove(i)
            if not events:
                connected.set()

        Client.connect(wait_connect)
        connected.wait()


if __name__ == '__main__':
    tests.main()
