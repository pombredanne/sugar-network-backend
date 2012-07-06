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

        self.node_mountpoint = tests.tmpdir + '/mnt/node'
        local_root.value = 'node'
        self.touch(('mnt/node/sugar-network/master', 'http://localhost:8100'))

        self.master_pid = self.popen([
            'sugar-network-server', '--port=8100', '--subscribe-port=8101',
            '--master-url=http://localhost:8100', '--data-root=master/db',
            '--index-flush-threshold=1024', '--index-flush-timeout=3',
            '--only-sync-notification', '-DDF', 'start',
            ])
        self.node_pid = self.popen([
            'sugar-network-service', '--port=8200', '--subscribe-port=8201',
            '--activity-dirs=node/Activities', '--local-root=node',
            '--mounts-root=mnt', '--server-mode',
            '--api-url=http://localhost:8100', '-DDF', 'start',
            ])

        self.wait_for_events({'event': 'mount', 'mountpoint': self.node_mountpoint})

        with Client('/') as client:
            if not client.connected:
                self.wait_for_events({'event': 'mount', 'mountpoint': '/'})

    def tearDown(self):
        self.waitpid(self.master_pid, signal.SIGINT)
        self.waitpid(self.node_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_Sneakernet(self):
        with Client(self.node_mountpoint) as client:
            context = client.Context(type='activity', title='title_1', summary='summary', description='description')
            guid_1 = context.post()
            self.touch(('preview_1', 'preview_1'))
            context.upload_blob('preview', 'preview_1')

            context = client.Context(type='activity', title='title_2', summary='summary', description='description')
            guid_2 = context.post()
            self.touch(('preview_2', 'preview_2'))
            context.upload_blob('preview', 'preview_2')

        with Client('/') as client:
            context = client.Context(type='activity', title='title_3', summary='summary', description='description')
            guid_3 = context.post()
            self.touch(('preview_3', 'preview_3'))
            context.upload_blob('preview', 'preview_3')

            context = client.Context(type='activity', title='title_4', summary='summary', description='description')
            guid_4 = context.post()
            self.touch(('preview_4', 'preview_4'))
            context.upload_blob('preview', 'preview_4')

        os.makedirs('sync1/sugar-network-sync')
        os.rename('sync1', 'mnt/sync1')
        self.wait_for_events({'event': 'sync_complete'})

        shutil.copytree('mnt/sync1', 'sync2')
        pid = self.popen('V=1 sugar-network-sync sync2', shell=True)
        self.waitpid(pid, 0)

        shutil.copytree('sync2', 'sync3')
        os.rename('sync3', 'mnt/sync3')
        self.wait_for_events({'event': 'sync_complete'})

        with Client(self.node_mountpoint) as client:
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
