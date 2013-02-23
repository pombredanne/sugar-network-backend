#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import shutil
import signal
from cStringIO import StringIO
from contextlib import contextmanager
from os.path import exists

import rrdtool

from __init__ import tests

from sugar_network.client import Client
from sugar_network.toolkit.sneakernet import InPacket, OutPacket
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit import sugar, util, coroutine


class MasterSlaveTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.touch(('master/db/master', 'master'))

        self.master_pid = self.popen(['sugar-network-node', '-F', 'start',
            '--port=8100', '--data-root=master/db', '--tmpdir=master/tmp',
            '-DDD', '--rundir=master/run', '--files-root=master/files',
            '--stats-root=master/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1',
            ])
        self.slave_pid = self.popen(['sugar-network-node', '-F', 'start',
            '--api-url=http://localhost:8100',
            '--port=8101', '--data-root=slave/db', '--tmpdir=slave/tmp',
            '-DDD', '--rundir=slave/run', '--files-root=slave/files',
            '--stats-root=slave/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1',
            ])
        coroutine.sleep(1)

    def tearDown(self):
        self.waitpid(self.master_pid, signal.SIGINT)
        self.waitpid(self.slave_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_OnlineSync(self):
        ts = int(time.time())
        master = Client('http://localhost:8100')
        slave = Client('http://localhost:8101')

        # Initial data

        context1 = master.post(['/context'], {
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        self.touch(('master/files/file1', 'file1'))

        context2 = slave.post(['/context'], {
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        slave.post(['user', tests.UID], {
            'name': 'db',
            'values': [(ts, {'field': 1})],
            }, cmd='stats-upload')

        # 1st sync
        slave.post(cmd='online_sync')

        self.assertEqual('title1', master.get(['context', context1, 'title']))
        self.assertEqual('title2', master.get(['context', context2, 'title']))
        stats = Rrd('master/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1', file('master/files/file1').read())

        self.assertEqual('title1', slave.get(['context', context1, 'title']))
        self.assertEqual('title2', slave.get(['context', context2, 'title']))
        stats = Rrd('slave/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1', file('slave/files/file1').read())

        # More data
        coroutine.sleep(1)

        context3 = master.post(['/context'], {
            'type': 'activity',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            })
        master.put(['context', context1, 'title'], 'title1_')
        self.touch(('master/files/file1', 'file1_'))
        self.touch(('master/files/file2', 'file2'))

        context4 = slave.post(['/context'], {
            'type': 'activity',
            'title': 'title4',
            'summary': 'summary',
            'description': 'description',
            })
        slave.put(['context', context2, 'title'], 'title2_')
        slave.post(['user', tests.UID], {
            'name': 'db',
            'values': [(ts + 1, {'field': 2})],
            }, cmd='stats-upload')

        # 2nd sync
        slave.post(cmd='online_sync')

        self.assertEqual('title1_', master.get(['context', context1, 'title']))
        self.assertEqual('title2_', master.get(['context', context2, 'title']))
        self.assertEqual('title3', master.get(['context', context3, 'title']))
        self.assertEqual('title4', master.get(['context', context4, 'title']))
        stats = Rrd('master/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0}), ('db', ts + 1, {'field': 2.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1_', file('master/files/file1').read())
        self.assertEqual('file2', file('master/files/file2').read())

        self.assertEqual('title1_', slave.get(['context', context1, 'title']))
        self.assertEqual('title2_', slave.get(['context', context2, 'title']))
        self.assertEqual('title3', slave.get(['context', context3, 'title']))
        self.assertEqual('title4', slave.get(['context', context4, 'title']))
        stats = Rrd('slave/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0}), ('db', ts + 1, {'field': 2.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1_', file('slave/files/file1').read())
        self.assertEqual('file2', file('slave/files/file2').read())

    def __test_Sneakernet(self):
        # Create shared files on master
        self.touch(('master/files/1/1', '1'))
        self.touch(('master/files/2/2', '2'))

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

            stats_timestamp = int(time.time())
            client.call('POST', 'stats-upload', mountpoint=mountpoint, document='user', guid=sugar.uid(), content={
                'name': 'db',
                'values': [(stats_timestamp + 1, {'f': 1})],
                })

        # Create node push packets with newly create data
        self.touch('mnt_2/.sugar-network-sync')
        os.rename('mnt_2', 'mnt/mnt_2')
        self.wait_for_events({'event': 'sync_complete'})

        # Upload node data to master
        shutil.copytree('mnt/mnt_2', 'mnt_3')
        pid = self.popen('V=1 mnt_3/sugar-network-sync', shell=True)
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

        self.assertEqual('1', file('node/files/1/1').read())
        self.assertEqual('2', file('node/files/2/2').read())

        master_stats = 'master/stats/%s/%s/db.rrd' % (sugar.uid()[:2], sugar.uid())
        assert exists(master_stats)
        __, __, values = rrdtool.fetch(master_stats, 'AVERAGE', '-s', str(stats_timestamp - 1), '-e', str(stats_timestamp + 1))
        self.assertEqual([(None,), (1,), (None,)], values)

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
