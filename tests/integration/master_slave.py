#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import shutil
import signal
from cStringIO import StringIO
from contextlib import contextmanager
from os.path import exists, join, dirname, abspath

import rrdtool

from __init__ import tests, src_root

from sugar_network.client import Client
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit import coroutine


# /tmp might be on tmpfs wich returns 0 bytes for free mem all time
local_tmproot = join(abspath(dirname(__file__)), '.tmp')


class MasterSlaveTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self, tmp_root=local_tmproot)

        self.touch(('master/db/master', '127.0.0.1:8100'))

        self.master_pid = self.popen([join(src_root, 'sugar-network-node'), '-F', 'start',
            '--port=8100', '--data-root=master/db', '--cachedir=master/tmp',
            '-DDD', '--rundir=master/run', '--files-root=master/files',
            '--stats-root=master/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1', '--pull-timeout=1',
            '--obs-url=',
            ])
        self.slave_pid = self.popen([join(src_root, 'sugar-network-node'), '-F', 'start',
            '--api-url=http://127.0.0.1:8100',
            '--port=8101', '--data-root=slave/db', '--cachedir=slave/tmp',
            '-DDD', '--rundir=slave/run', '--files-root=slave/files',
            '--stats-root=slave/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1', '--sync-layers=pilot',
            ])

        coroutine.sleep(3)

    def tearDown(self):
        self.waitpid(self.master_pid, signal.SIGINT)
        self.waitpid(self.slave_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_OnlineSync(self):
        ts = int(time.time())
        master = Client('http://127.0.0.1:8100')
        slave = Client('http://127.0.0.1:8101')

        # Initial data

        context1 = master.post(['/context'], {
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview1',
            'layer': 'pilot',
            })
        self.touch(('master/files/file1', 'file1'))

        context2 = slave.post(['/context'], {
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview2',
            'layer': 'pilot',
            })
        slave.post(['user', tests.UID], {
            'name': 'db',
            'values': [(ts, {'field': 1})],
            }, cmd='stats-upload')

        # 1st sync
        slave.post(cmd='online-sync')

        self.assertEqual('title1', master.get(['context', context1, 'title']))
        self.assertEqual('preview1', master.request('GET', ['context', context1, 'preview']).content)
        self.assertEqual('title2', master.get(['context', context2, 'title']))
        self.assertEqual('preview2', master.request('GET', ['context', context2, 'preview']).content)

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
            'preview': 'preview3',
            'layer': 'pilot',
            })
        master.put(['context', context1, 'title'], 'title1_')
        master.put(['context', context2, 'preview'], 'preview2_')
        self.touch(('master/files/file1', 'file1_'))
        self.touch(('master/files/file2', 'file2'))

        context4 = slave.post(['/context'], {
            'type': 'activity',
            'title': 'title4',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview4',
            'layer': 'pilot',
            })
        slave.put(['context', context2, 'title'], 'title2_')
        slave.put(['context', context1, 'preview'], 'preview1_')
        slave.post(['user', tests.UID], {
            'name': 'db',
            'values': [(ts + 1, {'field': 2})],
            }, cmd='stats-upload')

        # 2nd sync
        slave.post(cmd='online-sync')

        self.assertEqual('title1_', master.get(['context', context1, 'title']))
        self.assertEqual('preview1_', master.request('GET', ['context', context1, 'preview']).content)
        self.assertEqual('title2_', master.get(['context', context2, 'title']))
        self.assertEqual('preview2_', master.request('GET', ['context', context2, 'preview']).content)
        self.assertEqual('title3', master.get(['context', context3, 'title']))
        self.assertEqual('preview3', master.request('GET', ['context', context3, 'preview']).content)
        self.assertEqual('title4', master.get(['context', context4, 'title']))
        self.assertEqual('preview3', master.request('GET', ['context', context3, 'preview']).content)
        stats = Rrd('master/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0}), ('db', ts + 1, {'field': 2.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1_', file('master/files/file1').read())
        self.assertEqual('file2', file('master/files/file2').read())

        self.assertEqual('title1_', slave.get(['context', context1, 'title']))
        self.assertEqual('preview1_', slave.request('GET', ['context', context1, 'preview']).content)
        self.assertEqual('title2_', slave.get(['context', context2, 'title']))
        self.assertEqual('preview2_', slave.request('GET', ['context', context2, 'preview']).content)
        self.assertEqual('title3', slave.get(['context', context3, 'title']))
        self.assertEqual('preview3', slave.request('GET', ['context', context3, 'preview']).content)
        self.assertEqual('title4', slave.get(['context', context4, 'title']))
        self.assertEqual('preview4', slave.request('GET', ['context', context4, 'preview']).content)
        stats = Rrd('slave/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', ts, {'field': 1.0}), ('db', ts + 1, {'field': 2.0})]
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in stats])
        self.assertEqual('file1_', file('slave/files/file1').read())
        self.assertEqual('file2', file('slave/files/file2').read())

    def test_OfflineSync(self):
        master = Client('http://127.0.0.1:8100')
        slave = Client('http://127.0.0.1:8101')

        # Create shared files on master
        self.touch(('master/files/1/1', '1'))
        self.touch(('master/files/2/2', '2'))

        # Create initial data on master
        guid_1 = master.post(['context'], {
            'type': 'activity',
            'title': 'title_1',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview1',
            'layer': 'pilot',
            })
        guid_2 = master.post(['context'], {
            'type': 'activity',
            'title': 'title_2',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview2',
            'layer': 'pilot',
            })

        # Create initial data on slave
        guid_3 = slave.post(['context'], {
            'type': 'activity',
            'title': 'title_3',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview3',
            'layer': 'pilot',
            })
        guid_4 = slave.post(['context'], {
            'type': 'activity',
            'title': 'title_4',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview4',
            'layer': 'pilot',
            })
        stats_timestamp = int(time.time())
        slave.post(['user', tests.UID], {
            'name': 'db',
            'values': [(stats_timestamp, {'f': 1}), (stats_timestamp + 1, {'f': 2})],
            }, cmd='stats-upload')

        # Clone initial dump from master
        pid = self.popen('V=1 %s sync1/sugar-network-sync http://127.0.0.1:8100' % join(src_root, 'sugar-network-sync'), shell=True)
        self.waitpid(pid, 0)
        # Import cloned data on slave
        slave.post(cmd='offline-sync', path=tests.tmpdir + '/sync1/sugar-network-sync')
        # Upload slave initial data to master
        pid = self.popen('V=1 sync1/sugar-network-sync/sugar-network-sync', shell=True)
        self.waitpid(pid, 0)

        # Update data on master
        guid_5 = master.post(['context'], {
            'type': 'activity',
            'title': 'title_5',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview5',
            'layer': 'pilot',
            })
        master.put(['context', guid_1, 'title'], 'title_1_')
        master.put(['context', guid_3, 'preview'], 'preview3_')

        # Update data on slave
        guid_6 = slave.post(['context'], {
            'type': 'activity',
            'title': 'title_6',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview6',
            'layer': 'pilot',
            })
        slave.put(['context', guid_3, 'title'], 'title_3_')
        slave.put(['context', guid_1, 'preview'], 'preview1_')

        # Export slave changes
        slave.post(cmd='offline-sync', path=tests.tmpdir + '/sync2/sugar-network-sync')
        # Sync them with master
        pid = self.popen('V=1 sync2/sugar-network-sync/sugar-network-sync', shell=True)
        self.waitpid(pid, 0)
        # Process master's reply
        slave.post(cmd='offline-sync', path=tests.tmpdir + '/sync2/sugar-network-sync')

        self.assertEqual(
                {'total': 6, 'result': [
                    {'guid': guid_1, 'title': 'title_1_'},
                    {'guid': guid_2, 'title': 'title_2'},
                    {'guid': guid_3, 'title': 'title_3_'},
                    {'guid': guid_4, 'title': 'title_4'},
                    {'guid': guid_5, 'title': 'title_5'},
                    {'guid': guid_6, 'title': 'title_6'},
                    ]},
                master.get(['context'], reply=['guid', 'title'], layer='pilot'))
        self.assertEqual(
                {'total': 6, 'result': [
                    {'guid': guid_1, 'title': 'title_1_'},
                    {'guid': guid_2, 'title': 'title_2'},
                    {'guid': guid_3, 'title': 'title_3_'},
                    {'guid': guid_4, 'title': 'title_4'},
                    {'guid': guid_5, 'title': 'title_5'},
                    {'guid': guid_6, 'title': 'title_6'},
                    ]},
                slave.get(['context'], reply=['guid', 'title'], layer='pilot'))

        self.assertEqual('preview1_', master.request('GET', ['context', guid_1, 'preview']).content)
        self.assertEqual('preview2', master.request('GET', ['context', guid_2, 'preview']).content)
        self.assertEqual('preview3_', master.request('GET', ['context', guid_3, 'preview']).content)
        self.assertEqual('preview4', master.request('GET', ['context', guid_4, 'preview']).content)
        self.assertEqual('preview5', master.request('GET', ['context', guid_5, 'preview']).content)
        self.assertEqual('preview6', master.request('GET', ['context', guid_6, 'preview']).content)
        self.assertEqual('preview1_', slave.request('GET', ['context', guid_1, 'preview']).content)
        self.assertEqual('preview2', slave.request('GET', ['context', guid_2, 'preview']).content)
        self.assertEqual('preview3_', slave.request('GET', ['context', guid_3, 'preview']).content)
        self.assertEqual('preview4', slave.request('GET', ['context', guid_4, 'preview']).content)
        self.assertEqual('preview5', slave.request('GET', ['context', guid_5, 'preview']).content)
        self.assertEqual('preview6', slave.request('GET', ['context', guid_6, 'preview']).content)

        self.assertEqual('1', file('master/files/1/1').read())
        self.assertEqual('2', file('master/files/2/2').read())
        self.assertEqual('1', file('slave/files/1/1').read())
        self.assertEqual('2', file('slave/files/2/2').read())

        rrd = Rrd('master/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', stats_timestamp, {'f': 1.0}), ('db', stats_timestamp + 1, {'f': 2.0})],
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in rrd])
        rrd = Rrd('slave/stats/user/%s/%s' % (tests.UID[:2], tests.UID), 1)
        self.assertEqual([
            [('db', stats_timestamp, {'f': 1.0}), ('db', stats_timestamp + 1, {'f': 2.0})],
            ],
            [[(db.name,) + i for i in db.get(db.first, db.last)] for db in rrd])


if __name__ == '__main__':
    tests.main()
