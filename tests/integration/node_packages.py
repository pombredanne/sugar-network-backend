#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import shutil
import signal
import urllib2
from cStringIO import StringIO
from contextlib import contextmanager
from os.path import exists, join, dirname, abspath

import rrdtool

from __init__ import tests, src_root

from sugar_network import db, client
from sugar_network.client import Client, IPCClient
from sugar_network.db.router import Router, route
from sugar_network.node.obs import obs_url
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit import util, coroutine


# /tmp might be on tmpfs wich returns 0 bytes for free mem all time
local_tmproot = join(abspath(dirname(__file__)), '.tmp')


class NodePackagesSlaveTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self, tmp_root=local_tmproot)
        self.pids = []

    def tearDown(self):
        while self.pids:
            self.waitpid(self.pids.pop(), signal.SIGINT)
        tests.Test.tearDown(self)

    def test_packages(self):

        class OBS(db.CommandsProcessor):

            @route('GET', '/build')
            def build(self, request, response):
                response.content_type = 'text/xml'
                if request.path == ['build', 'base']:
                    return '<directory><entry name="Fedora-14"/></directory>'
                elif request.path == ['build', 'base', 'Fedora-14']:
                    return '<directory><entry name="i586"/></directory>'
                elif request.path == ['build', 'presolve']:
                    return '<directory><entry name="OLPC-11.3.1"/></directory>'
                elif request.path == ['build', 'presolve', 'OLPC-11.3.1']:
                    return '<directory><entry name="i586"/></directory>'

            @route('GET', '/resolve')
            def resolve(self, request, response):
                response.content_type = 'text/xml'
                return '<resolve><binary name="rpm" url="http://localhost:9999/packages/rpm" arch="arch"/></resolve>'

            @route('GET', '/packages')
            def packages(self, request, response):
                return 'package_content'

        obs = coroutine.WSGIServer(('localhost', 9999), Router(OBS()))
        coroutine.spawn(obs.serve_forever)

        # From master

        self.touch(('master/db/master', 'localhost:8100'))
        self.pids.append(self.popen([join(src_root, 'sugar-network-node'), '-F', 'start',
            '--port=8100', '--data-root=master/db', '--cachedir=master/tmp',
            '-DDD', '--rundir=master/run', '--files-root=master/files',
            '--stats-root=master/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1', '--pull-timeout=1',
            '--obs-url=http://localhost:9999',
            ]))
        coroutine.sleep(2)
        conn = Client('http://localhost:8100')

        conn.post(['/context'], {
            'guid': 'package',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'preview': 'preview',
            'layer': 'pilot',
            'type': 'package',
            'aliases': {'Fedora': {'binary': [['package']]}},
            })
        coroutine.sleep(3)

        self.assertEqual(
                '{"arch": [{"path": "rpm", "name": "rpm"}]}',
                conn.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.assertEqual(
                'package_content',
                urllib2.urlopen('http://localhost:8100/packages/presolve:OLPC-11.3.1/rpm').read())

        pid = self.popen([join(src_root, 'sugar-network-client'), '-F', 'start',
            '--api-url=http://localhost:8100', '--cachedir=master.client/tmp',
            '-DDD', '--rundir=master.client/run', '--layers=pilot',
            '--local-root=master.client', '--activity-dirs=master.client/activities',
            '--index-flush-threshold=1', '--ipc-port=8200',
            ])
        client.ipc_port.value = 8200
        ipc = IPCClient()
        coroutine.sleep(2)
        if ipc.get(cmd='status')['route'] == 'offline':
            self.wait_for_events(ipc, event='inline', state='online').wait()
        self.assertEqual(
                {"arch": [{"path": "rpm", "name": "rpm"}]},
                ipc.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.waitpid(pid, signal.SIGINT)

        # From slave

        self.pids.append(self.popen([join(src_root, 'sugar-network-node'), '-F', 'start',
            '--api-url=http://localhost:8100',
            '--port=8101', '--data-root=slave/db', '--cachedir=slave/tmp',
            '-DDD', '--rundir=slave/run', '--files-root=slave/files',
            '--stats-root=slave/stats', '--stats-user', '--stats-user-step=1',
            '--stats-user-rras=RRA:AVERAGE:0.5:1:100',
            '--index-flush-threshold=1', '--sync-layers=pilot',
            ]))
        coroutine.sleep(2)
        conn = Client('http://localhost:8101')

        conn.post(cmd='online-sync')

        self.assertEqual(
                '{"arch": [{"path": "rpm", "name": "rpm"}]}',
                conn.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.assertEqual(
                'package_content',
                urllib2.urlopen('http://localhost:8101/packages/presolve:OLPC-11.3.1/rpm').read())

        pid = self.popen([join(src_root, 'sugar-network-client'), '-F', 'start',
            '--api-url=http://localhost:8101', '--cachedir=master.client/tmp',
            '-DDD', '--rundir=master.client/run', '--layers=pilot',
            '--local-root=master.client', '--activity-dirs=master.client/activities',
            '--index-flush-threshold=1', '--ipc-port=8200',
            ])
        client.ipc_port.value = 8200
        ipc = IPCClient()
        coroutine.sleep(2)
        if ipc.get(cmd='status')['route'] == 'offline':
            self.wait_for_events(ipc, event='inline', state='online').wait()
        self.assertEqual(
                {"arch": [{"path": "rpm", "name": "rpm"}]},
                ipc.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.waitpid(pid, signal.SIGINT)

        # From personal slave

        os.makedirs('client/mnt/disk/sugar-network')
        self.pids.append(self.popen([join(src_root, 'sugar-network-client'), '-F', 'start',
            '--api-url=http://localhost:8100', '--cachedir=client/tmp',
            '-DDD', '--rundir=client/run', '--server-mode', '--layers=pilot',
            '--local-root=client', '--activity-dirs=client/activities',
            '--port=8102', '--index-flush-threshold=1',
            '--mounts-root=client/mnt', '--ipc-port=8202',
            ]))
        coroutine.sleep(2)
        conn = Client('http://localhost:8102')
        client.ipc_port.value = 8202
        ipc = IPCClient()
        if ipc.get(cmd='status')['route'] == 'offline':
            self.wait_for_events(ipc, event='inline', state='online').wait()

        pid = self.popen('V=1 %s sync/sugar-network-sync http://localhost:8100' % join(src_root, 'sugar-network-sync'), shell=True)
        self.waitpid(pid, 0)
        trigger = self.wait_for_events(ipc, event='sync_complete')
        os.rename('sync', 'client/mnt/sync')
        trigger.wait()
        pid = self.popen('V=1 client/mnt/sync/sugar-network-sync/sugar-network-sync', shell=True)
        self.waitpid(pid, 0)

        self.assertEqual(
                '{"arch": [{"path": "rpm", "name": "rpm"}]}',
                conn.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.assertEqual(
                'package_content',
                urllib2.urlopen('http://localhost:8102/packages/presolve:OLPC-11.3.1/rpm').read())

        self.assertEqual(
                {"arch": [{"path": "rpm", "name": "rpm"}]},
                ipc.get(['packages', 'presolve:OLPC-11.3.1', 'package']))
        self.assertEqual(
                'package_content',
                urllib2.urlopen('http://localhost:8202/packages/presolve:OLPC-11.3.1/rpm').read())


if __name__ == '__main__':
    tests.main()
