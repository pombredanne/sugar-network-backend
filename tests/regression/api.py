#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import signal
import shutil
import zipfile
from cStringIO import StringIO
from os.path import exists, join, dirname, abspath

from __init__ import tests, src_root

from sugar_network import toolkit, client, node
from sugar_network.toolkit import http, coroutine


PROD_ROOT = join(dirname(abspath(__file__)), 'production')


class Api(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.start_online_client()
        ipc = client.IPCConnection()

        ipc.upload(['release'], StringIO(
            self.zips(
                ['activity1.activity/activity/activity.info', [
                    '[Activity]',
                    'name = activity1',
                    'bundle_id = context1',
                    'exec = activity',
                    'icon = icon',
                    'activity_version = 1',
                    'license = Public Domain',
                    ]],
                ['activity1.activity/bin/activity', [
                    '#!/bin/sh',
                    'echo 1 > $1',
                    ]],
                )),
            cmd='submit', initial=True)
        ipc.upload(['release'], StringIO(
            self.zips(
                ['activity1.activity/activity/activity.info', [
                    '[Activity]',
                    'name = activity1',
                    'bundle_id = context1',
                    'exec = activity',
                    'icon = icon',
                    'activity_version = 2',
                    'license = Public Domain',
                    ]],
                ['activity1.activity/bin/activity', [
                    '#!/bin/sh',
                    'echo 2 > $1',
                    ]],
                )),
            cmd='submit')

        ipc.upload(['release'], StringIO(
            self.zips(
                ['activity2.activity/activity/activity.info', [
                    '[Activity]',
                    'name = activity2',
                    'bundle_id = context2',
                    'exec = activity',
                    'icon = icon',
                    'activity_version = 1',
                    'license = Public Domain',
                    ]],
                ['activity2.activity/bin/activity', [
                    '#!/bin/sh',
                    'echo 3 > $1',
                    ]],
                )),
            cmd='submit', initial=True)
        ipc.upload(['release'], StringIO(
            self.zips(
                ['activity2.activity/activity/activity.info', [
                    '[Activity]',
                    'name = activity2',
                    'bundle_id = context2',
                    'exec = activity',
                    'icon = icon',
                    'activity_version = 2',
                    'license = Public Domain',
                    ]],
                ['activity2.activity/bin/activity', [
                    '#!/bin/sh',
                    'echo 4 > $1',
                    ]],
                )),
            cmd='submit')

        self.client.close()
        client.ipc_port.value = 5001
        self.client_pid = self.popen([join(PROD_ROOT, 'sugar-network-client'),
            '-DDDF', 'start',
            '--local-root=client', '--mounts-root=mnt', '--cachedir=tmp',
            '--ipc-port=%s' % client.ipc_port.value, '--api=%s' % client.api.value,
            ])

    def tearDown(self):
        self.waitpid(self.client_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_API(self):
        ipc = http.Connection('http://127.0.0.1:%s' % client.ipc_port.value)

        self.assertEqual(True, ipc.get(cmd='inline'))
        self.assertEqual({'total': 2, 'result': [
            {'guid': 'context1', 'title': 'activity1'},
            {'guid': 'context2', 'title': 'activity2'},
            ]},
            ipc.get(['context'], reply=['guid', 'title'], order_by='guid'))

        ipc.get(['context', 'context1'], cmd='launch', args=[tests.tmpdir + '/out'])
        coroutine.sleep(1)
        self.assertEqual('2\n', file('out').read())

        ipc.put(['context', 'context2'], '1', 'cmd=clone')
        self.node.stop()

        ipc.get(['context', 'context2'], cmd='launch', args=[tests.tmpdir + '/out'])
        coroutine.sleep(1)
        self.assertEqual('4\n', file('out').read())


if __name__ == '__main__':
    tests.main()
