#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import signal
import shutil
import zipfile
from os.path import exists, join, dirname, abspath

from __init__ import tests, src_root

from sugar_network import toolkit
from sugar_network.client import Connection
from sugar_network.toolkit import coroutine


class NodeClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        os.makedirs('mnt')
        toolkit.cptree(src_root + '/tests/data/node', 'node')
        self.client_pid = None

        self.node_pid = self.popen([join(src_root, 'sugar-network-node'), '-F', 'start',
            '--port=8100', '--data-root=node', '--cachedir=tmp', '-DDD',
            '--rundir=run', '--stats-node-step=0',
            ])
        coroutine.sleep(2)

    def tearDown(self):
        self.waitpid(self.node_pid, signal.SIGINT)
        if not self.client_pid:
            self.waitpid(self.client_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_ReleaseActivity(self):
        blob1 = self.zips(['TestActivitry/activity/activity.info', [
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = activity2',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            ]])
        with file('bundle', 'wb') as f:
            f.write(blob1)
        impl1 = self.cli(['release', 'bundle', '--porcelain', 'initial'])

        blob2 = self.zips(['TestActivitry/activity/activity.info', [
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = activity2',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = stable',
            ]])
        with file('bundle', 'wb') as f:
            f.write(blob2)
        impl2 = self.cli(['release', 'bundle', '--porcelain'])

        self.assertEqual([
            {'guid': impl1, 'version': '1', 'stability': 'developer', 'license': ['Public Domain']},
            {'guid': impl2, 'version': '2', 'stability': 'stable', 'license': ['Public Domain']},
            ],
            self.cli(['GET', '/release', 'context=activity2', 'reply=guid,version,stability,license', 'order_by=version'])['result'])
        assert blob1 == file('node/release/%s/%s/data.blob' % (impl1[:2], impl1)).read()
        assert blob2 == file('node/release/%s/%s/data.blob' % (impl2[:2], impl2)).read()

    def test_ReleaseContext(self):
        context = self.cli(['POST', '/context'], stdin={
            'type': 'book',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        blob1 = 'content1'
        with file('bundle', 'wb') as f:
            f.write(blob1)
        impl1 = self.cli(['release', 'bundle', '--porcelain',
            'context=%s' % context,
            'license=GPLv3+',
            'version=1',
            'stability=developer',
            ])

        blob2 = 'content2'
        with file('bundle', 'wb') as f:
            f.write(blob2)
        impl2 = self.cli(['release', 'bundle', '--porcelain',
            'context=%s' % context,
            'license=GPLv3+',
            'version=2',
            'stability=stable',
            ])

        self.assertEqual([
            {'guid': impl1, 'version': '1', 'stability': 'developer', 'license': ['GPLv3+']},
            {'guid': impl2, 'version': '2', 'stability': 'stable', 'license': ['GPLv3+']},
            ],
            self.cli(['GET', '/release', 'context=%s' % context, 'reply=guid,version,stability,license', 'order_by=version'])['result'])
        assert blob1 == file('node/release/%s/%s/data.blob' % (impl1[:2], impl1)).read()
        assert blob2 == file('node/release/%s/%s/data.blob' % (impl2[:2], impl2)).read()

    def test_CloneContext(self):
        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        with file('bundle', 'wb') as f:
            f.write(self.zips(['TestActivitry/activity/activity.info', activity_info]))
        impl = self.cli(['release', 'bundle', '--porcelain', 'initial'])

        self.cli(['PUT', '/context/bundle_id', 'cmd=clone', '-jd1'])
        self.assertEqual(
                activity_info,
                file('client/db/release/%s/%s/data.blob/activity/activity.info' % (impl[:2], impl)).read())

    def test_UsecaseOOB(self):
        self.cli(['--quiet', 'PUT', '/context/context', 'cmd=clone', '-jd', 'true'])
        assert exists('client/db/release/im/release/data.blob/activity/activity.info')
        self.assertEqual(['clone'], json.load(file('client/db/context/co/context/layer'))['value'])

    def cli(self, cmd, stdin=None):
        cmd = ['sugar-network', '--local-root=client', '--ipc-port=5101', '--api-url=http://127.0.0.1:8100', '-DDD'] + cmd

        if '--anonymous' not in cmd and not self.client_pid:
            self.client_pid = self.popen([join(src_root, 'sugar-network-client'),
                '-DDDF', 'start',
                '--local-root=client',
                '--mounts-root=mnt', '--cachedir=tmp', '--ipc-port=5101',
                '--api-url=http://127.0.0.1:8100',
                ])
            coroutine.sleep(2)
            ipc = Connection('http://127.0.0.1:5101')
            if ipc.get(cmd='whoami')['route'] == 'offline':
                self.wait_for_events(ipc, event='inline', state='online').wait()

        result = toolkit.assert_call(cmd, stdin=json.dumps(stdin))
        if result and '--porcelain' not in cmd:
            result = json.loads(result)
        return result


if __name__ == '__main__':
    tests.main()
