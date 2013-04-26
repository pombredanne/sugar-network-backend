#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import signal
import shutil
import zipfile
from os.path import exists

from __init__ import tests, src_root

from sugar_network.client import IPCClient, Client
from sugar_network.toolkit import coroutine, util


class NodeClientTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        os.makedirs('mnt')
        util.cptree(src_root + '/tests/data/node', 'node')
        self.client_pid = None

        self.node_pid = self.popen(['sugar-network-node', '-F', 'start',
            '--port=8100', '--data-root=node', '--cachedir=tmp', '-DDD',
            '--rundir=run', '--stats-node-step=0',
            ])
        coroutine.sleep(2)

    def tearDown(self):
        self.waitpid(self.node_pid, signal.SIGINT)
        if not self.client_pid:
            self.waitpid(self.client_pid, signal.SIGINT)
        tests.Test.tearDown(self)

    def test_CloneContext(self):
        context = self.cli(['POST', '/context'], stdin={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        impl = self.cli(['POST', '/implementation'], stdin={
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
                    'extract': 'topdir',
                    },
                },
            })
        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('/topdir/probe', 'ok')
        bundle.close()
        self.cli(['PUT', '/implementation/%s/data' % impl, '--post-file=bundle'])

        self.cli(['PUT', '/context/%s' % context, 'cmd=clone', '-jd1'])
        assert exists('client/Activities/topdir/probe')
        self.assertEqual('ok', file('client/Activities/topdir/probe').read())

    def test_FavoriteContext(self):
        context = self.cli(['POST', '/context'], stdin={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })

        path = 'client/db/context/%s/%s/favorite' % (context[:2], context)
        assert not exists(path)

        self.cli(['PUT', '/context/%s' % context, 'cmd=favorite', '-jdtrue'])

        assert exists(path)
        self.assertEqual(True, json.load(file(path))['value'])

    def test_UsecaseOOB(self):
        privkey_path = '.sugar/default/owner.key'
        pubkey_path = '.sugar/default/owner.key.pub'
        os.unlink(privkey_path)
        os.unlink(pubkey_path)

        deplist = self.cli(['GET', '/context/activity', 'cmd=deplist', 'repo=Fedora-14', '--anonymous', '--no-dbus', '--porcelain'])
        assert not exists(privkey_path)
        assert not exists(pubkey_path)
        self.assertEqual(
                sorted(['dep1.rpm', 'dep2.rpm', 'dep3.rpm']),
                sorted(deplist.split('\n')))

        self.cli(['PUT', '/context/context', '--anonymous', 'cmd=clone', 'nodeps=1', 'stability=stable', '-jd', '1'])
        assert not exists(privkey_path)
        assert not exists(pubkey_path)

        self.cli(['PUT', '/context/context', '--anonymous', 'cmd=favorite', '-jd', 'true'])
        assert not exists(privkey_path)
        assert not exists(pubkey_path)
        assert exists('Activities/Chat.activity/activity/activity.info')
        self.assertEqual(True, json.load(file('client/db/context/co/context/favorite'))['value'])

    def cli(self, cmd, stdin=None):
        cmd = ['sugar-network', '--local-root=client', '--ipc-port=5101', '--api-url=http://localhost:8100', '-DDD'] + cmd

        if '--anonymous' not in cmd and not self.client_pid:
            self.client_pid = self.popen(['sugar-network-client',
                '-DDDF', 'start',
                '--activity-dirs=client/Activities', '--local-root=client',
                '--mounts-root=mnt', '--cachedir=tmp', '--ipc-port=5101',
                '--api-url=http://localhost:8100',
                ])
            coroutine.sleep(2)
            ipc = Client('http://localhost:5101')
            if not ipc.get(cmd='inline'):
                self.wait_for_events(ipc, event='inline', state='online').wait()

        result = util.assert_call(cmd, stdin=json.dumps(stdin))
        if result and '--porcelain' not in cmd:
            result = json.loads(result)
        return result


if __name__ == '__main__':
    tests.main()
