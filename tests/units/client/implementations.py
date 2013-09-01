#!/usr/bin/env python
# sugar-lint: disable

import os
import imp
import json
import time
import pickle
import shutil
import zipfile
import logging
from cStringIO import StringIO
from os.path import exists, dirname

from __init__ import tests

from sugar_network.client import journal, implementations
from sugar_network.toolkit import coroutine, enforce, lsb_release
from sugar_network.node import obs
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.client import IPCConnection, packagekit, solver
from sugar_network.toolkit import http, Option
from sugar_network import client


class Implementations(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        self.override(obs, 'get_repos', lambda: [])
        self.override(obs, 'presolve', lambda *args: None)

    def test_InstallDeps(self):
        self.start_online_client()
        conn = IPCConnection()

        blob = self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'requires = dep1; dep2',
            ]])
        impl = conn.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)

        conn.post(['context'], {
            'guid': 'dep1',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep1.bin']],
                    },
                },
            })
        conn.post(['context'], {
            'guid': 'dep2',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep2.bin']],
                    },
                },
            })

        def resolve(names):
            with file('resolve', 'a') as f:
                pickle.dump(names, f)
            return dict([(i, {'name': i, 'pk_id': i, 'version': '0', 'arch': '*', 'installed': i == 'dep1.bin'}) for i in names])

        def install(packages):
            with file('install', 'a') as f:
                pickle.dump([i['name'] for i in packages], f)

        self.override(packagekit, 'resolve', resolve)
        self.override(packagekit, 'install', install)
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        with file('resolve') as f:
            deps = [pickle.load(f),
                    pickle.load(f),
                    ]
            self.assertRaises(EOFError, pickle.load, f)
            self.assertEqual(
                    sorted([['dep1.bin'], ['dep2.bin']]),
                    sorted(deps))
        with file('install') as f:
            self.assertEqual(['dep2.bin'], pickle.load(f))
            self.assertRaises(EOFError, pickle.load, f)

    def test_SetExecPermissions(self):
        self.start_online_client()
        conn = IPCConnection()

        blob = self.zips(
            ['TestActivity/activity/activity.info', [
                '[Activity]',
                'name = TestActivity',
                'bundle_id = bundle_id',
                'exec = true',
                'icon = icon',
                'activity_version = 1',
                'license = Public Domain',
                ]],
            'TestActivity/activity/foo',
            'TestActivity/bin/bar',
            'TestActivity/bin/probe',
            'TestActivity/file1',
            'TestActivity/test/file2',
            )
        impl = conn.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)

        conn.put(['context', 'bundle_id'], True, cmd='clone')

        path = 'client/implementation/%s/%s/data.blob/' % (impl[:2], impl)
        assert os.access(path + 'activity/foo', os.X_OK)
        assert os.access(path + 'bin/bar', os.X_OK)
        assert os.access(path + 'bin/probe', os.X_OK)
        assert not os.access(path + 'file1', os.X_OK)
        assert not os.access(path + 'test/file2', os.X_OK)

    def test_ReuseCachedSolution(self):
        self.start_online_client()
        conn = IPCConnection()

        impl = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)
        solution = ['http://127.0.0.1:8888', ['stable'], [{
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['true'],
            'context': 'bundle_id',
            'path': tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl),
            'extract': 'TestActivity',
            'guid': impl,
            }]]
        cached_path = 'cache/solutions/bu/bundle_id'

        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])
        self.assertEqual(solution, json.load(file(cached_path)))

        os.utime(cached_path, (0, 0))
        self.assertEqual(solution, json.load(file(cached_path)))
        assert os.stat(cached_path).st_mtime == 0

    def test_InvalidaeCachedSolutions(self):
        self.start_online_client()
        conn = IPCConnection()

        conn.post(['context'], {
            'guid': 'bundle_id',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        solution = json.dumps(['http://127.0.0.1:8888', ['stable'], [{
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['true'],
            'context': 'bundle_id',
            'path': tests.tmpdir,
            'guid': 'impl',
            }]])
        cached_path = 'cache/solutions/bu/bundle_id'
        self.touch([cached_path, solution])
        cached_mtime = int(os.stat(cached_path).st_mtime)

        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        client.api_url.value = 'fake'
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])
        self.assertEqual(solution, file(cached_path).read())

        client.api_url.value = 'http://127.0.0.1:8888'
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        self.client_routes._node_mtime = cached_mtime + 2
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])
        self.assertEqual(solution, file(cached_path).read())

        self.client_routes._node_mtime = cached_mtime
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        self.override(packagekit, 'mtime', lambda: cached_mtime + 2)
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])
        self.assertEqual(solution, file(cached_path).read())

        self.override(packagekit, 'mtime', lambda: cached_mtime)
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        self.touch(('config', [
            '[stabilities]',
            'bundle_id = buggy',
            ]))
        Option.load(['config'])
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])
        self.assertEqual(solution, file(cached_path).read())

        self.touch(('config', [
            '[stabilities]',
            'bundle_id = stable',
            ]))
        Option.load(['config'])
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

    def test_DeliberateReuseCachedSolutionInOffline(self):
        self.start_online_client()
        conn = IPCConnection()

        conn.post(['context'], {
            'guid': 'bundle_id',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        solution = json.dumps(['http://127.0.0.1:8888', ['stable'], [{
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['true'],
            'context': 'bundle_id',
            'path': tests.tmpdir,
            'guid': 'impl',
            }]])
        self.touch(['cache/solutions/bu/bundle_id', solution])

        client.api_url.value = 'fake'
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])

        self.node.stop()
        coroutine.sleep(.1)
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

    def test_StabilityPreferences(self):
        self.start_online_client()
        conn = IPCConnection()

        conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)
        conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = testing',
            ]])), cmd='submit')
        conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 3',
            'license = Public Domain',
            'stability = buggy',
            ]])), cmd='submit')
        cached_path = 'cache/solutions/bu/bundle_id'

        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])
        self.assertEqual('1', json.load(file(cached_path))[2][0]['version'])

        self.touch(('config', [
            '[stabilities]',
            'bundle_id = testing',
            ]))
        Option.load(['config'])
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])
        self.assertEqual('2', json.load(file(cached_path))[2][0]['version'])

        self.touch(('config', [
            '[stabilities]',
            'bundle_id = testing buggy',
            ]))
        Option.load(['config'])
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])
        self.assertEqual('3', json.load(file(cached_path))[2][0]['version'])

        self.touch(('config', [
            '[stabilities]',
            'default = testing',
            ]))
        Option.load(['config'])
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])
        self.assertEqual('2', json.load(file(cached_path))[2][0]['version'])

    def test_LaunchContext(self):
        self.start_online_client()
        conn = IPCConnection()

        app = conn.upload(['implementation'], StringIO(self.zips(
            ['TestActivity/activity/activity.info', [
                '[Activity]',
                'name = TestActivity',
                'bundle_id = bundle_id',
                'exec = activity',
                'icon = icon',
                'activity_version = 1',
                'license = Public Domain',
                ]],
            ['TestActivity/bin/activity', [
                '#!/bin/sh',
                'cat $6',
                ]],
            )), cmd='submit', initial=True)

        conn.post(['context'], {
            'guid': 'document',
            'type': 'content',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        doc = conn.post(['implementation'], {
            'context': 'document',
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            })
        self.node_volume['implementation'].update(doc, {'data': {
            'mime_type': 'application/octet-stream',
            'blob': StringIO('content'),
            }})

        self.assertEqual('exit', [i for i in conn.get(['context', 'document'], cmd='launch', context='bundle_id')][-1]['event'])
        coroutine.sleep(.1)
        self.assertEqual('content', file('.sugar/default/logs/bundle_id.log').read())


if __name__ == '__main__':
    tests.main()
