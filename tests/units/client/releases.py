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
import hashlib
from cStringIO import StringIO
from os.path import exists, dirname

from __init__ import tests

from sugar_network.client import journal, releases, cache_limit
from sugar_network.toolkit import coroutine, lsb_release
from sugar_network.node import obs
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.release import Release
from sugar_network.client import IPCConnection, packagekit, solver
from sugar_network.toolkit import http, Option
from sugar_network import client


class Releases(tests.Test):

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
        impl = conn.upload(['release'], StringIO(blob), cmd='submit', initial=True)

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
        impl = conn.upload(['release'], StringIO(blob), cmd='submit', initial=True)

        conn.put(['context', 'bundle_id'], True, cmd='clone')

        path = 'client/release/%s/%s/data.blob/' % (impl[:2], impl)
        assert os.access(path + 'activity/foo', os.X_OK)
        assert os.access(path + 'bin/bar', os.X_OK)
        assert os.access(path + 'bin/probe', os.X_OK)
        assert not os.access(path + 'file1', os.X_OK)
        assert not os.access(path + 'test/file2', os.X_OK)

    def test_ReuseCachedSolution(self):
        self.start_online_client()
        conn = IPCConnection()

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ])
        blob = self.zips(['TestActivity/activity/activity.info', activity_info])
        impl = conn.upload(['release'], StringIO(blob), cmd='submit', initial=True)
        solution = ['http://127.0.0.1:8888', ['stable'], [{
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'context': 'bundle_id',
            'path': tests.tmpdir + '/client/release/%s/%s/data.blob' % (impl[:2], impl),
            'guid': impl,
            'layer': ['origin'],
            'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
            'ctime': self.node_volume['release'].get(impl).ctime,
            'notes': {'en-us': ''},
            'tags': [],
            'data': {
                'unpack_size': len(activity_info),
                'blob_size': len(blob),
                'digest': hashlib.sha1(blob).hexdigest(),
                'mime_type': 'application/vnd.olpc-sugar',
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                },
            }]]
        cached_path = 'solutions/bu/bundle_id'

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
            'context': 'bundle_id',
            'path': tests.tmpdir,
            'guid': 'impl',
            'data': {
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                },
            }]])
        cached_path = 'solutions/bu/bundle_id'
        self.touch([cached_path, solution])
        cached_mtime = int(os.stat(cached_path).st_mtime)

        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

        client.api.value = 'fake'
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])
        self.assertEqual(solution, file(cached_path).read())

        client.api.value = 'http://127.0.0.1:8888'
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
            'context': 'bundle_id',
            'path': tests.tmpdir,
            'guid': 'impl',
            'data': {
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                },
            }]])
        self.touch(['solutions/bu/bundle_id', solution])

        client.api.value = 'fake'
        self.assertEqual('NotFound', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['exception'])

        self.node.stop()
        coroutine.sleep(.1)
        self.assertEqual('exit', [i for i in conn.get(['context', 'bundle_id'], cmd='launch')][-1]['event'])

    def test_StabilityPreferences(self):
        self.start_online_client()
        conn = IPCConnection()

        conn.upload(['release'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)
        conn.upload(['release'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = testing',
            ]])), cmd='submit')
        conn.upload(['release'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 3',
            'license = Public Domain',
            'stability = buggy',
            ]])), cmd='submit')
        cached_path = 'solutions/bu/bundle_id'

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

        app = conn.upload(['release'], StringIO(self.zips(
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
            'type': 'book',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        doc = conn.post(['release'], {
            'context': 'document',
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            })
        self.node_volume['release'].update(doc, {'data': {
            'mime_type': 'application/octet-stream',
            'blob': StringIO('content'),
            }})

        self.assertEqual('exit', [i for i in conn.get(['context', 'document'], cmd='launch', context='bundle_id')][-1]['event'])
        coroutine.sleep(.1)
        self.assertEqual('content', file('.sugar/default/logs/bundle_id.log').read())

    def test_CreateAllImplPropsOnCheckin(self):
        home_volume = self.start_online_client()
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
            )
        impl = conn.upload(['release'], StringIO(blob), cmd='submit', initial=True)
        conn.put(['context', 'bundle_id'], True, cmd='clone')

        doc = home_volume['release'].get(impl)
        assert doc.meta('ctime') is not None
        assert doc.meta('mtime') is not None
        assert doc.meta('seqno') is not None
        self.assertEqual({tests.UID: {'name': 'test', 'order': 0, 'role': 3}}, doc.meta('author')['value'])
        self.assertEqual(['origin'], doc.meta('layer')['value'])
        self.assertEqual('bundle_id', doc.meta('context')['value'])
        self.assertEqual(['Public Domain'], doc.meta('license')['value'])
        self.assertEqual('1', doc.meta('version')['value'])
        self.assertEqual('stable', doc.meta('stability')['value'])
        self.assertEqual({'en-us': ''}, doc.meta('notes')['value'])
        self.assertEqual([], doc.meta('tags')['value'])

    def test_LaunchAcquiring(self):
        volume = self.start_online_client()
        conn = IPCConnection()

        app = conn.upload(['release'], StringIO(self.zips(
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
                'sleep 1',
                ]],
            )), cmd='submit', initial=True)

        conn.post(['context'], {
            'guid': 'document',
            'type': 'book',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        doc = conn.post(['release'], {
            'context': 'document',
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            })
        self.node_volume['release'].update(doc, {'data': {
            'mime_type': 'application/octet-stream',
            'blob': StringIO('content'),
            }})

        launch = conn.get(['context', 'document'], cmd='launch', context='bundle_id')
        self.assertEqual('launch', next(launch)['event'])
        self.assertEqual('exec', next(launch)['event'])

        class statvfs(object):
            f_blocks = 100
            f_bfree = 10
            f_frsize = 1
        self.override(os, 'statvfs', lambda *args: statvfs())
        cache_limit.value = 10

        self.assertRaises(RuntimeError, self.client_routes._cache.ensure, 1, 0)
        assert volume['release'].exists(app)
        assert volume['release'].exists(doc)
        self.assertEqual([], [i for i in self.client_routes._cache])

        self.assertEqual('exit', next(launch)['event'])
        self.assertEqual([app, doc], [i for i in self.client_routes._cache])

    def test_NoAcquiringForClones(self):
        volume = self.start_online_client()
        conn = IPCConnection()

        app = conn.upload(['release'], StringIO(self.zips(
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
                'sleep 1',
                ]],
            )), cmd='submit', initial=True)

        conn.put(['context', 'bundle_id'], True, cmd='clone')
        self.assertEqual([], [i for i in self.client_routes._cache])

        launch = conn.get(['context', 'bundle_id'], cmd='launch')
        self.assertEqual('launch', next(launch)['event'])
        self.assertEqual('exec', next(launch)['event'])
        self.assertEqual([], [i for i in self.client_routes._cache])
        self.assertEqual('exit', next(launch)['event'])
        self.assertEqual([], [i for i in self.client_routes._cache])


if __name__ == '__main__':
    tests.main()
