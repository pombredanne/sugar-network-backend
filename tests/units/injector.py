#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import shutil
import zipfile
from cStringIO import StringIO
from os.path import exists, dirname

from __init__ import tests

from active_toolkit import coroutine, enforce
from sugar_network import checkin, launch, zeroinstall
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.zerosugar import lsb_release, packagekit, injector
from sugar_network.local import activities
from sugar_network import IPCClient


class InjectorTest(tests.Test):

    def test_checkin_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'error': "Interface '%s' has no usable implementations" % context, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'echo',
                            },
                        },
                    'stability': 'stable',
                    'size': 0,
                    },
                },
            })

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'error': 'Cannot download implementation', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])
        os.unlink('cache/implementation/%s/%s/data.meta' % (impl[:2], impl))

        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        assert exists('Activities/data/probe')
        self.assertEqual('probe', file('Activities/data/probe').read())

    def test_launch_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'stability': 'stable',
                    'size': 0,
                    'extract': 'TestActivitry',
                    },
                },
            })

        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()

        pipe = launch('/', context)

        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'exec', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        impl_2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'stability': 'stable',
                    'size': 0,
                    'extract': 'TestActivitry',
                    },
                },
            })

        blob_path = 'remote/implementation/%s/%s/data' % (impl_2[:2], impl_2)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license=Public Domain',
            ]))
        bundle.close()

        shutil.rmtree('cache', ignore_errors=True)
        pipe = launch('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl_2, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'exec', 'implementation': impl_2, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

    def test_MissedFeeds(self):
        self.start_server()

        context = 'fake'
        pipe = launch('~', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '~', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '~', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'error': 'Cannot find feed(s) for %s' % context, 'mountpoint': '~', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

    def test_launch_Offline(self):
        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]))

        self.start_server()
        monitor = coroutine.spawn(activities.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        context = 'bundle_id'
        impl = tests.tmpdir + '/Activities/activity'

        pipe = launch('~', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '~', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '~', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl, 'mountpoint': '~', 'context': context, 'log_path': log_path},
            {'state': 'exec', 'implementation': impl, 'mountpoint': '~', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

    def test_InstallDeps(self):
        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'requires = dep1; dep2',
            ]))

        self.touch('remote/master')
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')
        monitor = coroutine.spawn(activities.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        remote.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'dep1',
            'packages': {
                lsb_release.distributor_id(): {
                    'binary': ['dep1.bin'],
                    },
                },
            })

        remote.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'dep2',
            'packages': {
                lsb_release.distributor_id(): {
                    'binary': ['dep2.bin'],
                    },
                },
            })

        def resolve(names):
            with file('resolve', 'w') as f:
                json.dump(names, f)
            return dict([(i, {'name': i, 'pk_id': i, 'version': '0', 'arch': '*', 'installed': i == 'dep1.bin'}) for i in names])

        def install(packages):
            with file('install', 'w') as f:
                json.dump([i['name'] for i in packages], f)

        self.override(packagekit, 'resolve', resolve)
        self.override(packagekit, 'install', install)

        context = 'bundle_id'
        pipe = launch('~', context)
        self.assertEqual('exec', [i for i in pipe][-1].get('state'))
        self.assertEqual(['dep1.bin', 'dep2.bin'], json.load(file('resolve')))
        self.assertEqual(['dep2.bin'], json.load(file('install')))

    def test_CacheSet(self):
        self.override(zeroinstall, 'solve', lambda *args: 'solved')

        self.assertEqual('solved', injector._solve('~', 'context'))
        self.assertEqual('solved', json.load(file('cache/solutions/~/context')))

        self.assertEqual('solved', injector._solve('/', 'context'))
        self.assertEqual('solved', json.load(file('cache/solutions/\\/context')))

        self.assertEqual('solved', injector._solve('/foo/bar', 'context'))
        self.assertEqual('solved', json.load(file('cache/solutions/\\foo\\bar/context')))

    def test_CacheGet(self):
        self.override(zeroinstall, 'solve', lambda *args: 'solved')

        cached_path = 'cache/solutions/~/context'
        self.touch((cached_path, '"cached"'))
        os.utime(cached_path, (1, 1))
        os.utime(dirname(cached_path), (1, 1))
        self.assertEqual('cached', injector._solve('~', 'context'))
        self.assertEqual('cached', json.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual('cached', injector._solve('~', 'context'))
        self.assertEqual('cached', json.load(file(cached_path)))

        os.utime(dirname(cached_path), (3, 3))
        self.assertEqual('solved', injector._solve('~', 'context'))
        self.assertEqual('solved', json.load(file(cached_path)))

    def test_CacheReuseOnSolveFails(self):
        self.override(zeroinstall, 'solve', lambda *args: enforce(False))

        self.assertRaises(RuntimeError, injector._solve, '~', 'context')

        cached_path = 'cache/solutions/~/context'
        self.touch((cached_path, '"cached"'))
        os.utime(cached_path, (1, 1))
        os.utime(dirname(cached_path), (1, 1))
        self.assertEqual('cached', injector._solve('~', 'context'))
        self.assertEqual('cached', json.load(file(cached_path)))

        os.utime(dirname(cached_path), (3, 3))
        self.assertEqual('cached', injector._solve('~', 'context'))
        self.assertEqual('cached', json.load(file(cached_path)))


if __name__ == '__main__':
    tests.main()
