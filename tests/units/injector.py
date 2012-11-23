#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
import zipfile
import logging
import cPickle as pickle
from cStringIO import StringIO
from os.path import exists, dirname

from __init__ import tests

from active_toolkit import coroutine, enforce
from sugar_network import zeroinstall
from sugar_network.client import journal
from sugar_network.toolkit import pipe as pipe_
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.zerosugar import lsb_release, packagekit, injector, clones
from sugar_network import IPCClient, client as local


class InjectorTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        self.override(pipe_, 'trace', lambda *args: None)

    def test_clone_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        pipe = injector.clone('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'mountpoint': '/', 'context': context},
            {'state': 'analyze', 'mountpoint': '/', 'context': context},
            {'state': 'failure', 'mountpoint': '/', 'context': context, 'error': "Interface '%s' has no usable implementations" % context, 'log_path': log_path, 'trace': None},
            ],
            [i for i in pipe])

        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
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
                    'extract': 'topdir',
                    },
                },
            })

        pipe = injector.clone('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'fork', 'mountpoint': '/', 'context': context},
            {'state': 'analyze', 'mountpoint': '/', 'context': context},
            {'state': 'solved', 'mountpoint': '/', 'context': context},
            {'state': 'download', 'mountpoint': '/', 'context': context},
            {'state': 'failure', 'mountpoint': '/', 'context': context, 'error': 'BLOB does not exist', 'log_path': log_path, 'trace': None,
                'solution': [{'name': 'title', 'prefix': 'topdir', 'version': '1', 'command': ['echo'], 'context': context, 'mountpoint': '/', 'id': impl}],
                },
            ],
            [i for i in pipe])
        assert not exists('cache/implementation/%s' % impl)

        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, pickle.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

        pipe = injector.clone('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual([
            {'state': 'fork', 'mountpoint': '/', 'context': context},
            {'state': 'analyze', 'mountpoint': '/', 'context': context},
            {'state': 'solved', 'mountpoint': '/', 'context': context},
            {'state': 'download', 'mountpoint': '/', 'context': context},
            {'state': 'ready', 'mountpoint': '/', 'context': context},
            {'state': 'exit', 'mountpoint': '/', 'context': context},
            ],
            [i for i in pipe])
        assert exists('cache/implementation/%s' % impl)
        assert exists('Activities/topdir/probe')
        self.assertEqual('probe', file('Activities/topdir/probe').read())

        os.unlink(blob_path)
        os.unlink(blob_path + '.blob')
        shutil.rmtree('Activities')

        pipe = injector.clone('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_3.log' % context
        self.assertEqual([
            {'state': 'fork', 'mountpoint': '/', 'context': context},
            {'state': 'analyze', 'mountpoint': '/', 'context': context},
            {'state': 'solved', 'mountpoint': '/', 'context': context},
            {'state': 'ready', 'mountpoint': '/', 'context': context},
            {'state': 'exit', 'mountpoint': '/', 'context': context},
            ],
            [i for i in pipe])
        assert exists('cache/implementation/%s' % impl)
        assert exists('Activities/topdir/probe')
        self.assertEqual('probe', file('Activities/topdir/probe').read())

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
        self.touch((blob_path, pickle.dumps({})))
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

        self.override(journal, 'create_activity_id', lambda: 'activity_id')
        pipe = injector.launch('/', context)

        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'download', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            ],
            [i for i in pipe])

        impl_2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
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
        self.touch((blob_path, pickle.dumps({})))
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
        pipe = injector.launch('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'download', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': context, 'color': None, 'mountpoint': '/', 'activity_id': 'activity_id'},
            ],
            [i for i in pipe])

    def test_launch_Offline(self):
        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = title',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]))

        self.start_server()
        monitor = coroutine.spawn(clones.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        context = 'bundle_id'
        impl = tests.tmpdir + '/Activities/activity'

        pipe = injector.launch('~', context, activity_id='activity_id')
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': 'bundle_id', 'color': None, 'mountpoint': '~', 'activity_id': 'activity_id'},
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
        monitor = coroutine.spawn(clones.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        remote.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'dep1',
            'packages': {
                lsb_release.distributor_id() + '-' + lsb_release.release(): {
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
                lsb_release.distributor_id() + '-' + lsb_release.release(): {
                    'binary': ['dep2.bin'],
                    },
                },
            })

        def resolve(names):
            with file('resolve', 'w') as f:
                pickle.dump(names, f)
            return dict([(i, {'name': i, 'pk_id': i, 'version': '0', 'arch': '*', 'installed': i == 'dep1.bin'}) for i in names])

        def install(packages):
            with file('install', 'w') as f:
                pickle.dump([i['name'] for i in packages], f)

        self.override(packagekit, 'resolve', resolve)
        self.override(packagekit, 'install', install)

        context = 'bundle_id'
        pipe = injector.launch('~', context)
        self.assertEqual('exit', [i for i in pipe][-1].get('state'))
        self.assertEqual(['dep1.bin', 'dep2.bin'], pickle.load(file('resolve')))
        self.assertEqual(['dep2.bin'], pickle.load(file('install')))

    def test_SolutionsCache_Set(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(zeroinstall, 'solve', lambda *args: solution)

        self.assertEqual(solution, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file('cache/solutions/~/co/context')))

        self.assertEqual(solution, injector._solve('/', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file('cache/solutions/#/co/context')))

        self.assertEqual(solution, injector._solve('/foo/bar', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file('cache/solutions/#foo#bar/co/context')))

    def test_SolutionsCache_InvalidateByAPIUrl(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(zeroinstall, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/~/co/context'

        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch((cached_path, pickle.dumps([local.api_url.value, solution2])))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        local.api_url.value = 'fake'
        self.assertEqual(solution, injector._solve('~', 'context'))
        self.assertEqual(['fake', solution], pickle.load(file(cached_path)))

    def test_SolutionsCache_InvalidateByMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(zeroinstall, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/~/co/context'

        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        injector.invalidate_solutions(1)
        self.touch((cached_path, pickle.dumps([local.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        injector.invalidate_solutions(3)
        self.assertEqual(solution, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file(cached_path)))

    def test_SolutionsCache_InvalidateByPMSMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(zeroinstall, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/~/co/context'

        injector._pms_path = 'pms'
        self.touch('pms')
        os.utime('pms', (1, 1))
        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch((cached_path, pickle.dumps([local.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        os.utime('pms', (3, 3))
        self.assertEqual(solution, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file(cached_path)))

    def test_SolutionsCache_InvalidateBySpecMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(zeroinstall, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/~/co/context'

        solution2 = [{'spec': 'spec', 'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch('spec')
        os.utime('spec', (1, 1))
        self.touch((cached_path, pickle.dumps([local.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        os.utime('spec', (3, 3))
        self.assertEqual(solution, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution], pickle.load(file(cached_path)))

    def test_CacheReuseOnSolveFails(self):
        self.override(zeroinstall, 'solve', lambda *args: enforce(False))
        cached_path = 'cache/solutions/~/co/context'

        self.assertRaises(RuntimeError, injector._solve, '~', 'context')

        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        injector.invalidate_solutions(1)
        self.touch((cached_path, pickle.dumps([local.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

        injector.invalidate_solutions(3)
        self.assertEqual(solution2, injector._solve('~', 'context'))
        self.assertEqual([local.api_url.value, solution2], pickle.load(file(cached_path)))

    def test_clone_SetExecPermissionsForActivities(self):
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
                    'extract': 'topdir',
                    },
                },
            })
        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, pickle.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/activity/foo', '')
        bundle.writestr('topdir/bin/bar', '')
        bundle.writestr('topdir/bin/probe', '')
        bundle.writestr('topdir/file1', '')
        bundle.writestr('topdir/test/file2', '')
        bundle.close()

        pipe = injector.clone('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual('exit', [i for i in pipe][-1]['state'])
        assert os.access('Activities/topdir/activity/foo', os.X_OK)
        assert os.access('Activities/topdir/bin/bar', os.X_OK)
        assert os.access('Activities/topdir/bin/probe', os.X_OK)
        assert not os.access('Activities/topdir/file1', os.X_OK)
        assert not os.access('Activities/topdir/test/file2', os.X_OK)

    def test_launch_Arguments(self):
        forks = []
        self.override(pipe_, 'fork', lambda callback, logname, session, args, **kwargs: forks.append(args))
        self.override(journal, 'create_activity_id', lambda: 'new_activity_id')

        injector.launch('/', 'app')
        injector.launch('/', 'app', ['foo'])
        injector.launch('/', 'app', ['foo'], activity_id='activity_id', object_id='object_id', uri='uri')

        self.assertEqual([
            ['-b', 'app', '-a', 'new_activity_id'],
            ['foo', '-b', 'app', '-a', 'new_activity_id'],
            ['foo', '-b', 'app', '-a', 'activity_id', '-o', 'object_id', '-u', 'uri'],
            ],
            forks)

    def test_ProcessCommonDependencies(self):
        self.touch('remote/master')
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'dependencies': ['dep1', 'dep2'],
            })
        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'echo',
                            },
                        },
                    'requires': {
                        'dep2': {'restrictions': [['1', '2']]},
                        'dep3': {},
                    },
                },
            }})
        remote.post(['context'], {
            'implement': 'dep1',
            'type': 'package',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            'packages': {
                lsb_release.distributor_id() + '-' + lsb_release.release(): {
                    'binary': ['dep1.bin'],
                    },
                },
            })
        remote.post(['context'], {
            'implement': 'dep2',
            'type': 'package',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            'packages': {
                lsb_release.distributor_id() + '-' + lsb_release.release(): {
                    'binary': ['dep2.bin'],
                    },
                },
            })
        remote.post(['context'], {
            'implement': 'dep3',
            'type': 'package',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            'packages': {
                lsb_release.distributor_id() + '-' + lsb_release.release(): {
                    'binary': ['dep3.bin'],
                    },
                },
            })

        def resolve(names):
            return dict([(i, {'name': i, 'pk_id': i, 'version': '1', 'arch': '*', 'installed': True}) for i in names])

        self.override(packagekit, 'resolve', resolve)

        self.assertEqual(
                sorted([
                    {'version': '1', 'id': 'dep1', 'context': 'dep1', 'name': 'title1'},
                    {'version': '1', 'id': 'dep2', 'context': 'dep2', 'name': 'title2'},
                    {'version': '1', 'id': 'dep3', 'context': 'dep3', 'name': 'title3'},
                    {'name': 'title', 'version': '1', 'command': ['echo'], 'context': context, 'mountpoint': '/', 'id': impl},
                    ]),
                sorted(zeroinstall.solve('/', context)))


if __name__ == '__main__':
    tests.main()
