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

from sugar_network.client import journal
from sugar_network.toolkit import coroutine, enforce, pipe as pipe_, lsb_release
from sugar_network.node import obs
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.client import IPCClient, packagekit, injector, clones, solver
from sugar_network import client


class InjectorTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        self.override(pipe_, 'trace', lambda *args: None)
        self.override(obs, 'get_repos', lambda: [])
        self.override(obs, 'presolve', lambda *args: None)

    def test_clone(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        pipe = injector.clone(context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context},
            {'state': 'analyze', 'context': context},
            {'state': 'failure', 'context': context, 'error': "Interface '%s' has no usable implementations" % context, 'log_path': log_path, 'trace': None},
            ],
            [i for i in pipe])

        impl = conn.post(['implementation'], {
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

        pipe = injector.clone(context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context},
            {'state': 'analyze', 'context': context},
            {'state': 'solved', 'context': context},
            {'state': 'download', 'context': context},
            {'state': 'failure', 'context': context, 'error': 'BLOB does not exist', 'log_path': log_path, 'trace': None,
                'solution': [{'name': 'title', 'prefix': 'topdir', 'version': '1', 'command': ['echo'], 'context': context, 'id': impl, 'stability': 'stable'}],
                },
            ],
            [i for i in pipe])
        assert not exists('cache/implementation/%s' % impl)

        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

        pipe = injector.clone(context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context},
            {'state': 'analyze', 'context': context},
            {'state': 'solved', 'context': context},
            {'state': 'download', 'context': context},
            {'state': 'ready', 'context': context},
            {'state': 'exit', 'context': context},
            ],
            [i for i in pipe])
        assert exists('cache/implementation/%s' % impl)
        assert exists('Activities/topdir/probe')
        self.assertEqual('probe', file('Activities/topdir/probe').read())

    def test_clone_impl(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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
                    'extract': 'topdir',
                    },
                },
            })
        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

        pipe = injector.clone_impl(context, impl, {'*-*': {'extract': 'topdir'}})
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context},
            {'state': 'download', 'context': context},
            {'state': 'exit', 'context': context},
            ],
            [i for i in pipe])
        assert exists('cache/implementation/%s' % impl)
        assert exists('Activities/topdir/probe')
        self.assertEqual('probe', file('Activities/topdir/probe').read())

    def test_clone_CachedSolutionPointsToClonedPath(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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
        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

        for event in injector.clone(context):
            pass
        self.assertEqual('exit', event['state'])
        __, (solution,) = json.load(file('cache/solutions/%s/%s' % (context[:2], context)))
        self.assertEqual(tests.tmpdir + '/Activities/topdir', solution['path'])

    def test_launch_Online(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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

        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
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
        pipe = injector.launch(context)

        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'download', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            ],
            [i for i in pipe])

        impl_2 = conn.post(['implementation'], {
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

        blob_path = 'master/implementation/%s/%s/data' % (impl_2[:2], impl_2)
        self.touch((blob_path, json.dumps({})))
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
        pipe = injector.launch(context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'download', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': context, 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': context, 'color': None, 'activity_id': 'activity_id'},
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

        home_volume = self.start_offline_client()
        monitor = coroutine.spawn(clones.monitor, home_volume['context'], ['Activities'])
        coroutine.sleep()

        context = 'bundle_id'
        impl = tests.tmpdir + '/Activities/activity'

        pipe = injector.launch(context, activity_id='activity_id')
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'fork', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            {'state': 'analyze', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            {'state': 'solved', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            {'state': 'ready', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exec', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            {'state': 'exit', 'context': 'bundle_id', 'color': None, 'activity_id': 'activity_id'},
            ],
            [i for i in pipe])

    def test_InstallDeps(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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
                    'extract': 'topdir',
                    'requires': {
                        'dep1': {},
                        'dep2': {},
                        },
                    },
                },
            })
        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

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

        pipe = injector.launch(context)
        self.assertEqual('exit', [i for i in pipe][-1].get('state'))
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

    def test_SolutionsCache_Set(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.override(solver, 'solve', lambda *args: solution)

        self.assertEqual(solution, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution], json.load(file('cache/solutions/co/context')))

    def test_SolutionsCache_InvalidateByAPIUrl(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.override(solver, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/co/context'

        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        client.api_url.value = 'fake'
        self.assertEqual(solution, injector._solve('context'))
        self.assertEqual(['fake', solution], json.load(file(cached_path)))

    def test_SolutionsCache_InvalidateByMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.override(solver, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/co/context'

        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        injector.invalidate_solutions(1)
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        injector.invalidate_solutions(3)
        self.assertEqual(solution, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution], json.load(file(cached_path)))

    def test_SolutionsCache_InvalidateByPMSMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.override(solver, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/co/context'

        injector._pms_path = 'pms'
        self.touch('pms')
        os.utime('pms', (1, 1))
        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        os.utime('pms', (3, 3))
        self.assertEqual(solution, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution], json.load(file(cached_path)))

    def test_SolutionsCache_DeliberateReuseInOffline(self):
        solution1 = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        solution2 = [{'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.override(solver, 'solve', lambda *args: solution1)
        cached_path = 'cache/solutions/co/context'

        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        injector.invalidate_solutions(2)
        self.assertEqual(solution1, injector._solve('context'))

        self.override(client, 'IPCClient', lambda: _FakeConnection(False))
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        injector.invalidate_solutions(2)
        self.assertEqual(solution2, injector._solve('context'))


    def test_SolutionsCache_InvalidateBySpecMtime(self):
        solution = [{'name': 'name', 'context': 'context', 'id': 'id', 'version': 'version'}]
        self.override(client, 'IPCClient', lambda: _FakeConnection(True))
        self.override(solver, 'solve', lambda *args: solution)
        cached_path = 'cache/solutions/co/context'

        solution2 = [{'spec': 'spec', 'name': 'name2', 'context': 'context2', 'id': 'id2', 'version': 'version2'}]
        self.touch('spec')
        os.utime('spec', (1, 1))
        self.touch((cached_path, json.dumps([client.api_url.value, solution2])))
        os.utime(cached_path, (1, 1))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        os.utime(cached_path, (2, 2))
        self.assertEqual(solution2, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution2], json.load(file(cached_path)))

        os.utime('spec', (3, 3))
        self.assertEqual(solution, injector._solve('context'))
        self.assertEqual([client.api_url.value, solution], json.load(file(cached_path)))

    def test_clone_SetExecPermissionsForActivities(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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
        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/activity/foo', '')
        bundle.writestr('topdir/bin/bar', '')
        bundle.writestr('topdir/bin/probe', '')
        bundle.writestr('topdir/file1', '')
        bundle.writestr('topdir/test/file2', '')
        bundle.close()

        pipe = injector.clone(context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual('exit', [i for i in pipe][-1]['state'])
        assert os.access('Activities/topdir/activity/foo', os.X_OK)
        assert os.access('Activities/topdir/bin/bar', os.X_OK)
        assert os.access('Activities/topdir/bin/probe', os.X_OK)
        assert not os.access('Activities/topdir/file1', os.X_OK)
        assert not os.access('Activities/topdir/test/file2', os.X_OK)

    def test_clone_InvalidateSolutionByAbsentImpls(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = conn.post(['implementation'], {
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
        blob_path = 'master/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, json.dumps({})))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()

        for event in injector.clone(context):
            pass
        self.assertEqual('exit', event['state'])
        shutil.rmtree('Activities/topdir')

        for event in injector.clone(context):
            pass
        self.assertEqual('exit', event['state'])
        assert exists('Activities/topdir')

    def test_launch_Arguments(self):
        forks = []
        self.override(pipe_, 'fork', lambda callback, log_path, session, args=None, **kwargs: forks.append(args))
        self.override(journal, 'create_activity_id', lambda: 'new_activity_id')

        injector.launch('app')
        injector.launch('app', ['foo'])
        injector.launch('app', ['foo'], activity_id='activity_id', object_id='object_id', uri='uri')

        self.assertEqual([
            ['-b', 'app', '-a', 'new_activity_id'],
            ['foo', '-b', 'app', '-a', 'new_activity_id'],
            ['foo', '-b', 'app', '-a', 'activity_id', '-o', 'object_id', '-u', 'uri'],
            ],
            forks)

    def test_ProcessCommonDependencies(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'dependencies': ['dep1', 'dep2'],
            })
        impl = conn.post(['implementation'], {
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
        conn.post(['context'], {
            'guid': 'dep1',
            'type': 'package',
            'title': 'title1',
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
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep2.bin']],
                    },
                },
            })
        conn.post(['context'], {
            'guid': 'dep3',
            'type': 'package',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep3.bin']],
                    },
                },
            })

        def resolve(names):
            return dict([(i, {'name': i, 'pk_id': i, 'version': '1', 'arch': '*', 'installed': True}) for i in names])

        self.override(packagekit, 'resolve', resolve)

        self.assertEqual(
                sorted([
                    {'version': '1', 'id': 'dep1', 'context': 'dep1', 'name': 'title1', 'stability': 'packaged'},
                    {'version': '1', 'id': 'dep2', 'context': 'dep2', 'name': 'title2', 'stability': 'packaged'},
                    {'version': '1', 'id': 'dep3', 'context': 'dep3', 'name': 'title3', 'stability': 'packaged'},
                    {'name': 'title', 'version': '1', 'command': ['echo'], 'context': context, 'id': impl, 'stability': 'stable'},
                    ]),
                sorted(solver.solve(conn, context)))

    def test_LoadFeed_SetPackages(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.post(['implementation'], {
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
                        'dep': {},
                    },
                },
            }})
        conn.post(['context'], {
            'guid': 'dep',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        def resolve(names):
            return dict([(i, {'name': i, 'pk_id': i, 'version': '1', 'arch': '*', 'installed': True}) for i in names])
        self.override(packagekit, 'resolve', resolve)

        self.assertRaises(RuntimeError, solver.solve, conn, context)

        conn.put(['context', 'dep', 'aliases'], {
            lsb_release.distributor_id(): {
                'status': 'success',
                'binary': [['bin']],
                },
            })
        self.assertEqual('dep', solver.solve(conn, context)[-1]['context'])

        conn.put(['context', 'dep', 'aliases'], {
            'foo': {
                'status': 'success',
                'binary': [['bin']],
                },
            })
        self.assertRaises(RuntimeError, solver.solve, conn, context)

        conn.put(['context', 'dep', 'aliases'], {
            lsb_release.distributor_id(): {
                'binary': [['bin']],
                },
            })
        self.assertEqual('dep', solver.solve(conn, context)[-1]['context'])

    def test_SolveSugar(self):
        self.touch(('__init__.py', ''))
        self.touch(('jarabe.py', 'class config: version = "0.94"'))
        file_, pathname_, description_ = imp.find_module('jarabe', ['.'])
        imp.load_module('jarabe', file_, pathname_, description_)

        self.start_online_client([User, Context, Implementation])
        conn = IPCClient()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.post(['context'], {
            'guid': 'sugar',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        impl = conn.post(['implementation'], {
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
                        'sugar': {},
                    },
                },
            }})
        self.assertEqual([
            {'name': 'title', 'version': '1', 'command': ['echo'], 'context': context, 'id': impl, 'stability': 'stable'},
            {'name': 'sugar', 'version': '0.94', 'context': 'sugar', 'path': '/', 'id': 'sugar-0.94', 'stability': 'packaged'},
            ],
            solver.solve(conn, context))

        conn.put(['implementation', impl], {
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'echo',
                            },
                        },
                    'requires': {
                        'sugar': {'restrictions': [['0.80', '0.87']]},
                    },
                },
            }})
        self.assertEqual([
            {'name': 'title', 'version': '1', 'command': ['echo'], 'context': context, 'id': impl, 'stability': 'stable'},
            {'name': 'sugar', 'version': '0.86', 'context': 'sugar', 'path': '/', 'id': 'sugar-0.86', 'stability': 'packaged'},
            ],
            solver.solve(conn, context))


class _FakeConnection(object):

    def __init__(self, inline):
        self.inline = inline

    def get(self, cmd=None, *args, **kwargs):
        if cmd == 'inline':
            return self.inline


if __name__ == '__main__':
    tests.main()
