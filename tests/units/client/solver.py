#!/usr/bin/env python
# sugar-lint: disable

import os
import imp

from __init__ import tests

from sugar_network.client import IPCConnection, packagekit, solver
from sugar_network.toolkit import lsb_release


class SolverTest(tests.Test):

    def test_select_architecture(self):
        host_arch = os.uname()[-1]
        all_arches = [i for i in solver.machine_ranks.keys() if i]

        self.assertEqual(host_arch, solver.select_architecture(
            sorted(all_arches, cmp=lambda x, y: cmp(solver.machine_ranks[x], solver.machine_ranks[y]))))
        self.assertEqual(host_arch, solver.select_architecture(
            sorted(all_arches, cmp=lambda x, y: cmp(solver.machine_ranks[y], solver.machine_ranks[x]))))
        self.assertEqual(host_arch, solver.select_architecture([host_arch]))
        self.assertEqual(host_arch, solver.select_architecture(['foo', host_arch, 'bar']))

    def test_ProcessCommonDependencies(self):
        self.start_online_client()
        conn = IPCConnection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'dependencies': ['dep1', 'dep2'],
            })
        impl = conn.post(['release'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['release'].update(impl, {'data': {
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
                    {'version': '1', 'guid': 'dep1', 'context': 'dep1', 'stability': 'packaged', 'license': None},
                    {'version': '1', 'guid': 'dep2', 'context': 'dep2', 'stability': 'packaged', 'license': None},
                    {'version': '1', 'guid': 'dep3', 'context': 'dep3', 'stability': 'packaged', 'license': None},
                    {'version': '1', 'context': context, 'guid': impl, 'stability': 'stable', 'license': ['GPLv3+'],
                        'layer': ['origin'],
                        'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                        'ctime': self.node_volume['release'].get(impl).ctime,
                        'notes': {'en-us': ''},
                        'tags': [],
                        'data': {'spec': {'*-*': {'commands': {'activity': {'exec': 'echo'}}, 'requires':
                            {'dep2': {'restrictions': [['1', '2']]}, 'dep3': {}}}}},
                        'requires': {'dep1': {}, 'dep2': {}}},
                    ]),
                sorted(solver.solve(self.client_routes.fallback, context, ['stable'])))

    def test_SolveSugar(self):
        self.touch(('__init__.py', ''))
        self.touch(('jarabe.py', 'class config: version = "0.94"'))
        file_, pathname_, description_ = imp.find_module('jarabe', ['.'])
        imp.load_module('jarabe', file_, pathname_, description_)

        self.start_online_client()
        conn = IPCConnection()

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

        impl = conn.post(['release'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['release'].update(impl, {'data': {
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
                },
            }})
        self.assertEqual([
            {
                'version': '1',
                'context': context,
                'guid': impl,
                'stability': 'stable',
                'license': ['GPLv3+'],
                'layer': ['origin'],
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'ctime': self.node_volume['release'].get(impl).ctime,
                'notes': {'en-us': ''},
                'tags': [],
                'data': {'spec': {'*-*': {'commands': {'activity': {'exec': 'echo'}}, 'requires': {'sugar': {}}}}}},
            {'version': '0.94', 'context': 'sugar', 'guid': 'sugar-0.94', 'stability': 'packaged', 'license': None},
            ],
            solver.solve(self.client_routes.fallback, context, ['stable']))

        self.node_volume['release'].update(impl, {'data': {
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
                },
            }})
        self.assertEqual([
            {
                'version': '1',
                'context': context,
                'guid': impl,
                'stability': 'stable',
                'license': ['GPLv3+'],
                'layer': ['origin'],
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'ctime': self.node_volume['release'].get(impl).ctime,
                'notes': {'en-us': ''},
                'tags': [],
                'data': {'spec': {'*-*': {'commands': {'activity': {'exec': 'echo'}}, 'requires':
                    {'sugar': {'restrictions': [['0.80', '0.87']]}}}}}},
            {'version': '0.86', 'context': 'sugar', 'guid': 'sugar-0.86', 'stability': 'packaged', 'license': None},
            ],
            solver.solve(self.client_routes.fallback, context, ['stable']))

    def test_StripSugarVersion(self):
        self.touch(('__init__.py', ''))
        self.touch(('jarabe.py', 'class config: version = "0.94.1"'))
        file_, pathname_, description_ = imp.find_module('jarabe', ['.'])
        imp.load_module('jarabe', file_, pathname_, description_)

        self.start_online_client()
        conn = IPCConnection()

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

        impl = conn.post(['release'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['release'].update(impl, {'data': {
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
                },
            }})
        self.assertEqual([
            {
                'version': '1',
                'context': context,
                'guid': impl,
                'stability': 'stable',
                'license': ['GPLv3+'],
                'layer': ['origin'],
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'ctime': self.node_volume['release'].get(impl).ctime,
                'notes': {'en-us': ''},
                'tags': [],
                'data': {'spec': {'*-*': {'commands': {'activity': {'exec': 'echo'}}, 'requires': {'sugar': {}}}}}},
            {'version': '0.94', 'context': 'sugar', 'guid': 'sugar-0.94', 'stability': 'packaged', 'license': None},
            ],
            solver.solve(self.client_routes.fallback, context, ['stable']))


if __name__ == '__main__':
    tests.main()
