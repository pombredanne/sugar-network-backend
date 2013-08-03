#!/usr/bin/env python
# sugar-lint: disable

import os

from __init__ import tests

from sugar_network.client import IPCConnection, packagekit, solver, clones
from sugar_network.toolkit import lsb_release, Option


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

    def test_FirstSuccessfulSolveMighMissImplsDueToPackageDeps(self):
        self.override(packagekit, 'resolve', lambda names:
                dict([(i, {'name': i, 'pk_id': i, 'version': '0', 'arch': '*', 'installed': True}) for i in names]))

        self.touch(('Activities/1/activity/activity.info', [
            '[Activity]',
            'name = name',
            'bundle_id = bundle_id',
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]))
        self.touch(('Activities/2/activity/activity.info', [
            '[Activity]',
            'name = name',
            'bundle_id = bundle_id',
            'exec = false',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'requires = dep',
            ]))

        home_volume = self.start_online_client()
        clones.populate(home_volume['context'], ['Activities'])
        ipc = IPCConnection()

        ipc.post(['context'], {
            'guid': 'dep',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep.bin']],
                    },
                },
            })

        solution = solver.solve(ipc, 'bundle_id')
        self.assertEqual(
                2, len(solution))
        self.assertEqual(
                ('bundle_id', '2'),
                (solution[0]['context'], solution[0]['version']))
        self.assertEqual(
                ('dep', '0'),
                (solution[1]['context'], solution[1]['version']))

    def test_StabilityPreferences(self):
        self.start_online_client()
        ipc = IPCConnection()
        data = {'spec': {'*-*': {'commands': {'activity': {'exec': 'echo'}}, 'extract': 'topdir'}}}

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl1 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl1, {'data': data})
        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'testing',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl2, {'data': data})
        impl3 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '3',
            'stability': 'buggy',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl3, {'data': data})
        impl4 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '4',
            'stability': 'insecure',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl4, {'data': data})

        self.assertEqual('1', solver.solve(ipc, context)[0]['version'])

        self.touch(('config', [
            '[stabilities]',
            '%s = testing' % context,
            ]))
        Option.load(['config'])
        self.assertEqual('2', solver.solve(ipc, context)[0]['version'])

        self.touch(('config', [
            '[stabilities]',
            '%s = testing buggy' % context,
            ]))
        Option.load(['config'])
        self.assertEqual('3', solver.solve(ipc, context)[0]['version'])

        self.touch(('config', [
            '[stabilities]',
            'default = insecure',
            '%s = stable' % context,
            ]))
        Option.load(['config'])
        self.assertEqual('1', solver.solve(ipc, context)[0]['version'])

        self.touch(('config', [
            '[stabilities]',
            'default = insecure',
            ]))
        Option.load(['config'])
        self.assertEqual('4', solver.solve(ipc, context)[0]['version'])


if __name__ == '__main__':
    tests.main()
