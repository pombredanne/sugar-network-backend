#!/usr/bin/env python
# sugar-lint: disable

import os

from __init__ import tests

from sugar_network.client import IPCConnection, packagekit, solver, clones
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


if __name__ == '__main__':
    tests.main()
