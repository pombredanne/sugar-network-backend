#!/usr/bin/env python
# sugar-lint: disable

import os

from __init__ import tests

from sugar_network.client import solver


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


if __name__ == '__main__':
    tests.main()
