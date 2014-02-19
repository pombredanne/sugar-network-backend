#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit import sat


class SAT(tests.Test):

    def test_AtMostOne(self):
        self.assertEqual(
                {'c': 1},
                sat.solve([[1]], {'c': [1]}, decide))
        self.assertEqual(
                {'c': 1},
                sat.solve([[1, 2]], {'c': [1, 2]}, decide))
        self.assertEqual(
                {'c': 1},
                sat.solve([[1, 2, 3]], {'c': [1, 2, 3]}, decide))

    def test_DeepSolve(self):
        self.assertEqual({
            'c1': 1,
            'c2': 2,
            'c3': 3,
            'c4': 5,
            },
            sat.solve(
                [
                    [1, 6], [-1, 2],
                    [-2, 3],
                    [-3, 5],
                    ],
                {
                    'c1': [1, 6],
                    'c2': [2],
                    'c3': [3],
                    'c4': [4, 5],
                    },
                decide))

    def test_SwitchToAnotherBranch(self):
        self.assertEqual({
            'c1': 6,
            'c4': 4,
            },
            sat.solve(
                [
                    [1, 6], [-1, 2], [-6, 4],
                    [-2, 3], [-2, 4],
                    [-3, 5],
                    ],
                {
                    'c1': [1, 6],
                    'c2': [2],
                    'c3': [3],
                    'c4': [4, 5],
                    },
                decide))

    def __test_zi(self):
        from zeroinstall.injector import sat

        problem = sat.Problem()

        v1 = problem.add_variable(1)
        v2 = problem.add_variable(2)
        v3 = problem.add_variable(3)
        v4 = problem.add_variable(4)
        v5 = problem.add_variable(5)
        v6 = problem.add_variable(6)

        c1 = problem.at_most_one([v1, v6])
        problem.add_clause([v1, v6])
        problem.add_clause([sat.neg(v1), v2])
        problem.add_clause([sat.neg(v6), v4])

        c2 = problem.at_most_one([v2])
        problem.add_clause([sat.neg(v2), v3])
        problem.add_clause([sat.neg(v2), v4])

        c3 = problem.at_most_one([v3])
        problem.add_clause([sat.neg(v3), v5])

        c4 = problem.at_most_one([v4, v5])

        assert problem.run_solver(lambda: decide({'c1': c1, 'c2': c2, 'c3': c3, 'c4': c4}))
        self.assertEqual(v6, c1.current)
        self.assertEqual(None, c2.current)
        self.assertEqual(None, c3.current)
        self.assertEqual(v4, c4.current)


def decide(clauses):
    for i in clauses.values():
        if i.current is None:
            r = i.best_undecided()
            if r is not None:
                return r


if __name__ == '__main__':
    tests.main()
