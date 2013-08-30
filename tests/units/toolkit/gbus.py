#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit import gbus


class GbusTest(tests.Test):

    def test_call(self):

        def op(result, arg):
            result.set(arg)

        self.assertEqual('probe', gbus.call(op, 'probe'))

    def test_pipe(self):

        def op(pipe, args):
            for i in args:
                pipe(i)
            pipe()

        self.assertEqual(
                [1, 2, 3],
                [i for i in gbus.pipe(op, [1, 2, 3])])


if __name__ == '__main__':
    tests.main()
