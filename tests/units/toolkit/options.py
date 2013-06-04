#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit import Option


class OptionsTest(tests.Test):

    def test_LoadDirs(self):
        self.touch(
                ('d/1', ['[section]', 'p1=1', 'p2=2']),
                ('d/2', ['[section]', 'p2=22', 'p3=33']),
                )

        p1 = Option(name='p1')
        p2 = Option(name='p2')
        p3 = Option(name='p3')

        Option.seek('section', [p1, p2, p3])
        Option.load(['d'])

        self.assertEqual('1', p1.value)
        self.assertEqual('22', p2.value)
        self.assertEqual('33', p3.value)


if __name__ == '__main__':
    tests.main()
