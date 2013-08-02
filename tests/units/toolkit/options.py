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

    def test_SaveChangedOptions(self):
        option = Option(name='option')
        Option.seek('section', [option])

        self.touch(('config', [
            '[section]',
            'option = 1',
            ]))
        Option.load(['config'])
        self.assertEqual('1', option.value)

        option.value = '2'
        Option.save()

        option.value = ''
        Option.load(['config'])
        self.assertEqual('2', option.value)

    def test_PreserveNonSeekConfigOnSave(self):
        option = Option(name='option')
        Option.seek('section', [option])

        self.touch(('config', [
            '[foo]',
            'o1 = 1  # foo',
            '[section]',
            'option = 1',
            '[bar]',
            'o2 = 2  # bar',
            ]))
        Option.load(['config'])
        self.assertEqual('1', option.value)

        option.value = '2'
        Option.save()

        self.assertEqual('\n'.join([
            '[foo]',
            'o1 = 1  # foo',
            '',
            '[section]',
            'option = 2',
            '',
            '[bar]',
            'o2 = 2  # bar',
            '\n',
            ]),
            file('config').read())


if __name__ == '__main__':
    tests.main()
