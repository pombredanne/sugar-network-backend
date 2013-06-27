#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from sugar_network.toolkit import spec


class SpecTest(tests.Test):

    def test_Dependency(self):
        self.assertEqual(
                [],
                [i for i in spec._Dependency().versions_range()])
        self.assertEqual(
                [],
                [i for i in spec._Dependency({'restrictions': []}).versions_range()])
        self.assertEqual(
                ['1'],
                [i for i in spec._Dependency({'restrictions': [('1', '2')]}).versions_range()])
        self.assertEqual(
                ['1.2'],
                [i for i in spec._Dependency({'restrictions': [('1.2', '1.2.999')]}).versions_range()])
        self.assertEqual(
                ['1.2', '1.3'],
                [i for i in spec._Dependency({'restrictions': [('1.2', '1.4')]}).versions_range()])
        self.assertEqual(
                ['1.2.3', '1.3'],
                [i for i in spec._Dependency({'restrictions': [('1.2.3', '1.4')]}).versions_range()])
        self.assertEqual(
                ['1.2', '1.3', '1.4'],
                [i for i in spec._Dependency({'restrictions': [('1.2', '1.4.5')]}).versions_range()])

    def test_parse_requires(self):
        self.assertEqual(
                {'a': {}, 'b': {}, 'c': {}},
                spec._parse_requires('a; b; c'))

        self.assertEqual(
                {
                    'a': {'restrictions': [('1', '2')]},
                    'b': {'restrictions': [('1.2', '1.3')]},
                    'c': {'restrictions': [('2.2', None)]},
                    'd': {'restrictions': [(None, '3')]},
                    },
                spec._parse_requires('a = 1; b=1.2; c>= 2.2; d <3-3'))

        self.assertEqual(
                {
                    'a': {'importance': 'recommended'},
                    'b': {},
                    'c': {'importance': 'recommended', 'restrictions': [(None, '1')]},
                    },
                spec._parse_requires('[a]; b; [c<1]'))

    def test_parse_bindings(self):
        self.assertEqual(
                [
                    ('bind1', '', 'prepend'),
                    ('bind2', '', 'prepend'),
                    ('bind3', '', 'prepend'),
                    ],
                spec._parse_bindings(' bind1; prepend bind2;bind3  '))

        self.assertEqual(
                [
                    ('bind1', '', 'append'),
                    ('bind2', 'foo', 'append'),
                    ],
                spec._parse_bindings('append bind1; append bind2 foo'))

        self.assertEqual(
                [
                    ('bind1', '', 'replace'),
                    ('bind2', 'foo', 'replace'),
                    ],
                spec._parse_bindings('replace bind1; replace bind2 foo'))

    def test_ActivityInfo(self):
        stream = StringIO()
        stream.write('\n'.join([
            '[Activity]',
            'name = Terminal',
            'activity_version = 35',
            'bundle_id = org.laptop.Terminal',
            'exec = sugar-activity terminal.TerminalActivity',
            'icon = activity-terminal',
            'mime_types = image/png;image/svg+xml',
            'license = GPLv2+',
            'tags = terminal; console',
            'requires = sugar = 0.94',
            ]))
        stream.seek(0)

        recipe = spec.Spec(stream)
        self.assertEqual('Terminal', recipe['name'])
        self.assertEqual('Terminal', recipe['summary'])
        self.assertEqual('Terminal', recipe['description'])
        self.assertEqual(['GPLv2+'], recipe['license'])
        self.assertEqual('http://wiki.sugarlabs.org/go/Activities/Terminal', recipe['homepage'])
        self.assertEqual('activity/activity-terminal.svg', recipe['icon'])
        self.assertEqual('35', recipe['version'])
        self.assertEqual('stable', recipe['stability'])
        self.assertEqual(['terminal', 'console'], recipe['tags'])
        self.assertEqual(['image/png', 'image/svg+xml'], recipe['mime_types'])
        self.assertEqual(
                {
                    'activity': {
                        'exec': 'sugar-activity terminal.TerminalActivity',
                        },
                    },
                recipe.commands)
        self.assertEqual(
                {
                    'sugar': {
                        'restrictions': [('0.94', '0.95')],
                        },
                    },
                recipe.requires)

    def testVersions(self):

        def pv(v):
            parsed = spec.parse_version(v)
            self.assertEqual(v, spec.format_version(parsed))
            return parsed

        assert pv('1.0') > pv('0.9')
        assert pv('1.0') > pv('1')
        assert pv('1.0') == pv('1.0')
        assert pv('0.9.9') < pv('1.0')
        assert pv('10') > pv('2')

        self.assertRaises(ValueError, spec.parse_version, '.')
        self.assertRaises(ValueError, spec.parse_version, 'hello')
        self.assertRaises(ValueError, spec.parse_version, '2./1')
        self.assertRaises(ValueError, spec.parse_version, '.1')
        self.assertRaises(ValueError, spec.parse_version, '')

        # Check parsing
        self.assertEqual([[1], 0], pv('1'))
        self.assertEqual([[1,0], 0], pv('1.0'))
        self.assertEqual([[1,0], -2, [5], 0], pv('1.0-pre5'))
        self.assertEqual([[1,0], -1, [5], 0], pv('1.0-rc5'))
        self.assertEqual([[1,0], 0, [5], 0], pv('1.0-5'))
        self.assertEqual([[1,0], 1, [5], 0], pv('1.0-r5'))
        self.assertEqual([[1,0], 2, [5], 0], pv('1.0-post5'))
        self.assertEqual([[1,0], 1], pv('1.0-r'))
        self.assertEqual([[1,0], 2], pv('1.0-post'))
        self.assertEqual([[1], -1, [2,0], -2, [2], 1], pv('1-rc2.0-pre2-r'))
        self.assertEqual([[1], -1, [2,0], -2, [2], 2], pv('1-rc2.0-pre2-post'))
        self.assertEqual([[1], -1, [2,0], -2, [], 1], pv('1-rc2.0-pre-r'))
        self.assertEqual([[1], -1, [2,0], -2, [], 2], pv('1-rc2.0-pre-post'))

        assert pv('1.0-0') > pv('1.0')
        assert pv('1.0-1') > pv('1.0-0')
        assert pv('1.0-0') < pv('1.0-1')

        assert pv('1.0-pre99') > pv('1.0-pre1')
        assert pv('1.0-pre99') < pv('1.0-rc1')
        assert pv('1.0-rc1') < pv('1.0')
        assert pv('1.0') < pv('1.0-0')
        assert pv('1.0-0') < pv('1.0-r')
        assert pv('1.0-r') < pv('1.0-post')
        assert pv('2.1.9-pre-1') > pv('2.1.9-pre')

        assert pv('2-r999') < pv('3-pre1')


if __name__ == '__main__':
    tests.main()
