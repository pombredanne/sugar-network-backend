#!/usr/bin/env python
# sugar-lint: disable

import json

from __init__ import tests

from active_document import metadata


class MetadataTest(tests.Test):

    def test_Property_encode(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(1, prop.encode(1))
        self.assertEqual(1, prop.encode(1.1))
        self.assertEqual(1, prop.encode('1'))
        self.assertRaises(ValueError, prop.encode, '1.0')
        self.assertRaises(ValueError, prop.encode, '')
        self.assertRaises(ValueError, prop.encode, None)

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(1.0, prop.encode(1))
        self.assertEqual(1.1, prop.encode(1.1))
        self.assertEqual(1.0, prop.encode('1'))
        self.assertEqual(1.1, prop.encode('1.1'))
        self.assertRaises(ValueError, prop.encode, '')
        self.assertRaises(ValueError, prop.encode, None)

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(False, prop.encode(0))
        self.assertEqual(True, prop.encode(1))
        self.assertEqual(True, prop.encode(1.1))
        self.assertEqual(True, prop.encode('1'))
        self.assertEqual(True, prop.encode('A'))
        self.assertEqual(False, prop.encode(''))
        self.assertRaises(ValueError, prop.encode, None)

        prop = metadata.Property('prop', typecast=[int])
        self.assertEqual((1,), prop.encode(1))
        self.assertRaises(ValueError, prop.encode, None)
        self.assertRaises(ValueError, prop.encode, '')
        self.assertEqual((), prop.encode([]))
        self.assertEqual((123,), prop.encode('123'))
        self.assertRaises(ValueError, prop.encode, 'a')
        self.assertEqual((123, 4, 5), prop.encode(['123', 4, 5.6]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertRaises(ValueError, prop.encode, 0)
        self.assertRaises(ValueError, prop.encode, None)
        self.assertRaises(ValueError, prop.encode, '')
        self.assertRaises(ValueError, prop.encode, 'A')
        self.assertEqual(1, prop.encode(1))
        self.assertEqual(2, prop.encode(2))

        prop = metadata.Property('prop', typecast=[[True, False, 'probe']])
        self.assertRaises(ValueError, prop.encode, None)
        self.assertEqual((0, ), prop.encode(0))
        self.assertRaises(ValueError, prop.encode, 'A')
        self.assertEqual((True, ), prop.encode(True))
        self.assertEqual((False, ), prop.encode(False))
        self.assertRaises(ValueError, prop.encode, [3])
        self.assertRaises(ValueError, prop.encode, ['A'])
        self.assertRaises(ValueError, prop.encode, '')
        self.assertEqual((), prop.encode([]))
        self.assertEqual((True,), prop.encode([True]))
        self.assertEqual((False,), prop.encode([False]))
        self.assertEqual((True, False, True), prop.encode([True, False, True]))
        self.assertEqual((True, False, 'probe'), prop.encode([True, False, 'probe']))
        self.assertRaises(ValueError, prop.encode, [True, None])

        prop = metadata.Property('prop', typecast=[str])
        self.assertEqual(('',), prop.encode(''))
        self.assertEqual(('',), prop.encode(['']))
        self.assertEqual((), prop.encode([]))

        prop = metadata.Property('prop', typecast=[])
        self.assertRaises(ValueError, prop.encode, None)
        self.assertEqual(('',), prop.encode(''))
        self.assertEqual(('',), prop.encode(['']))
        self.assertEqual((), prop.encode([]))
        self.assertEqual(('0',), prop.encode(0))
        self.assertEqual(('',), prop.encode(''))
        self.assertEqual(('foo',), prop.encode('foo'))

        prop = metadata.Property('prop', typecast=[['A', 'B', 'C']])
        self.assertRaises(ValueError, prop.encode, '')
        self.assertRaises(ValueError, prop.encode, [''])
        self.assertEqual((), prop.encode([]))
        self.assertEqual(('A', 'B', 'C'), prop.encode(['A', 'B', 'C']))
        self.assertRaises(ValueError, prop.encode, ['a'])
        self.assertRaises(ValueError, prop.encode, ['A', 'x'])

        prop = metadata.Property('prop', typecast=[frozenset(['A', 'B', 'C'])])
        self.assertEqual(('A', 'B', 'C'), prop.encode(['A', 'B', 'C']))

        prop = metadata.Property('prop', typecast=lambda x: x + 1)
        self.assertEqual(1, prop.encode(0))

    def test_Property_reprcast(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(['0'], prop.reprcast(0))
        self.assertEqual(['1'], prop.reprcast(1))

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(['0'], prop.reprcast(0))
        self.assertEqual(['1.1'], prop.reprcast(1.1))

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(['1'], prop.reprcast(True))
        self.assertEqual(['0'], prop.reprcast(False))

        prop = metadata.Property('prop', typecast=[int])
        self.assertEqual(['0', '1'], prop.reprcast([0, 1]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertEqual(['2', '1'], prop.reprcast([2, 1]))

        prop = metadata.Property('prop', typecast=[[True, 0, 'probe']])
        self.assertEqual(['probe', '1', '0'], prop.reprcast(['probe', True, 0]))

        prop = metadata.Property('prop', reprcast=lambda x: x.keys())
        self.assertEqual(['a', '2'], prop.reprcast({'a': 1, 2: 'b'}))

    def test_AggregatedValue_cmp(self):
        self.assertEqual(metadata.AggregatedValue('value', True), metadata.AggregatedValue('value', True))
        self.assertNotEqual(metadata.AggregatedValue('value', True), metadata.AggregatedValue('value2', True))
        self.assertNotEqual(metadata.AggregatedValue('value', True), metadata.AggregatedValue('value', False))

        self.assertEqual(1, metadata.AggregatedValue('value', True))
        self.assertEqual(0, metadata.AggregatedValue('value', False))

        self.assertEqual('1', json.dumps(metadata.AggregatedValue('value', True)))
        self.assertEqual('1', json.dumps(metadata.AggregatedValue('foo', True)))
        self.assertEqual('0', json.dumps(metadata.AggregatedValue('value', False)))
        self.assertEqual('0', json.dumps(metadata.AggregatedValue('bar', False)))


if __name__ == '__main__':
    tests.main()
