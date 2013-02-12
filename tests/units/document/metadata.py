#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from active_document import metadata


class MetadataTest(tests.Test):

    def test_Property_decode(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(1, prop.decode(1))
        self.assertEqual(1, prop.decode(1.1))
        self.assertEqual(1, prop.decode('1'))
        self.assertRaises(ValueError, prop.decode, '1.0')
        self.assertRaises(ValueError, prop.decode, '')
        self.assertRaises(ValueError, prop.decode, None)

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(1.0, prop.decode(1))
        self.assertEqual(1.1, prop.decode(1.1))
        self.assertEqual(1.0, prop.decode('1'))
        self.assertEqual(1.1, prop.decode('1.1'))
        self.assertRaises(ValueError, prop.decode, '')
        self.assertRaises(ValueError, prop.decode, None)

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(False, prop.decode(0))
        self.assertEqual(True, prop.decode(1))
        self.assertEqual(True, prop.decode(1.1))
        self.assertEqual(True, prop.decode('1'))
        self.assertEqual(True, prop.decode('A'))
        self.assertEqual(False, prop.decode(''))
        self.assertRaises(ValueError, prop.decode, None)

        prop = metadata.Property('prop', typecast=[int])
        self.assertEqual((1,), prop.decode(1))
        self.assertRaises(ValueError, prop.decode, None)
        self.assertRaises(ValueError, prop.decode, '')
        self.assertEqual((), prop.decode([]))
        self.assertEqual((123,), prop.decode('123'))
        self.assertRaises(ValueError, prop.decode, 'a')
        self.assertEqual((123, 4, 5), prop.decode(['123', 4, 5.6]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertRaises(ValueError, prop.decode, 0)
        self.assertRaises(ValueError, prop.decode, None)
        self.assertRaises(ValueError, prop.decode, '')
        self.assertRaises(ValueError, prop.decode, 'A')
        self.assertEqual(1, prop.decode(1))
        self.assertEqual(2, prop.decode(2))

        prop = metadata.Property('prop', typecast=[[True, False, 'probe']])
        self.assertRaises(ValueError, prop.decode, None)
        self.assertEqual((0, ), prop.decode(0))
        self.assertRaises(ValueError, prop.decode, 'A')
        self.assertEqual((True, ), prop.decode(True))
        self.assertEqual((False, ), prop.decode(False))
        self.assertRaises(ValueError, prop.decode, [3])
        self.assertRaises(ValueError, prop.decode, ['A'])
        self.assertRaises(ValueError, prop.decode, '')
        self.assertEqual((), prop.decode([]))
        self.assertEqual((True,), prop.decode([True]))
        self.assertEqual((False,), prop.decode([False]))
        self.assertEqual((True, False, True), prop.decode([True, False, True]))
        self.assertEqual((True, False, 'probe'), prop.decode([True, False, 'probe']))
        self.assertRaises(ValueError, prop.decode, [True, None])

        prop = metadata.Property('prop', typecast=[str])
        self.assertEqual(('',), prop.decode(''))
        self.assertEqual(('',), prop.decode(['']))
        self.assertEqual((), prop.decode([]))

        prop = metadata.Property('prop', typecast=[])
        self.assertRaises(ValueError, prop.decode, None)
        self.assertEqual(('',), prop.decode(''))
        self.assertEqual(('',), prop.decode(['']))
        self.assertEqual((), prop.decode([]))
        self.assertEqual(('0',), prop.decode(0))
        self.assertEqual(('',), prop.decode(''))
        self.assertEqual(('foo',), prop.decode('foo'))

        prop = metadata.Property('prop', typecast=[['A', 'B', 'C']])
        self.assertRaises(ValueError, prop.decode, '')
        self.assertRaises(ValueError, prop.decode, [''])
        self.assertEqual((), prop.decode([]))
        self.assertEqual(('A', 'B', 'C'), prop.decode(['A', 'B', 'C']))
        self.assertRaises(ValueError, prop.decode, ['a'])
        self.assertRaises(ValueError, prop.decode, ['A', 'x'])

        prop = metadata.Property('prop', typecast=[frozenset(['A', 'B', 'C'])])
        self.assertEqual(('A', 'B', 'C'), prop.decode(['A', 'B', 'C']))

        prop = metadata.Property('prop', typecast=lambda x: x + 1)
        self.assertEqual(1, prop.decode(0))

    def test_Property_to_string(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(['0'], prop.to_string(0))
        self.assertEqual(['1'], prop.to_string(1))

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(['0'], prop.to_string(0))
        self.assertEqual(['1.1'], prop.to_string(1.1))

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(['1'], prop.to_string(True))
        self.assertEqual(['0'], prop.to_string(False))

        prop = metadata.Property('prop', typecast=[int])
        self.assertEqual(['0', '1'], prop.to_string([0, 1]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertEqual(['2', '1'], prop.to_string([2, 1]))

        prop = metadata.Property('prop', typecast=[[True, 0, 'probe']])
        self.assertEqual(['probe', '1', '0'], prop.to_string(['probe', True, 0]))

        prop = metadata.Property('prop', reprcast=lambda x: x.keys())
        self.assertEqual(['a', '2'], prop.to_string({'a': 1, 2: 'b'}))


if __name__ == '__main__':
    tests.main()
