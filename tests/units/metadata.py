#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from active_document import metadata


class MetadataTest(tests.Test):

    def test_Property_convert(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(1, prop.convert(1))
        self.assertEqual(1, prop.convert(1.1))
        self.assertEqual(1, prop.convert('1'))
        self.assertRaises(ValueError, prop.convert, '1.0')
        self.assertRaises(ValueError, prop.convert, '')
        self.assertRaises(TypeError, prop.convert, None)

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(1.0, prop.convert(1))
        self.assertEqual(1.1, prop.convert(1.1))
        self.assertEqual(1.0, prop.convert('1'))
        self.assertEqual(1.1, prop.convert('1.1'))
        self.assertRaises(ValueError, prop.convert, '')
        self.assertRaises(TypeError, prop.convert, None)

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(False, prop.convert(0))
        self.assertEqual(True, prop.convert(1))
        self.assertEqual(True, prop.convert(1.1))
        self.assertEqual(True, prop.convert('1'))
        self.assertEqual(True, prop.convert('A'))
        self.assertEqual(False, prop.convert(''))
        self.assertEqual(False, prop.convert(None))

        prop = metadata.Property('prop', typecast=[int])
        self.assertRaises(TypeError, prop.convert, 1)
        self.assertRaises(TypeError, prop.convert, None)
        self.assertEqual((), prop.convert(''))
        self.assertEqual((), prop.convert([]))
        self.assertEqual((1, 2, 3), prop.convert('123'))
        self.assertRaises(ValueError, prop.convert, 'a')
        self.assertEqual((123, 4, 5), prop.convert(['123', 4, 5.6]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertRaises(RuntimeError, prop.convert, 0)
        self.assertRaises(RuntimeError, prop.convert, None)
        self.assertRaises(RuntimeError, prop.convert, '')
        self.assertRaises(RuntimeError, prop.convert, 'A')
        self.assertEqual(1, prop.convert(1))
        self.assertEqual(2, prop.convert(2))

        prop = metadata.Property('prop', typecast=[[True, False, 'probe']])
        self.assertRaises(TypeError, prop.convert, 0)
        self.assertRaises(TypeError, prop.convert, None)
        self.assertRaises(RuntimeError, prop.convert, 'A')
        self.assertRaises(TypeError, prop.convert, True)
        self.assertRaises(TypeError, prop.convert, False)
        self.assertRaises(RuntimeError, prop.convert, [3])
        self.assertRaises(RuntimeError, prop.convert, ['A'])
        self.assertEqual((), prop.convert(''))
        self.assertEqual((), prop.convert([]))
        self.assertEqual((True,), prop.convert([True]))
        self.assertEqual((False,), prop.convert([False]))
        self.assertEqual((True, False, True), prop.convert([True, False, True]))
        self.assertEqual((True, False, 'probe'), prop.convert([True, False, 'probe']))
        self.assertRaises(RuntimeError, prop.convert, [True, None])

        prop = metadata.Property('prop', typecast=[str])
        self.assertEqual((), prop.convert(''))
        self.assertEqual(('',), prop.convert(['']))
        self.assertEqual((), prop.convert([]))

        prop = metadata.Property('prop', typecast=[])
        self.assertEqual((), prop.convert(''))
        self.assertEqual(('',), prop.convert(['']))
        self.assertEqual((), prop.convert([]))

    def test_Property_serialise(self):
        prop = metadata.Property('prop', typecast=int)
        self.assertEqual(['0'], prop.serialise(0))
        self.assertEqual(['1'], prop.serialise(1))

        prop = metadata.Property('prop', typecast=float)
        self.assertEqual(['0'], prop.serialise(0))
        self.assertEqual(['1.1'], prop.serialise(1.1))

        prop = metadata.Property('prop', typecast=bool)
        self.assertEqual(['1'], prop.serialise(True))
        self.assertEqual(['0'], prop.serialise(False))

        prop = metadata.Property('prop', typecast=[int])
        self.assertEqual(['0', '1'], prop.serialise([0, 1]))

        prop = metadata.Property('prop', typecast=[1, 2])
        self.assertEqual(['2', '1'], prop.serialise([2, 1]))

        prop = metadata.Property('prop', typecast=[[True, 0, 'probe']])
        self.assertEqual(['probe', '1', '0'], prop.serialise(['probe', True, 0]))


if __name__ == '__main__':
    tests.main()
