#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db


class MetadataTest(tests.Test):

    def test_Typecast(self):
        prop = db.Numeric()
        self.assertEqual(1, prop.typecast(1))
        self.assertEqual(1, prop.typecast(1.1))
        self.assertEqual(1, prop.typecast('1'))
        self.assertRaises(ValueError, prop.typecast, '1.0')
        self.assertRaises(ValueError, prop.typecast, '')
        self.assertRaises(TypeError, prop.typecast, None)

        prop = db.Boolean()
        self.assertEqual(False, prop.typecast(0))
        self.assertEqual(True, prop.typecast(1))
        self.assertEqual(True, prop.typecast(1.1))
        self.assertEqual(True, prop.typecast('1'))
        self.assertEqual(False, prop.typecast('false'))
        self.assertEqual(True, prop.typecast(True))
        self.assertEqual(False, prop.typecast(False))
        self.assertEqual(False, prop.typecast('False'))
        self.assertEqual(False, prop.typecast('0'))
        self.assertEqual(False, prop.typecast(''))
        self.assertEqual(False, prop.typecast(None))

        prop = db.List(subtype=db.Numeric())
        self.assertEqual([1], prop.typecast(1))
        self.assertEqual([], prop.typecast(None))
        self.assertRaises(ValueError, prop.typecast, '')
        self.assertEqual([], prop.typecast([]))
        self.assertEqual([123], prop.typecast('123'))
        self.assertRaises(ValueError, prop.typecast, 'a')
        self.assertEqual([123, 4, 5], prop.typecast(['123', 4, 5.6]))

        prop = db.Enum(items=[1, 2])
        self.assertRaises(ValueError, prop.typecast, 0)
        self.assertRaises(TypeError, prop.typecast, None)
        self.assertRaises(ValueError, prop.typecast, '')
        self.assertRaises(ValueError, prop.typecast, 'A')
        self.assertRaises(ValueError, prop.typecast, '3')
        self.assertEqual(1, prop.typecast(1))
        self.assertEqual(2, prop.typecast(2))
        self.assertEqual(1, prop.typecast('1'))

        prop = db.List()
        self.assertEqual([], prop.typecast(None))
        self.assertEqual([''], prop.typecast(''))
        self.assertEqual([''], prop.typecast(['']))
        self.assertEqual([], prop.typecast([]))
        self.assertEqual([0], prop.typecast(0))
        self.assertEqual([''], prop.typecast(''))
        self.assertEqual(['foo'], prop.typecast('foo'))

        prop = db.List(subtype=db.Enum(['A', 'B', 'C']))
        self.assertRaises(ValueError, prop.typecast, '')
        self.assertRaises(ValueError, prop.typecast, [''])
        self.assertEqual([], prop.typecast([]))
        self.assertEqual(['A', 'B', 'C'], prop.typecast(['A', 'B', 'C']))
        self.assertRaises(ValueError, prop.typecast, ['a'])
        self.assertRaises(ValueError, prop.typecast, ['A', 'x'])


if __name__ == '__main__':
    tests.main()
