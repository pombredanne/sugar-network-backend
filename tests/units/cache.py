#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import cache


class CacheTest(tests.Test):

    def test_properties_absent(self):
        self.assertEqual(None, cache.get_properties('resource', 'guid'))

    def test_properties_Set(self):
        cache.set_properties({'prop': 'value'}, 'resource', 'guid')
        self.assertEqual(
                {'prop': 'value'},
                cache.get_properties('resource', 'guid'))


if __name__ == '__main__':
    tests.main()
