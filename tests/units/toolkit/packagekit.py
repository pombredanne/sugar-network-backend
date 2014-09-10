#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit.packagekit import parse_version


class Packagekit(tests.Test):

    def test_parse_version(self):
		self.assertEqual('0.3.1-1', parse_version('1:0.3.1-1'))
		self.assertEqual('0.3.1-1', parse_version('0.3.1-1ubuntu0'))
		self.assertEqual('0.3-post1-rc2', parse_version('0.3-post1-rc2'))
		self.assertEqual('0.3.1-2', parse_version('0.3.1-r2-r3'))
		self.assertEqual('6.17', parse_version('6b17'))
		self.assertEqual('20-1', parse_version('b20_1'))
		self.assertEqual('17', parse_version('p17'))
		self.assertEqual('7-pre3-2.1.1-3', parse_version('7~u3-2.1.1-3'))	# Debian snapshot
		self.assertEqual('7-pre3-2.1.1-pre1-1', parse_version('7~u3-2.1.1~pre1-1ubuntu2'))
		self.assertEqual(None, parse_version('cvs'))


if __name__ == '__main__':
    tests.main()
