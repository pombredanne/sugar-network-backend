#!/usr/bin/env python
# sugar-lint: disable

import time

from __init__ import tests

import sugar_network as sn


class ResourcesTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

    def test_Walkthrough(self):
        self.httpd(8888)
        sn.api_url.value = 'http://localhost:8888'

        time.sleep(3)

        # Register user
        sn.User(sn.guid()).call('stats-info')

        query = sn.User.find()
        self.assertEqual(1, query.total)
        self.assertEqual(sn.guid(), query[0]['guid'])
        self.assertEqual(sn.nickname(), query[0]['nickname'])


if __name__ == '__main__':
    tests.main()
