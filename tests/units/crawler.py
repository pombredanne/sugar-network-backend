#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil

import gevent

from __init__ import tests

from local_document import crawler


class CrawlerTest(tests.Test):

    def test_Walkthrough(self):

        def found_cb(path):
            found.append(path)

        def lost_cb(path):
            lost.append(path)

        found = []
        lost = []

        crawler.found.connect(found_cb)
        crawler.lost.connect(lost_cb)

        self.touch('file')
        os.makedirs('activity-1')
        os.makedirs('activity-2/activity')
        self.touch('activity-3/activity/activity.info')
        self.touch('activity-4/activity/activity.info')
        self.touch('activity-5/activity/activity.info')

        crawler_ = crawler.Crawler('.')
        gevent.sleep()

        self.assertEqual(
                sorted([
                    tests.tmpdir + '/activity-3',
                    tests.tmpdir + '/activity-4',
                    tests.tmpdir + '/activity-5',
                    ]),
                sorted(found))
        self.assertEqual([], lost)
        del found[:]

        with file('activity-4/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        with file('activity-2/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-2'], found)
        self.assertEqual([], lost)
        del found[:]

        os.makedirs('activity-6/activity')
        gevent.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        with file('activity-6/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-6'], found)
        self.assertEqual([], lost)
        del found[:]

        os.unlink('activity-5/activity/activity.info')
        gevent.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/activity-5'], lost)
        del lost[:]

        shutil.rmtree('activity-5')
        gevent.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        shutil.rmtree('activity-4')
        gevent.sleep(.1)
        gevent.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/activity-4'], lost)
        del lost[:]

        crawler_.close()


if __name__ == '__main__':
    tests.main()
