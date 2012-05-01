#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil

import gevent

from __init__ import tests

from local_document import crawler, env


class CrawlerTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.job = None
        self.found = []
        self.lost = []

        crawler.found.connect(self.found_cb)
        crawler.lost.connect(self.lost_cb)

    def tearDown(self):
        if self.job is not None:
            self.job.kill()
        tests.Test.tearDown(self)

    def found_cb(self, path):
        self.found.append(path)

    def lost_cb(self, path):
        self.lost.append(path)

    def test_Walkthrough(self):
        self.touch('file')
        os.makedirs('activity-1')
        os.makedirs('activity-2/activity')
        self.touch('activity-3/activity/activity.info')
        self.touch('activity-4/activity/activity.info')
        self.touch('activity-5/activity/activity.info')

        self.job = gevent.spawn(crawler.dispatch, ['.'])
        gevent.sleep(1)

        self.assertEqual(
                sorted([
                    tests.tmpdir + '/activity-3',
                    tests.tmpdir + '/activity-4',
                    tests.tmpdir + '/activity-5',
                    ]),
                sorted(self.found))
        self.assertEqual([], self.lost)
        del self.found[:]

        with file('activity-4/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        with file('activity-2/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-2'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]

        os.makedirs('activity-6/activity')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        with file('activity-6/activity/activity.info', 'w') as f:
            f.close()
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-6'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]

        os.unlink('activity-5/activity/activity.info')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/activity-5'], self.lost)
        del self.lost[:]

        shutil.rmtree('activity-5')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        shutil.rmtree('activity-4')
        gevent.sleep(.1)
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/activity-4'], self.lost)
        del self.lost[:]

    def test_Moves(self):
        self.touch('Activities/activity/activity/activity.info')

        self.job = gevent.spawn(crawler.dispatch, ['Activities'])
        gevent.sleep()
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity', '.')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('activity', 'Activities/')
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity/activity', 'Activities/activity/activity2')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('Activities/activity/activity2', 'Activities/activity/activity')
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity/activity/activity.info', 'Activities/activity/activity/activity.info2')
        gevent.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('Activities/activity/activity/activity.info2', 'Activities/activity/activity/activity.info')
        gevent.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]


if __name__ == '__main__':
    tests.main()
