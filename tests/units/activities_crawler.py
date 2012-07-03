#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
from os.path import exists

from __init__ import tests

from active_toolkit import coroutine
from sugar_network.local import activities_crawler


class ActivitiesCrawlerTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        self.job = None
        self.found = []
        self.lost = []

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

        self.job = coroutine.spawn(activities_crawler.dispatch, ['.'],
                self.found_cb, self.lost_cb)
        coroutine.sleep(1)

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
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        with file('activity-2/activity/activity.info', 'w') as f:
            f.close()
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-2'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]

        os.makedirs('activity-6/activity')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        with file('activity-6/activity/activity.info', 'w') as f:
            f.close()
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-6'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]

        os.unlink('activity-5/activity/activity.info')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/activity-5'], self.lost)
        del self.lost[:]

        shutil.rmtree('activity-5')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([], self.lost)

        shutil.rmtree('activity-4')
        coroutine.sleep(.1)
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/activity-4'], self.lost)
        del self.lost[:]

    def test_Moves(self):
        self.touch('Activities/activity/activity/activity.info')

        self.job = coroutine.spawn(activities_crawler.dispatch, ['Activities'],
                self.found_cb, self.lost_cb)
        coroutine.sleep()
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity', '.')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('activity', 'Activities/')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity/activity', 'Activities/activity/activity2')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('Activities/activity/activity2', 'Activities/activity/activity')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]

        shutil.move('Activities/activity/activity/activity.info', 'Activities/activity/activity/activity.info2')
        coroutine.sleep(.1)
        self.assertEqual([], self.found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.lost)
        del self.found[:]
        del self.lost[:]
        shutil.move('Activities/activity/activity/activity.info2', 'Activities/activity/activity/activity.info')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], self.found)
        self.assertEqual([], self.lost)
        del self.found[:]
        del self.lost[:]

    def test_NoPermissions(self):
        assert not exists('/foo/bar')
        activities_crawler.populate(['/foo/bar'], None, None)
        assert not exists('/foo/bar')


if __name__ == '__main__':
    tests.main()