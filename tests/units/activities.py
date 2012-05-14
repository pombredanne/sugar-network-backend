#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
import hashlib
from os.path import abspath, lexists, exists

import gevent

from __init__ import tests

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from local_document.mounts import Mounts
from local_document import activities, sugar


class ActivitiesTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.mounts = Mounts('local', [User, Context])
        self.job = None

    def tearDown(self):
        if self.job is not None:
            self.job.kill()
        self.mounts.close()
        tests.Test.tearDown(self)

    def test_Checkin(self):
        self.job = gevent.spawn(activities.monitor, self.mounts)
        gevent.sleep()

        self.mounts.home_volume['context'].create_with_guid(
                'org.sugarlabs.HelloWorld', {
                    'type': 'activity',
                    'title': 'title',
                    'summary': 'summary',
                    'description': 'description',
                    'author': [sugar.uid()],
                    })

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        gevent.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))

        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': 'title', 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_OfflineCheckin(self):
        self.job = gevent.spawn(activities.monitor, self.mounts)
        gevent.sleep()

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        gevent.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))

        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': 'HelloWorld', 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_Checkout(self):
        self.job = gevent.spawn(activities.monitor, self.mounts)

        self.mounts.home_volume['context'].create_with_guid(
                'org.sugarlabs.HelloWorld', {
                    'type': 'activity',
                    'title': 'title',
                    'summary': 'summary',
                    'description': 'description',
                    'author': [sugar.uid()],
                    })

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        gevent.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        assert exists('activities/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': 'title', 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

        shutil.rmtree('Activities/activity')
        gevent.sleep(1)

        assert not exists('activities/checkins/' + hashed_path)
        assert not exists('activities/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': 'title', 'keep': False, 'keep_impl': 0},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))


if __name__ == '__main__':
    tests.main()
