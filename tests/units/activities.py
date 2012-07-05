#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
import hashlib
from os.path import abspath, lexists, exists

from __init__ import tests

import active_document as ad
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from active_toolkit import coroutine, util
from sugar_network.local.mounts import HomeMount
from sugar_network.local.mountset import Mountset
from sugar_network.local import activities
from sugar_network.toolkit import sugar
from sugar_network.resources.volume import Volume


class ActivitiesTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        volume = Volume('local', [User, Context])
        self.mounts = Mountset(volume)
        self.mounts['~'] = HomeMount(volume)
        self.mounts.open()
        self.job = None

    def tearDown(self):
        if self.job is not None:
            self.job.kill()
        self.mounts.close()
        tests.Test.tearDown(self)

    def test_Checkin_Create(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume, ['Activities'])
        coroutine.sleep()

        self.mounts.home_volume['context'].create(
                guid='org.sugarlabs.HelloWorld', type='activity',
                title={'en': 'title'}, summary={'en': 'summary'},
                description={'en': 'description'}, user=[sugar.uid()])

        os.makedirs('Activities/activity/activity')
        coroutine.sleep(1)
        spec = ['[Activity]',
                'name = HelloWorld',
                'activity_version = 1',
                'bundle_id = org.sugarlabs.HelloWorld',
                'exec = sugar-activity activity.HelloWorldActivity',
                'icon = activity-helloworld',
                'license = GPLv2+',
                ]
        with file('Activities/activity/activity/activity.info', 'w') as f:
            coroutine.sleep(1)
            f.write('\n'.join(spec))
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_Checkin_Copy(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume, ['Activities'])
        coroutine.sleep()

        self.mounts.home_volume['context'].create(
                guid='org.sugarlabs.HelloWorld', type='activity',
                title={'en': 'title'}, summary={'en': 'summary'},
                description={'en': 'description'}, user=[sugar.uid()])

        self.touch(('activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        shutil.copytree('activity', 'Activities/activity')
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_Checkin_Hardlink(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume, ['Activities'])
        coroutine.sleep()

        self.mounts.home_volume['context'].create(
                guid='org.sugarlabs.HelloWorld', type='activity',
                title={'en': 'title'}, summary={'en': 'summary'},
                description={'en': 'description'}, user=[sugar.uid()])

        self.touch(('activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        os.makedirs('Activities/activity/activity')
        coroutine.sleep(1)
        os.link('activity/activity/activity.info', 'Activities/activity/activity/activity.info')
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_OfflineCheckin(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume, ['Activities'])
        coroutine.sleep()

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('activities/context/org.sugarlabs.HelloWorld/' + hashed_path))

        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'HelloWorld'}, 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

    def test_Checkout(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume, ['Activities'])

        self.mounts.home_volume['context'].create(
                guid='org.sugarlabs.HelloWorld', type='activity',
                title={'en': 'title'}, summary={'en': 'summary'},
                description={'en': 'description'}, user=[sugar.uid()])

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = activity-helloworld',
            'license = GPLv2+',
            ]))
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('activities/checkins/' + hashed_path)
        assert exists('activities/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'keep': False, 'keep_impl': 2},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))

        shutil.rmtree('Activities/activity')
        coroutine.sleep(1)

        assert not exists('activities/checkins/' + hashed_path)
        assert not exists('activities/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'keep': False, 'keep_impl': 0},
                self.mounts.home_volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'keep', 'keep_impl']))


if __name__ == '__main__':
    tests.main()
