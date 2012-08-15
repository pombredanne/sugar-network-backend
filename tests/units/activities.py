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

    def test_Inotify_NoPermissions(self):
        assert not exists('/foo/bar')
        inotify = activities._Inotify(self.mounts.home_volume['context'])
        inotify.setup(['/foo/bar'])
        assert not exists('/foo/bar')

    def test_Inotify_Walkthrough(self):
        self.touch('file')
        os.makedirs('activity-1')
        os.makedirs('activity-2/activity')
        self.touch('activity-3/activity/activity.info')
        self.touch('activity-4/activity/activity.info')
        self.touch('activity-5/activity/activity.info')

        found = []
        lost = []

        inotify = activities._Inotify(self.mounts.home_volume['context'])
        inotify.found = found.append
        inotify.lost = lost.append
        inotify.setup(['.'])
        self.job = coroutine.spawn(inotify.serve_forever)
        coroutine.sleep(1)

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
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        with file('activity-2/activity/activity.info', 'w') as f:
            f.close()
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-2'], found)
        self.assertEqual([], lost)
        del found[:]

        os.makedirs('activity-6/activity')
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        with file('activity-6/activity/activity.info', 'w') as f:
            f.close()
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/activity-6'], found)
        self.assertEqual([], lost)
        del found[:]

        os.unlink('activity-5/activity/activity.info')
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/activity-5'], lost)
        del lost[:]

        shutil.rmtree('activity-5')
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([], lost)

        shutil.rmtree('activity-4')
        coroutine.sleep(.1)
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/activity-4'], lost)
        del lost[:]

    def test_Inotify_Moves(self):
        self.touch('Activities/activity/activity/activity.info')

        found = []
        lost = []

        inotify = activities._Inotify(self.mounts.home_volume['context'])
        inotify.found = found.append
        inotify.lost = lost.append
        inotify.setup(['Activities'])
        self.job = coroutine.spawn(inotify.serve_forever)
        coroutine.sleep(.1)

        shutil.move('Activities/activity', '.')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], lost)
        del found[:]
        del lost[:]
        shutil.move('activity', 'Activities/')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], found)
        self.assertEqual([], lost)
        del found[:]
        del lost[:]

        shutil.move('Activities/activity/activity', 'Activities/activity/activity2')
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], lost)
        del found[:]
        del lost[:]
        shutil.move('Activities/activity/activity2', 'Activities/activity/activity')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], found)
        self.assertEqual([], lost)
        del found[:]
        del lost[:]

        shutil.move('Activities/activity/activity/activity.info', 'Activities/activity/activity/activity.info2')
        coroutine.sleep(.1)
        self.assertEqual([], found)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], lost)
        del found[:]
        del lost[:]
        shutil.move('Activities/activity/activity/activity.info2', 'Activities/activity/activity/activity.info')
        coroutine.sleep(.1)
        self.assertEqual([tests.tmpdir + '/Activities/activity'], found)
        self.assertEqual([], lost)
        del found[:]
        del lost[:]

    def test_Checkin_Create(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume['context'], ['Activities'])
        coroutine.sleep()

        self.mounts.home_volume['context'].create(
                guid='org.sugarlabs.HelloWorld', type='activity',
                title={'en': 'title'}, summary={'en': 'summary'},
                description={'en': 'description'}, user=[sugar.uid()])

        os.makedirs('Activities/activity/activity')
        coroutine.sleep(1)
        self.touch('Activities/activity/activity/icon.svg')
        self.touch(('Activities/activity/activity/mimetypes.xml', [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<mime-info xmlns="http://www.freedesktop.org/standards/shared-mime-info">',
            '<mime-type type="application/x-foo-bar">',
            '<comment xml:lang="en">foo-bar</comment>',
            '<glob pattern="*.foo"/>',
            '</mime-type>',
            '</mime-info>',
            ]))
        spec = ['[Activity]',
                'name = HelloWorld',
                'activity_version = 1',
                'bundle_id = org.sugarlabs.HelloWorld',
                'exec = sugar-activity activity.HelloWorldActivity',
                'icon = icon',
                'license = GPLv2+',
                'mime_types = foo/bar',
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
        assert exists('share/icons/sugar/scalable/mimetypes/foo-bar.svg')
        self.assertEqual(
                tests.tmpdir + '/Activities/activity/activity/icon.svg',
                os.readlink('share/icons/sugar/scalable/mimetypes/foo-bar.svg'))
        assert exists('share/mime/packages/%s.xml' % hashed_path)
        self.assertEqual(
                tests.tmpdir + '/Activities/activity/activity/mimetypes.xml',
                os.readlink('share/mime/packages/%s.xml' % hashed_path))
        assert exists('share/mime/application/x-foo-bar.xml')

    def test_Checkin_Copy(self):
        self.job = coroutine.spawn(activities.monitor,
                self.mounts.home_volume['context'], ['Activities'])
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
                self.mounts.home_volume['context'], ['Activities'])
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
                self.mounts.home_volume['context'], ['Activities'])
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
                self.mounts.home_volume['context'], ['Activities'])

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
