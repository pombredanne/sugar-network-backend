#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
import hashlib
from os.path import abspath, lexists, exists

from __init__ import tests

from sugar_network import db, model
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.client import clones
from sugar_network.toolkit import coroutine


class CloneTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.volume = db.Volume('local', [User, Context])
        self.job = None

    def tearDown(self):
        if self.job is not None:
            self.job.kill()
        self.volume.close()
        tests.Test.tearDown(self)

    def test_Inotify_NoPermissions(self):
        assert not exists('/foo/bar')
        inotify = clones._Inotify(self.volume['context'])
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

        inotify = clones._Inotify(self.volume['context'])
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

        inotify = clones._Inotify(self.volume['context'])
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
        self.job = coroutine.spawn(clones.monitor,
                self.volume['context'], ['Activities'])
        coroutine.sleep()

        self.volume['context'].create({
                'guid': 'org.sugarlabs.HelloWorld',
                'type': 'activity',
                'title': {'en': 'title'},
                'summary': {'en': 'summary'},
                'description': {'en': 'description'},
                'user': [tests.UID],
                })

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
        assert exists('clones/checkin/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('clones/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'favorite': False, 'clone': 2},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))
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
        self.job = coroutine.spawn(clones.monitor,
                self.volume['context'], ['Activities'])
        coroutine.sleep()

        self.volume['context'].create({
                'guid': 'org.sugarlabs.HelloWorld',
                'type': 'activity',
                'title': {'en': 'title'},
                'summary': {'en': 'summary'},
                'description': {'en': 'description'},
                'user': [tests.UID],
                })

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
        assert exists('clones/checkin/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('clones/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'favorite': False, 'clone': 2},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))

    def test_Checkin_Hardlink(self):
        self.job = coroutine.spawn(clones.monitor,
                self.volume['context'], ['Activities'])
        coroutine.sleep()

        self.volume['context'].create({
                'guid': 'org.sugarlabs.HelloWorld',
                'type': 'activity',
                'title': {'en': 'title'},
                'summary': {'en': 'summary'},
                'description': {'en': 'description'},
                'user': [tests.UID],
                })

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
        assert exists('clones/checkin/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('clones/context/org.sugarlabs.HelloWorld/' + hashed_path))
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'favorite': False, 'clone': 2},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))

    def test_OfflineCheckin(self):
        self.job = coroutine.spawn(clones.monitor,
                self.volume['context'], ['Activities'])
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
        assert exists('clones/checkin/' + hashed_path)
        self.assertEqual(
                abspath('Activities/activity'),
                os.readlink('clones/context/org.sugarlabs.HelloWorld/' + hashed_path))

        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en-us': 'HelloWorld'}, 'favorite': False, 'clone': 2},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))

    def test_Checkout(self):
        self.job = coroutine.spawn(clones.monitor,
                self.volume['context'], ['Activities'])

        self.volume['context'].create({
                'guid': 'org.sugarlabs.HelloWorld',
                'type': 'activity',
                'title': {'en': 'title'},
                'summary': {'en': 'summary'},
                'description': {'en': 'description'},
                'user': [tests.UID],
                })

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
        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = org.sugarlabs.HelloWorld',
            'exec = sugar-activity activity.HelloWorldActivity',
            'icon = icon',
            'license = GPLv2+',
            'mime_types = foo/bar',
            ]))
        coroutine.sleep(1)

        hashed_path = hashlib.sha1(tests.tmpdir + '/Activities/activity').hexdigest()
        assert exists('clones/checkin/' + hashed_path)
        assert exists('clones/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'favorite': False, 'clone': 2},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))
        assert exists('share/icons/sugar/scalable/mimetypes/foo-bar.svg')
        assert exists('share/mime/packages/%s.xml' % hashed_path)
        assert exists('share/mime/application/x-foo-bar.xml')

        shutil.rmtree('Activities/activity')
        coroutine.sleep(1)

        assert not exists('clones/checkin/' + hashed_path)
        assert not exists('clones/context/org.sugarlabs.HelloWorld/' + hashed_path)
        self.assertEqual(
                {'guid': 'org.sugarlabs.HelloWorld', 'title': {'en': 'title'}, 'favorite': False, 'clone': 0},
                self.volume['context'].get('org.sugarlabs.HelloWorld').properties(['guid', 'title', 'favorite', 'clone']))
        assert not lexists('share/icons/sugar/scalable/mimetypes/foo-bar.svg')
        assert not lexists('share/mime/packages/%s.xml' % hashed_path)
        assert not lexists('share/mime/application/x-foo-bar.xml')

    def test_Sync(self):
        volume = db.Volume('client', model.RESOURCES)
        volume['context'].create({
            'guid': 'context1',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'clone': 0,
            })
        volume['context'].create({
            'guid': 'context2',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'clone': 1,
            })
        volume['context'].create({
            'guid': 'context3',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'clone': 2,
            })

        os.makedirs('Activities')
        os.utime('Activities', (volume['context'].mtime + 1, volume['context'].mtime + 1))

        self.touch(clones._context_path('context1', 'clone'))
        self.touch(clones._context_path('context2', 'clone'))
        clones.populate(volume['context'], ['Activities'])

        self.assertEqual(0, volume['context'].get('context1')['clone'])
        self.assertEqual(2, volume['context'].get('context2')['clone'])
        self.assertEqual(0, volume['context'].get('context3')['clone'])

    def test_SyncByMtime(self):
        volume = db.Volume('client', model.RESOURCES)
        volume['context'].create({
            'guid': 'context',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'clone': 2,
            })

        os.makedirs('Activities')
        os.utime('Activities', (2, 2))

        volume['context'].mtime = 3
        clones.populate(volume['context'], ['Activities'])
        self.assertEqual(2, volume['context'].get('context')['clone'])

        volume['context'].mtime = 2
        clones.populate(volume['context'], ['Activities'])
        self.assertEqual(2, volume['context'].get('context')['clone'])

        volume['context'].mtime = 1
        clones.populate(volume['context'], ['Activities'])
        self.assertEqual(0, volume['context'].get('context')['clone'])


if __name__ == '__main__':
    tests.main()
