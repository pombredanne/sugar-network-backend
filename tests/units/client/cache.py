#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import shutil
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.client import cache_limit, cache_limit_percent, cache_lifetime, IPCConnection
from sugar_network.client.cache import Cache
from sugar_network.toolkit import http


class CacheTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class statvfs(object):
            f_blocks = 100
            f_bfree = 100
            f_frsize = 1

        self.statvfs = statvfs
        self.override(os, 'statvfs', lambda *args: statvfs())

    def test_open(self):
        volume = db.Volume('db', [Context, Implementation])

        volume['implementation'].create({
            'guid': '1',
            'context': 'context',
            'license': ['GPL'],
            'version': '1',
            'stability': 'stable',
            'data': {'blob_size': 1},
            })
        os.utime('db/implementation/1/1', (1, 1))
        volume['implementation'].create({
            'guid': '5',
            'context': 'context',
            'license': ['GPL'],
            'version': '5',
            'stability': 'stable',
            'data': {'blob_size': 5},
            })
        os.utime('db/implementation/5/5', (5, 5))
        volume['implementation'].create({
            'guid': '2',
            'context': 'context',
            'license': ['GPL'],
            'version': '2',
            'stability': 'stable',
            'data': {},
            })
        os.utime('db/implementation/2/2', (2, 2))
        volume['implementation'].create({
            'guid': '3',
            'context': 'context',
            'license': ['GPL'],
            'version': '3',
            'stability': 'stable',
            })
        os.utime('db/implementation/3/3', (3, 3))
        volume['implementation'].create({
            'guid': '4',
            'context': 'context',
            'license': ['GPL'],
            'version': '4',
            'stability': 'stable',
            'data': {'blob_size': 4, 'unpack_size': 44},
            })
        os.utime('db/implementation/4/4', (4, 4))

        cache = Cache(volume)
        self.assertEqual(['5', '4', '1'], [i for i in cache])

    def test_open_IgnoreClones(self):
        volume = db.Volume('db', [Context, Implementation])

        volume['context'].create({
            'guid': 'context',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        volume['implementation'].create({
            'guid': 'impl',
            'context': 'context',
            'license': ['GPL'],
            'version': '1',
            'stability': 'stable',
            'data': {'blob_size': 1},
            })

        cache = Cache(volume)
        self.assertEqual(['impl'], [i for i in cache])

        os.symlink('../../../implementation/im/impl', 'db/context/co/context/.clone')
        cache = Cache(volume)
        self.assertEqual([], [i for i in cache])

    def test_ensure(self):
        volume = db.Volume('db', [Context, Implementation])

        volume['implementation'].create({'data': {'blob_size': 1}, 'guid': '1', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/1/1', (1, 1))
        volume['implementation'].create({'data': {'blob_size': 2}, 'guid': '2', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/2/2', (2, 2))
        volume['implementation'].create({'data': {'blob_size': 3}, 'guid': '3', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/3/3', (3, 3))
        cache = Cache(volume)
        cache_limit.value = 10
        self.statvfs.f_bfree = 11

        self.assertRaises(RuntimeError, cache.ensure, 100, 0)
        assert volume['implementation'].exists('1')
        assert volume['implementation'].exists('2')
        assert volume['implementation'].exists('3')

        cache.ensure(1, 0)
        assert volume['implementation'].exists('1')
        assert volume['implementation'].exists('2')
        assert volume['implementation'].exists('3')

        cache.ensure(2, 0)
        assert not volume['implementation'].exists('1')
        assert volume['implementation'].exists('2')
        assert volume['implementation'].exists('3')

        cache.ensure(4, 0)
        assert not volume['implementation'].exists('2')
        assert not volume['implementation'].exists('3')

        self.assertRaises(RuntimeError, cache.ensure, 2, 0)

    def test_ensure_ConsiderTmpSize(self):
        volume = db.Volume('db', [Context, Implementation])
        volume['implementation'].create({'data': {'blob_size': 1}, 'guid': '1', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})

        cache = Cache(volume)
        cache_limit.value = 10
        self.statvfs.f_bfree = 10

        self.assertRaises(RuntimeError, cache.ensure, 1, 11)
        assert volume['implementation'].exists('1')

        cache.ensure(1, 10)
        assert not volume['implementation'].exists('1')

    def test_recycle(self):
        ts = time.time()

        volume = db.Volume('db', [Context, Implementation])
        volume['implementation'].create({'data': {'blob_size': 1}, 'guid': '1', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/1/1', (ts - 1.5 * 86400, ts - 1.5 * 86400))
        volume['implementation'].create({'data': {'blob_size': 1}, 'guid': '2', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/2/2', (ts - 2.5 * 86400, ts - 2.5 * 86400))
        volume['implementation'].create({'data': {'blob_size': 1}, 'guid': '3', 'context': 'context', 'version': '1', 'license': ['GPL'], 'stability': 'stable'})
        os.utime('db/implementation/3/3', (ts - 3.5 * 86400, ts - 3.5 * 86400))
        cache = Cache(volume)

        cache_lifetime.value = 4
        cache.recycle()
        assert volume['implementation'].exists('1')
        assert volume['implementation'].exists('2')
        assert volume['implementation'].exists('3')

        cache_lifetime.value = 3
        cache.recycle()
        assert volume['implementation'].exists('1')
        assert volume['implementation'].exists('2')
        assert not volume['implementation'].exists('3')

        cache_lifetime.value = 1
        cache.recycle()
        assert not volume['implementation'].exists('1')
        assert not volume['implementation'].exists('2')
        assert not volume['implementation'].exists('3')

        cache.recycle()

    def test_checkin(self):
        local_volume = self.start_online_client()
        conn = IPCConnection()
        self.statvfs.f_blocks = 0

        impl1 = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = context1',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)
        impl2 = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = context2',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)
        impl3 = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = context3',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)

        self.assertEqual('exit', [i for i in conn.get(['context', 'context1'], cmd='launch')][-1]['event'])
        self.assertEqual([impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)

        self.assertEqual('exit', [i for i in conn.get(['context', 'context2'], cmd='launch')][-1]['event'])
        self.assertEqual([impl2, impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)
        assert local_volume['implementation'].exists(impl2)

        self.assertEqual('exit', [i for i in conn.get(['context', 'context3'], cmd='launch')][-1]['event'])
        self.assertEqual([impl3, impl2, impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)
        assert local_volume['implementation'].exists(impl2)
        assert local_volume['implementation'].exists(impl3)

    def test_checkout(self):
        local_volume = self.start_online_client()
        conn = IPCConnection()
        self.statvfs.f_blocks = 0

        impl1 = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = context',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)

        conn.put(['context', 'context'], True, cmd='clone')
        self.assertEqual([], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)

        conn.put(['context', 'context'], False, cmd='clone')
        self.assertEqual([impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)

        impl2 = conn.upload(['implementation'], StringIO(self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = context',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = stable',
            ]])), cmd='submit', initial=True)

        shutil.rmtree('solutions')
        conn.put(['context', 'context'], True, cmd='clone')
        self.assertEqual([impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)
        assert local_volume['implementation'].exists(impl2)

        conn.put(['context', 'context'], False, cmd='clone')
        self.assertEqual([impl2, impl1], [i for i in self.client_routes._cache])
        assert local_volume['implementation'].exists(impl1)
        assert local_volume['implementation'].exists(impl2)


if __name__ == '__main__':
    tests.main()
