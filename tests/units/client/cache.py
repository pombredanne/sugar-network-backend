#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network.client import cache, cache_limit, cache_lifetime
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
        cache_limit.value = 0

    def test_get(self):
        self.override(http.Connection, 'download', lambda self_, path: StringIO(self.zips(('topdir/probe', '/'.join(path)))))
        cache.get('impl', {'unpack_size': 100})
        self.assertEqual(100, json.load(file('cache/implementation/impl/.unpack_size')))
        self.assertEqual('implementation/impl/data', file('cache/implementation/impl/topdir/probe').read())

    def test_ensure(self):
        self.touch(('cache/implementation/1/.unpack_size', '1', 1))
        self.touch(('cache/implementation/2/.unpack_size', '1', 2))
        self.touch(('cache/implementation/3/.unpack_size', '1', 3))
        cache_limit.value = 10

        self.statvfs.f_bfree = 11
        cache.ensure(1, 0)
        assert exists('cache/implementation/1')
        assert exists('cache/implementation/2')
        assert exists('cache/implementation/3')

        self.statvfs.f_bfree = 10
        cache.ensure(1, 0)
        assert not exists('cache/implementation/1')
        assert exists('cache/implementation/2')
        assert exists('cache/implementation/3')

        self.statvfs.f_bfree = 11
        cache.ensure(3, 0)
        assert not exists('cache/implementation/1')
        assert not exists('cache/implementation/2')
        assert not exists('cache/implementation/3')

        self.statvfs.f_bfree = 10
        self.assertRaises(RuntimeError, cache.ensure, 1, 0)

    def test_ensure_FailRightAway(self):
        self.touch(('cache/implementation/1/.unpack_size', '1', 1))
        cache_limit.value = 10
        self.statvfs.f_bfree = 10

        self.assertRaises(RuntimeError, cache.ensure, 2, 0)
        assert exists('cache/implementation/1')

        cache.ensure(1, 0)
        assert not exists('cache/implementation/1')

    def test_ensure_ConsiderTmpSize(self):
        self.touch(('cache/implementation/1/.unpack_size', '1', 1))
        cache_limit.value = 10
        self.statvfs.f_bfree = 10

        self.assertRaises(RuntimeError, cache.ensure, 2, 0)
        assert exists('cache/implementation/1')

        cache.ensure(1, 0)
        assert not exists('cache/implementation/1')

    def test_recycle(self):
        ts = time.time()
        self.touch(('cache/implementation/1/.unpack_size', '1'))
        os.utime('cache/implementation/1', (ts - 1.5 * 86400, ts - 1.5 * 86400))
        self.touch(('cache/implementation/2/.unpack_size', '1'))
        os.utime('cache/implementation/2', (ts - 2.5 * 86400, ts - 2.5 * 86400))
        self.touch(('cache/implementation/3/.unpack_size', '1'))
        os.utime('cache/implementation/3', (ts - 3.5 * 86400, ts - 3.5 * 86400))

        cache_lifetime.value = 4
        cache.recycle()
        assert exists('cache/implementation/1')
        assert exists('cache/implementation/2')
        assert exists('cache/implementation/3')

        cache_lifetime.value = 3
        cache.recycle()
        assert exists('cache/implementation/1')
        assert exists('cache/implementation/2')
        assert not exists('cache/implementation/3')

        cache_lifetime.value = 1
        cache.recycle()
        assert not exists('cache/implementation/1')
        assert not exists('cache/implementation/2')
        assert not exists('cache/implementation/3')

    def test_recycle_CallEnsure(self):
        self.touch(('cache/implementation/1/.unpack_size', '1', 100))
        cache_limit.value = 10
        cache_lifetime.value = 0

        self.statvfs.f_bfree = 100
        cache.recycle()
        assert exists('cache/implementation/1')

        self.statvfs.f_bfree = 0
        cache.recycle()
        assert not exists('cache/implementation/1')

    def test_RecycleBadDirs(self):
        cache_limit.value = 10
        self.statvfs.f_bfree = 10
        self.touch('cache/implementation/1/foo')
        self.touch('cache/implementation/2/bar')
        self.touch(('cache/implementation/3/.unpack_size', '1'))
        cache.ensure(1, 0)
        assert not exists('cache/implementation/1')
        assert not exists('cache/implementation/2')
        assert not exists('cache/implementation/3')

        self.statvfs.f_bfree = 100
        self.touch('cache/implementation/1/foo')
        self.touch('cache/implementation/2/bar')
        cache.recycle()
        assert not exists('cache/implementation/1')
        assert not exists('cache/implementation/2')


if __name__ == '__main__':
    tests.main()
