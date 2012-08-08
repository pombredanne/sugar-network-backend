#!/usr/bin/env python
# sugar-lint: disable

import os

from __init__ import tests

from sugar_network.toolkit import mounts_monitor
from active_toolkit import coroutine


class MountsMonitorTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        mounts_monitor._COMPLETE_MOUNT_TIMEOUT = 0

    def test_Populate(self):
        self.touch('mnt/foo')
        self.touch('mnt/bar')
        self.touch('mnt/fake')

        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('foo', found.append, None)
        mounts_monitor.connect('bar', found.append, None)
        self.assertEqual(
                ['mnt/foo', 'mnt/bar'],
                found)

    def test_Found(self):
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('foo', found.append, None)
        mounts_monitor.connect('bar', found.append, None)

        coroutine.dispatch()
        self.touch('mnt/foo')
        self.touch('mnt/bar')
        self.touch('mnt/fake')
        coroutine.sleep(.5)

        self.assertEqual(
                ['mnt/foo', 'mnt/bar'],
                found)

    def test_Lost(self):
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        lost = []
        mounts_monitor.connect('foo', found.append, lost.append)
        mounts_monitor.connect('bar', found.append, lost.append)

        coroutine.dispatch()
        self.touch('mnt/foo')
        self.touch('mnt/bar')
        self.touch('mnt/fake')
        os.unlink('mnt/foo')
        os.unlink('mnt/bar')
        os.unlink('mnt/fake')
        coroutine.sleep(.5)

        self.assertEqual(
                ['mnt/foo', 'mnt/bar'],
                found)
        self.assertEqual(
                ['mnt/foo', 'mnt/bar'],
                lost)

    def test_FoundTimeout(self):
        mounts_monitor._COMPLETE_MOUNT_TIMEOUT = 2
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('probe', found.append, None)

        coroutine.dispatch()
        self.touch('mnt/probe')
        coroutine.sleep(1)
        self.assertEqual([], found)
        coroutine.sleep(1.5)
        self.assertEqual(['mnt/probe'], found)


if __name__ == '__main__':
    tests.main()
