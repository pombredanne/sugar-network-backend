#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil

from __init__ import tests

from sugar_network.toolkit import mounts_monitor
from active_toolkit import coroutine


class MountsMonitorTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        mounts_monitor._COMPLETE_MOUNT_TIMEOUT = 0.01

    def test_Populate(self):
        self.touch('mnt/1/foo')
        self.touch('mnt/2/foo')
        self.touch('mnt/2/bar')
        self.touch('mnt/3/fake')
        os.makedirs('mnt/4')

        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('foo', found.append, None)
        mounts_monitor.connect('bar', found.append, None)
        self.assertEqual(
                sorted(['mnt/1', 'mnt/2', 'mnt/2']),
                sorted(found))

    def test_Found(self):
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('foo', found.append, None)
        mounts_monitor.connect('bar', found.append, None)

        coroutine.dispatch()
        self.touch('mnt/1/foo')
        self.touch('mnt/2/foo')
        self.touch('mnt/2/bar')
        self.touch('mnt/3/fake')
        os.makedirs('mnt/4')
        coroutine.sleep(.5)

        self.assertEqual(
                sorted(['mnt/1', 'mnt/2', 'mnt/2']),
                sorted(found))

    def test_Lost(self):
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        lost = []
        mounts_monitor.connect('foo', found.append, lost.append)
        mounts_monitor.connect('bar', found.append, lost.append)

        coroutine.dispatch()
        self.touch('mnt/1/foo')
        self.touch('mnt/2/foo')
        self.touch('mnt/2/bar')
        self.touch('mnt/3/fake')
        os.makedirs('mnt/4')
        coroutine.sleep(.1)
        shutil.rmtree('mnt/1')
        shutil.rmtree('mnt/2')
        shutil.rmtree('mnt/3')
        shutil.rmtree('mnt/4')
        coroutine.sleep(.1)

        self.assertEqual(
                sorted(['mnt/1', 'mnt/2', 'mnt/2']),
                sorted(found))
        self.assertEqual(
                sorted(['mnt/1', 'mnt/2', 'mnt/2']),
                sorted(lost))

    def test_FoundTimeout(self):
        mounts_monitor._COMPLETE_MOUNT_TIMEOUT = 2
        os.makedirs('mnt')
        mounts_monitor.start('mnt')

        found = []
        mounts_monitor.connect('probe', found.append, None)

        coroutine.dispatch()
        self.touch('mnt/1/probe')
        coroutine.sleep(1)
        self.assertEqual([], found)
        coroutine.sleep(1.5)
        self.assertEqual(['mnt/1'], found)


if __name__ == '__main__':
    tests.main()
