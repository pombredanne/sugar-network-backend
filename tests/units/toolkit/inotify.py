#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil

from __init__ import tests

from sugar_network.toolkit import inotify, coroutine


class InotifyTest(tests.Test):

    def test_monitor_Creates(self):
        events = []
        os.makedirs('files')

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        os.makedirs('files/5')
        coroutine.sleep(.1)
        os.makedirs('files/5/6')
        coroutine.sleep(.1)
        self.touch('files/5/7')
        coroutine.sleep(.1)
        self.touch('files/5/6/8')
        coroutine.sleep(.1)

        self.assertEqual([
            (inotify.EVENT_DIR_CREATED, '5'),
            (inotify.EVENT_DIR_CREATED, '5/6'),
            (inotify.EVENT_FILE_UPDATED, '5/7'),
            (inotify.EVENT_FILE_UPDATED, '5/6/8'),
            ],
            events)

    def test_monitor_PopulateOnCreates(self):
        events = []
        os.makedirs('files')

        def monitor():
            delay = .5
            for event in inotify.monitor('files'):
                if delay:
                    coroutine.sleep(delay)
                    delay = None
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        os.makedirs('files/1')
        os.makedirs('files/1/2')
        self.touch('files/1/3')
        self.touch('files/1/2/4')
        coroutine.sleep(1)
        self.assertEqual(
                sorted([
                    (inotify.EVENT_DIR_CREATED, '1'),
                    (inotify.EVENT_DIR_CREATED, '1/2'),
                    (inotify.EVENT_FILE_UPDATED, '1/3'),
                    (inotify.EVENT_FILE_UPDATED, '1/2/4'),
                    ]),
                sorted(events))

    def test_monitor_Deletes(self):
        os.makedirs('files/1')
        os.makedirs('files/1/2')
        self.touch('files/1/3')
        self.touch('files/1/2/4')
        events = []

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        os.unlink('files/1/2/4')
        os.unlink('files/1/3')
        shutil.rmtree('files/1/2')
        shutil.rmtree('files/1')
        coroutine.sleep(.5)
        self.assertEqual([
            (inotify.EVENT_FILE_DELETED, '1/2/4'),
            (inotify.EVENT_FILE_DELETED, '1/3'),
            (inotify.EVENT_DIR_DELETED, '1/2'),
            (inotify.EVENT_DIR_DELETED, '1'),
            ],
            events)

    def test_monitor_RecursiveDeletes(self):
        os.makedirs('files/1')
        os.makedirs('files/1/2')
        self.touch('files/1/3')
        self.touch('files/1/2/4')
        events = []

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        shutil.rmtree('files/1')
        coroutine.sleep(.5)
        self.assertEqual(sorted([
            (inotify.EVENT_FILE_DELETED, '1/2/4'),
            (inotify.EVENT_FILE_DELETED, '1/3'),
            (inotify.EVENT_DIR_DELETED, '1/2'),
            (inotify.EVENT_DIR_DELETED, '1'),
            ]),
            sorted(events))

    def test_monitor_Moves(self):
        os.makedirs('files/1')
        self.touch('files/1/2')
        os.makedirs('files/3')
        events = []

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        shutil.move('files/1/2', 'files/1/2_')
        shutil.move('files/3', 'files/3_')
        coroutine.sleep(.5)
        self.assertEqual([
            (inotify.EVENT_FILE_MOVED_FROM, '1/2'),
            (inotify.EVENT_FILE_UPDATED, '1/2_'),
            (inotify.EVENT_DIR_MOVED_FROM, '3'),
            (inotify.EVENT_DIR_CREATED, '3_'),
            ],
            events)

    def test_monitor_RecursiveMovesFrom(self):
        os.makedirs('files/1/2/3')
        self.touch('files/1/2/4')
        self.touch('files/1/2/3/5')
        events = []

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        shutil.move('files/1', '.')
        coroutine.sleep(.5)
        self.assertEqual([
            (inotify.EVENT_DIR_MOVED_FROM, '1'),
            ],
            events)

    def test_monitor_RecursiveMoves(self):
        os.makedirs('files/1/2/3')
        self.touch('files/1/2/4')
        self.touch('files/1/2/3/5')
        events = []

        def monitor():
            for event in inotify.monitor('files'):
                events.append(event)
        coroutine.spawn(monitor)
        coroutine.dispatch()

        shutil.move('files/1', 'files/1_')
        coroutine.sleep(.5)
        self.assertEqual(sorted([
            (inotify.EVENT_DIR_MOVED_FROM, '1'),
            (inotify.EVENT_DIR_CREATED, '1_'),
            (inotify.EVENT_DIR_CREATED, '1_/2'),
            (inotify.EVENT_FILE_UPDATED, '1_/2/4'),
            (inotify.EVENT_DIR_CREATED, '1_/2/3'),
            (inotify.EVENT_FILE_UPDATED, '1_/2/3/5'),
            ]),
            sorted(events))

    def test_monitor_Updates(self):
        self.touch(('files/probe', '1'))
        events = []

        def monitor():
            for event, path in inotify.monitor('files'):
                events.append((event, path, file('files/' + path).read()))
        coroutine.spawn(monitor)
        coroutine.dispatch()

        with file('files/probe', 'w') as f:
            f.write('2')
        coroutine.sleep(.1)
        self.assertEqual([
            (inotify.EVENT_FILE_UPDATED, 'probe', '2'),
            ],
            events)

        with file('files/probe', 'w') as f:
            f.write('3')
        coroutine.sleep(.1)
        self.assertEqual([
            (inotify.EVENT_FILE_UPDATED, 'probe', '2'),
            (inotify.EVENT_FILE_UPDATED, 'probe', '3'),
            ],
            events)

    def test_monitor_RootDelete(self):
        os.makedirs('files')
        events = []

        def monitor():
            try:
                for event in inotify.monitor('files'):
                    events.append(event)
            except Exception, e:
                events.append(str(e))
        coroutine.spawn(monitor)
        coroutine.dispatch()

        shutil.rmtree('files')
        coroutine.sleep(.1)
        self.assertEqual([
            (inotify.EVENT_DIR_DELETED, '.'),
            'Root directory deleted',
            ],
            events)


if __name__ == '__main__':
    tests.main()
