#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit.coroutine import Spooler, spawn, sleep


class CoroutineTest(tests.Test):

    def test_Spooler_ContinuousFeeding(self):
        spooler = Spooler()
        events = []

        def consumer(num):
            while True:
                events[num].append(spooler.wait())

        for i in range(10):
            events.append([])
            spawn(consumer, i)
        sleep(.1)

        for i in range(10):
            spooler.notify_all(i)
        sleep(.1)
        self.assertEqual([range(10)] * 10, events)


if __name__ == '__main__':
    tests.main()
