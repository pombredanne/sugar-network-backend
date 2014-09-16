#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit.coroutine import Spooler, spawn, sleep, this


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

    def test_this_Reset(self):
        probe = []

        this.prop1 = 1
        this.add_property('prop2', lambda: len(probe))

        def child():
            probe.append(this.prop1)
            probe.append(this.prop2)
            this.prop1 = 2
            probe.append(this.prop1)
            probe.append(this.prop2)
            this.reset()
            probe.append(this.prop1)
            probe.append(this.prop2)

        spawn(child).join()
        self.assertEqual([
            1, 1,
            2, 1,
            1, 5,
            ], probe)

    def test_this_ResetTheSameCoroutine(self):
        probe = []

        this.prop1 = 1
        this.add_property('prop2', lambda: len(probe))

        probe.append(this.prop1)
        probe.append(this.prop2)
        this.prop1 = 2
        probe.append(this.prop1)
        probe.append(this.prop2)
        this.reset()
        probe.append(this.prop1)
        probe.append(this.prop2)

        self.assertEqual([
            1, 1,
            2, 1,
            2, 5,
            ], probe)


if __name__ == '__main__':
    tests.main()
