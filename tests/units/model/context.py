#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.node import obs
from sugar_network.client import IPCConnection
from sugar_network.toolkit import coroutine, enforce


class ContextTest(tests.Test):

    def test_SetCommonLayerForPackages(self):
        self.start_online_client()
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(['common'], ipc.get(['context', guid, 'layer']))

        guid = ipc.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'layer': 'foo',
            })
        self.assertEqual(['foo', 'common'], ipc.get(['context', guid, 'layer']))

        guid = ipc.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'layer': ['common', 'bar'],
            })
        self.assertEqual(['common', 'bar'], ipc.get(['context', guid, 'layer']))


if __name__ == '__main__':
    tests.main()
