#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.node import obs
from sugar_network.model.context import Context
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

    def test_DefaultImages(self):
        self.start_online_client()
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'guid': 'guid',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        assert exists('master/context/gu/guid/artifact_icon.blob')
        assert exists('master/context/gu/guid/icon.blob')
        assert exists('master/context/gu/guid/preview.blob')

    def test_RatingSort(self):
        directory = db.Volume('db', [Context])['context']

        directory.create({'guid': '1', 'type': 'activity', 'title': '', 'summary': '', 'description': '', 'rating': [0, 0]})
        directory.create({'guid': '2', 'type': 'activity', 'title': '', 'summary': '', 'description': '', 'rating': [1, 2]})
        directory.create({'guid': '3', 'type': 'activity', 'title': '', 'summary': '', 'description': '', 'rating': [1, 4]})
        directory.create({'guid': '4', 'type': 'activity', 'title': '', 'summary': '', 'description': '', 'rating': [10, 10]})
        directory.create({'guid': '5', 'type': 'activity', 'title': '', 'summary': '', 'description': '', 'rating': [30, 90]})

        self.assertEqual(
                ['1', '2', '3', '4', '5'],
                [i.guid for i in directory.find()[0]])
        self.assertEqual(
                ['1', '4', '2', '5', '3'],
                [i.guid for i in directory.find(order_by='rating')[0]])
        self.assertEqual(
                ['3', '5', '2', '4', '1'],
                [i.guid for i in directory.find(order_by='-rating')[0]])


if __name__ == '__main__':
    tests.main()
