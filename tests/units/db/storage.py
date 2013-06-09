#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
import hashlib
import threading
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network.db import env
from sugar_network.db.metadata import Metadata, StoredProperty
from sugar_network.db.metadata import BlobProperty
from sugar_network.db.storage import Storage
from sugar_network.toolkit import BUFFER_SIZE, util


class StorageTest(tests.Test):

    def storage(self, props):

        class Test(object):
            pass

        metadata = Metadata(Test)
        for i in props:
            metadata[i.name] = i
        return Storage(tests.tmpdir, metadata)

    def test_Record_get(self):
        storage = self.storage([StoredProperty('prop')])

        self.assertEqual(None, storage.get('guid').get('prop'))
        self.touch(('gu/guid/prop', json.dumps({
            'value': 'value',
            'foo': 'bar',
            })))
        self.assertEqual({
            'value': 'value',
            'foo': 'bar',
            'mtime': int(os.stat('gu/guid/prop').st_mtime),
            },
            storage.get('guid').get('prop'))

    def test_Record_set(self):
        storage = self.storage([StoredProperty('prop')])

        storage.get('guid').set('prop', value='value', foo='bar')
        self.assertEqual({
            'value': 'value',
            'foo': 'bar',
            'mtime': int(os.stat('gu/guid/prop').st_mtime),
            },
            storage.get('guid').get('prop'))

    def test_delete(self):
        storage = self.storage([StoredProperty('prop')])

        assert not exists('ab/absent')
        storage.delete('absent')

        record = storage.get('guid')
        record.set('prop', value='value')
        assert exists('gu/guid')
        storage.delete('guid')
        assert not exists('gu/guid')

    def test_Record_consistent(self):
        storage = self.storage([
            StoredProperty('guid'),
            StoredProperty('prop'),
            ])
        record = storage.get('guid')

        self.assertEqual(False, record.consistent)

        record.set('prop', value='value')
        self.assertEqual(False, record.consistent)

        record.set('guid', value='value')
        self.assertEqual(True, record.consistent)

    def test_walk(self):
        storage = self.storage([StoredProperty('guid')])

        storage.get('guid1').set('guid', value=1, mtime=1)
        storage.get('guid2').set('guid', value=2, mtime=2)
        storage.get('guid3').set('guid', value=3, mtime=3)

        self.assertEqual(
                sorted(['guid1', 'guid2', 'guid3']),
                sorted([i for i in storage.walk(0)]))

        self.assertEqual(
                sorted(['guid2', 'guid3']),
                sorted([i for i in storage.walk(1)]))

        self.assertEqual(
                sorted(['guid3']),
                sorted([i for i in storage.walk(2)]))

        self.assertEqual(
                sorted([]),
                sorted([i for i in storage.walk(3)]))

    def test_walk_SkipGuidLess(self):
        storage = self.storage([
            StoredProperty('guid'),
            StoredProperty('prop'),
            ])

        record = storage.get('guid1')
        record.set('guid', value=1)
        record.set('prop', value=1)

        record = storage.get('guid2')
        record.set('prop', value=2)

        record = storage.get('guid3')
        record.set('guid', value=3)
        record.set('prop', value=3)

        self.assertEqual(
                sorted(['guid1', 'guid3']),
                sorted([i for i in storage.walk(0)]))


if __name__ == '__main__':
    tests.main()
