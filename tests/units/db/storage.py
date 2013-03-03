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

    def test_Record_set_blob_ByStream(self):
        storage = self.storage([BlobProperty('prop')])

        record = storage.get('guid1')
        data = '!' * BUFFER_SIZE * 2
        record.set_blob('prop', StringIO(data))
        self.assertEqual({
            'blob': tests.tmpdir + '/gu/guid1/prop.blob',
            'mtime': int(os.stat('gu/guid1/prop').st_mtime),
            'digest': hashlib.sha1(data).hexdigest(),
            },
            record.get('prop'))
        self.assertEqual(data, file('gu/guid1/prop.blob').read())

        record = storage.get('guid2')
        record.set_blob('prop', StringIO('12345'), 1)
        self.assertEqual({
            'blob': tests.tmpdir + '/gu/guid2/prop.blob',
            'mtime': int(os.stat('gu/guid2/prop').st_mtime),
            'digest': hashlib.sha1('1').hexdigest(),
            },
            record.get('prop'))
        self.assertEqual('1', file('gu/guid2/prop.blob').read())

    def test_Record_set_blob_ByPath(self):
        storage = self.storage([BlobProperty('prop')])

        record = storage.get('guid1')
        self.touch(('file', 'data'))
        record.set_blob('prop', tests.tmpdir + '/file')
        self.assertEqual({
            'blob': tests.tmpdir + '/gu/guid1/prop.blob',
            'mtime': int(os.stat('gu/guid1/prop').st_mtime),
            'digest': hashlib.sha1('data').hexdigest(),
            },
            record.get('prop'))
        self.assertEqual('data', file('gu/guid1/prop.blob').read())

        record = storage.get('guid2')
        self.touch(('directory/1', '1'))
        self.touch(('directory/2/3', '3'))
        self.touch(('directory/2/4/5', '5'))
        record.set_blob('prop', tests.tmpdir + '/directory')
        self.assertEqual({
            'blob': tests.tmpdir + '/gu/guid2/prop.blob',
            'mtime': int(os.stat('gu/guid2/prop').st_mtime),
            'digest': hashlib.sha1(
                '1' '1'
                '2/3' '3'
                '2/4/5' '5'
                ).hexdigest(),
            },
            record.get('prop'))
        util.assert_call('diff -r directory gu/guid2/prop.blob', shell=True)

    def test_Record_set_blob_ByUrl(self):
        storage = self.storage([BlobProperty('prop')])
        record = storage.get('guid1')

        record.set_blob('prop', url='http://sugarlabs.org')
        self.assertEqual({
            'url': 'http://sugarlabs.org',
            'mtime': int(os.stat('gu/guid1/prop').st_mtime),
            },
            record.get('prop'))
        assert not exists('gu/guid1/prop.blob')

    def test_Record_set_blob_ByValue(self):
        storage = self.storage([BlobProperty('prop')])
        record = storage.get('guid')

        record.set_blob('prop', '/foo/bar')
        self.assertEqual({
            'blob': tests.tmpdir + '/gu/guid/prop.blob',
            'mtime': int(os.stat('gu/guid/prop').st_mtime),
            'digest': hashlib.sha1('/foo/bar').hexdigest(),
            },
            record.get('prop'))
        self.assertEqual('/foo/bar', file('gu/guid/prop.blob').read())

    def test_delete(self):
        storage = self.storage([StoredProperty('prop')])

        assert not exists('ab/absent')
        storage.delete('absent')

        record = storage.get('guid')
        self.touch(('directory/1/2/3', '3'))
        record.set_blob('prop', 'directory')
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
