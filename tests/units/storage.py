#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import threading
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from active_document import env
from active_document.metadata import Metadata, ActiveProperty
from active_document.metadata import BlobProperty
from active_document.storage import Storage, _PAGE_SIZE


class StorageTest(tests.Test):

    def storage(self, props):

        class Test(object):
            pass

        metadata = Metadata(Test)
        for i in props:
            metadata[i.name] = i
        return Storage(tests.tmpdir, metadata)

    def test_get(self):
        storage = self.storage([ActiveProperty('prop', slot=1)])
        self.assertRaises(env.NotFound, storage.get, '1')
        storage.put('1', {'seqno': 1, 'prop': 'value'})
        self.assertEqual('value', storage.get('1').get('prop'))

    def test_Walkthrough(self):
        storage = self.storage([
            ActiveProperty('guid', slot=0),
            ActiveProperty('prop_1', slot=1),
            ActiveProperty('prop_2', slot=2),
            ])

        storage.put('1', {'seqno': 1, 'prop_1': 'value_1', 'prop_2': 'value_2', 'guid': '1'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_2', record.get('prop_2'))

        storage.put('1', {'seqno': 2, 'prop_2': 'value_3'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_3', record.get('prop_2'))

        storage.put('2', {'seqno': 3, 'prop_1': 'value_4', 'guid': '2'})

        record = storage.get('2')
        self.assertEqual('value_4', record.get('prop_1'))

        self.assertEqual(
                sorted(['1', '2']),
                sorted([guid for guid, props in storage.walk(0)]))

        storage.delete('1')

        self.assertEqual(
                sorted(['2']),
                sorted([guid for guid, props in storage.walk(0)]))

        storage.delete('2')

        self.assertEqual(
                sorted([]),
                sorted([guid for guid, props in storage.walk(0)]))

    def test_BLOBs(self):
        storage = self.storage([])

        stream = StringIO('foo')
        self.assertEqual(False, storage.set_blob(1, 'guid', 'blob', stream))

        path = storage.get_blob('guid', 'blob')
        self.assertEqual('foo', file(path).read())

        data = '!' * _PAGE_SIZE * 2
        stream = StringIO(data)
        self.assertEqual(False, storage.set_blob(2, 'guid', 'blob', stream))

        path = storage.get_blob('guid', 'blob')
        self.assertEqual(data, file(path).read())

        stream = StringIO('12345')
        self.assertEqual(False, storage.set_blob(3, 'guid', 'blob', stream, 1))
        self.assertEqual('1', file('gu/guid/blob').read())

        self.assertEqual(False, storage.set_blob(4, 'guid', 'blob', stream, 2))
        self.assertEqual('23', file('gu/guid/blob').read())

        storage.put('guid', {'seqno': 5, 'guid': 'guid'})
        self.assertEqual(True, storage.set_blob(6, 'guid', 'blob', stream))

    def test_diff(self):
        storage = self.storage([
            ActiveProperty('prop_1', slot=1),
            BlobProperty('prop_3'),
            ])

        storage.put('guid', {'seqno': 1, 'prop_1': 'value'})
        storage.set_blob(2, 'guid', 'prop_3', StringIO('blob'))

        os.utime('gu/guid/prop_1', (1, 1))
        os.utime('gu/guid/prop_3', (2, 2))

        traits, blobs = storage.diff('guid', [1, 2, 3, 4])
        self.assertEqual(
                {
                    'prop_1': ('value', 1),
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [2, 3, 4])
        self.assertEqual(
                {},
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [3, 4])
        self.assertEqual(
                {},
                traits)
        self.assertEqual(
                {},
                blobs)

    def test_merge(self):
        storage = self.storage([
            ActiveProperty('guid', slot=0),
            ActiveProperty('prop_1', slot=1),
            BlobProperty('prop_3'),
            ])

        diff = {
                'prop_1': ('value', 1),
                'prop_3': (StringIO('blob'), 4),
                }

        self.assertEqual(False, storage.merge(1, 'guid_1', diff))
        assert exists('gu/guid_1/.seqno')
        assert not exists('gu/guid_1/guid')
        assert os.stat('gu/guid_1/prop_1').st_mtime == 1
        self.assertEqual('"value"', file('gu/guid_1/prop_1').read())
        assert os.stat('gu/guid_1/prop_3').st_mtime == 4
        self.assertEqual('blob', file('gu/guid_1/prop_3').read())

        diff['guid'] = ('fake', 5)
        self.assertRaises(RuntimeError, storage.merge, 2, 'guid_2', diff)

        diff['guid'] = ('guid_2', 5)
        self.assertEqual(True, storage.merge(2, 'guid_2', diff))
        assert exists('gu/guid_2/.seqno')
        assert os.stat('gu/guid_2/guid').st_mtime == 5
        self.assertEqual('"guid_2"', file('gu/guid_2/guid').read())

        ts = int(time.time())
        storage.put('guid_3', {'seqno': 3, 'prop_1': 'value_2'})
        storage.set_blob(2, 'guid_3', 'prop_3', StringIO('blob_2'))

        diff.pop('guid')
        self.assertEqual(False, storage.merge(4, 'guid_3', diff))
        assert os.stat('gu/guid_3/prop_1').st_mtime >= ts
        self.assertEqual('"value_2"', file('gu/guid_3/prop_1').read())
        assert os.stat('gu/guid_3/prop_3').st_mtime >= ts
        self.assertEqual('blob_2', file('gu/guid_3/prop_3').read())

    def test_put_Times(self):
        storage = self.storage([
            ActiveProperty('prop_1', slot=1),
            BlobProperty('prop_3'),
            ])

        ts = int(time.time())

        storage.put('guid', {'seqno': 1, 'prop_1': 'value'})
        self.assertEqual(1, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('gu/guid/prop_1.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('gu/guid/prop_1').st_mtime

        self.assertEqual(
                int(os.stat('gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

        time.sleep(1)
        ts += 1
        storage.put('guid', {'seqno': 3, 'prop_1': 'value'})
        self.assertEqual(3, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('gu/guid/prop_1.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('gu/guid/prop_1').st_mtime
        self.assertEqual(
                int(os.stat('gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

    def test_walk(self):
        storage = self.storage([ActiveProperty('guid', slot=0)])

        self.override(time, 'time', lambda: 0)
        storage.merge(1, '1', {'guid': ['1', 0]})
        storage.merge(2, '2', {'guid': ['2', 0]})
        storage.merge(3, '3', {'guid': ['3', 0]})

        self.assertEqual(
                sorted(['1', '2', '3']),
                sorted([i for i, __ in storage.walk(0)]))
        self.assertEqual(
                [],
                [i for i, __ in storage.walk(1)])

        self.override(time, 'time', lambda: 1)
        storage.merge(4, '4', {'guid': ['4', 0]})
        self.assertEqual(
                sorted(['1', '2', '3', '4']),
                sorted([i for i, __ in storage.walk(0)]))
        self.assertEqual(
                sorted(['4']),
                sorted([i for i, __ in storage.walk(1)]))
        self.assertEqual(
                [],
                [i for i, __ in storage.walk(2)])

        self.override(time, 'time', lambda: 2)
        storage.put('5', {'seqno': 1, 'guid': '5'})
        self.assertEqual(
                sorted(['1', '2', '3', '4', '5']),
                sorted([i for i, __ in storage.walk(0)]))
        self.assertEqual(
                sorted(['4', '5']),
                sorted([i for i, __ in storage.walk(1)]))
        self.assertEqual(
                sorted(['5']),
                sorted([i for i, __ in storage.walk(2)]))
        self.assertEqual(
                [],
                [i for i, __ in storage.walk(3)])

    def test_walk_seqno(self):
        storage = self.storage([ActiveProperty('guid', slot=0)])
        storage.put('1', {'seqno': 1, 'guid': '1'})
        self.assertEqual(
                [{'guid': '1', 'seqno': storage.get('1').get('seqno')}],
                [props for __, props in storage.walk(0)])

    def test_walk_SkipGuidLess(self):
        storage = self.storage([
            ActiveProperty('guid', slot=0),
            ActiveProperty('prop', slot=1),
            ])
        storage.merge(1, '1', {'prop': ['1', 0]})
        self.assertEqual(
                [],
                [props for __, props in storage.walk(0)])


if __name__ == '__main__':
    tests.main()
