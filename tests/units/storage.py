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
from active_document.metadata import AggregatorProperty, BlobProperty
from active_document.metadata import CounterProperty
from active_document.storage import Storage, _PAGE_SIZE


class StorageTest(tests.Test):

    def storage(self, props):

        class Test(object):
            pass

        metadata = Metadata(Test)
        for i in props:
            metadata[i.name] = i
        return Storage(metadata)

    def test_get(self):
        storage = self.storage([ActiveProperty('prop', slot=1)])
        self.assertRaises(env.NotFound, storage.get, '1')
        storage.put('1', {'prop': 'value'})
        self.assertEqual('value', storage.get('1').get('prop'))

    def test_Walkthrough(self):
        storage = self.storage([
            ActiveProperty('guid', slot=0),
            ActiveProperty('prop_1', slot=1),
            ActiveProperty('prop_2', slot=2),
            ])

        storage.put('1', {'prop_1': 'value_1', 'prop_2': 'value_2', 'guid': '1'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_2', record.get('prop_2'))

        storage.put('1', {'prop_2': 'value_3'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_3', record.get('prop_2'))

        storage.put('2', {'prop_1': 'value_4', 'guid': '2'})

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
        self.assertEqual(None, storage.set_blob('guid', 'blob', stream))

        stream = storage.get_blob('guid', 'blob')
        self.assertEqual('foo', stream.read())

        data = '!' * _PAGE_SIZE * 2
        stream = StringIO(data)
        self.assertEqual(None, storage.set_blob('guid', 'blob', stream))

        stream = storage.get_blob('guid', 'blob')
        self.assertEqual(data, stream.read())

        stream = StringIO('12345')
        self.assertEqual(None, storage.set_blob('guid', 'blob', stream, 1))
        self.assertEqual('1', file('test/gu/guid/blob').read())

        self.assertEqual(None, storage.set_blob('guid', 'blob', stream, 2))
        self.assertEqual('23', file('test/gu/guid/blob').read())

        storage.put('guid', {'guid': 'guid'})
        self.assertEqual(6, storage.set_blob('guid', 'blob', stream))

    def test_Aggregates(self):
        storage = self.storage([])

        self.assertEqual(0, storage.is_aggregated('guid', 'prop', 'probe'))
        self.assertEqual(0, storage.count_aggregated('guid', 'prop'))

        storage.aggregate('guid', 'prop', 'probe')
        self.assertEqual(1, storage.is_aggregated('guid', 'prop', 'probe'))
        self.assertEqual(1, storage.count_aggregated('guid', 'prop'))

        storage.aggregate('guid', 'prop', 'probe2')
        self.assertEqual(1, storage.is_aggregated('guid', 'prop', 'probe'))
        self.assertEqual(1, storage.is_aggregated('guid', 'prop', 'probe2'))
        self.assertEqual(2, storage.count_aggregated('guid', 'prop'))

        storage.disaggregate('guid', 'prop', 'probe')
        self.assertEqual(0, storage.is_aggregated('guid', 'prop', 'probe'))
        self.assertEqual(1, storage.is_aggregated('guid', 'prop', 'probe2'))
        self.assertEqual(1, storage.count_aggregated('guid', 'prop'))

        storage.disaggregate('guid', 'prop', 'probe2')
        self.assertEqual(0, storage.is_aggregated('guid', 'prop', 'probe'))
        self.assertEqual(0, storage.is_aggregated('guid', 'prop', 'probe2'))
        self.assertEqual(0, storage.count_aggregated('guid', 'prop'))

    def test_diff(self):
        storage = self.storage([
            ActiveProperty('prop_1', slot=1),
            AggregatorProperty('prop_2', 'counter'),
            BlobProperty('prop_3'),
            CounterProperty('prop_4', slot=2),
            ])

        storage.put('guid', {'prop_1': 'value', 'prop_4': '0'})
        storage.aggregate('guid', 'prop_2', 'enabled')
        storage.disaggregate('guid', 'prop_2', 'disabled')
        storage.set_blob('guid', 'prop_3', StringIO('blob'))

        os.utime('test/gu/guid/prop_1', (1, 1))
        os.utime('test/gu/guid/prop_2.enabled.value', (2, 2))
        os.utime('test/gu/guid/prop_2.disabled.value', (3, 3))
        os.utime('test/gu/guid/prop_3', (2, 2))

        traits, blobs = storage.diff('guid', [1, 2, 3, 4])
        self.assertEqual(
                {
                    'prop_1': ('value', 1),
                    'prop_2': [(('enabled', True), 2), (('disabled', False), 3)],
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [2, 3, 4])
        self.assertEqual(
                {
                    'prop_2': [(('enabled', True), 2), (('disabled', False), 3)],
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [3, 4])
        self.assertEqual(
                {
                    'prop_2': [(('disabled', False), 3)],
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [4])
        self.assertEqual(
                {},
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid', [5])
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
            AggregatorProperty('prop_2', 'counter'),
            BlobProperty('prop_3'),
            ])

        diff = {
                'prop_1': ('value', 1),
                'prop_2': [(('enabled', True), 2), (('disabled', False), 3)],
                'prop_3': (StringIO('blob'), 4),
                }

        self.assertEqual(None, storage.merge('guid_1', diff))
        assert exists('test/gu/guid_1/.seqno')
        assert not exists('test/gu/guid_1/guid')
        assert os.stat('test/gu/guid_1/prop_1').st_mtime == 1
        self.assertEqual('"value"', file('test/gu/guid_1/prop_1').read())
        assert os.stat('test/gu/guid_1/prop_2.enabled.value').st_mtime == 2
        assert storage.is_aggregated('guid_1', 'prop_2', 'enabled')
        assert os.stat('test/gu/guid_1/prop_2.disabled.value').st_mtime == 3
        assert not storage.is_aggregated('guid_1', 'prop_2', 'disabled')
        assert os.stat('test/gu/guid_1/prop_3').st_mtime == 4
        self.assertEqual('blob', file('test/gu/guid_1/prop_3').read())

        diff['guid'] = ('fake', 5)
        self.assertRaises(RuntimeError, storage.merge, 'guid_2', diff)

        diff['guid'] = ('guid_2', 5)
        self.assertEqual(2, storage.merge('guid_2', diff))
        assert exists('test/gu/guid_2/.seqno')
        assert os.stat('test/gu/guid_2/guid').st_mtime == 5
        self.assertEqual('"guid_2"', file('test/gu/guid_2/guid').read())

        ts = int(time.time())
        storage.put('guid_3', {'prop_1': 'value_2'})
        storage.disaggregate('guid_3', 'prop_2', 'enabled')
        storage.aggregate('guid_3', 'prop_2', 'disabled')
        storage.set_blob('guid_3', 'prop_3', StringIO('blob_2'))

        diff.pop('guid')
        self.assertEqual(None, storage.merge('guid_3', diff))
        assert os.stat('test/gu/guid_3/prop_1').st_mtime >= ts
        self.assertEqual('"value_2"', file('test/gu/guid_3/prop_1').read())
        assert os.stat('test/gu/guid_3/prop_2.enabled.value').st_mtime >= ts
        assert not storage.is_aggregated('guid_3', 'prop_2', 'enabled')
        assert os.stat('test/gu/guid_3/prop_2.disabled.value').st_mtime >= ts
        assert storage.is_aggregated('guid_3', 'prop_2', 'disabled')
        assert os.stat('test/gu/guid_3/prop_3').st_mtime >= ts
        self.assertEqual('blob_2', file('test/gu/guid_3/prop_3').read())

    def test_merge_ZeroSeqnoByDefault(self):
        storage = self.storage([ActiveProperty('guid', slot=0)])
        self.assertEqual(0, storage.merge('guid', {'guid': ('guid', 1)}, False))
        self.assertEqual(0, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/guid.seqno').st_mtime),
                storage.get('guid').get('seqno'))

    def test_put_Times(self):
        storage = self.storage([
            ActiveProperty('prop_1', slot=1),
            AggregatorProperty('prop_2', 'counter'),
            BlobProperty('prop_3'),
            ])

        ts = int(time.time())

        storage.put('guid', {'prop_1': 'value'})
        self.assertEqual(1, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_1.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_1').st_mtime

        storage.aggregate('guid', 'prop_2', 'value')
        self.assertEqual(2, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_2.value.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_2.value.value').st_mtime

        storage.set_blob('guid', 'prop_3', StringIO('value'))
        self.assertEqual(3, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_3.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_3').st_mtime

        self.assertEqual(
                int(os.stat('test/gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

        time.sleep(1)
        ts += 1
        storage.put('guid', {'prop_1': 'value'})
        self.assertEqual(4, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_1.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_1').st_mtime
        self.assertEqual(
                int(os.stat('test/gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

        time.sleep(1)
        ts += 1
        storage.disaggregate('guid', 'prop_2', 'value')
        self.assertEqual(5, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_2.value.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_2.value.value').st_mtime
        self.assertEqual(
                int(os.stat('test/gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

        time.sleep(1)
        ts += 1
        storage.set_blob('guid', 'prop_3', StringIO('value'))
        self.assertEqual(6, storage.get('guid').get('seqno'))
        self.assertEqual(
                int(os.stat('test/gu/guid/prop_3.seqno').st_mtime),
                storage.get('guid').get('seqno'))
        assert ts <= os.stat('test/gu/guid/prop_3').st_mtime
        self.assertEqual(
                int(os.stat('test/gu/guid/.seqno').st_mtime),
                storage.get('guid').get('seqno'))

    def test_walk(self):
        storage = self.storage([ActiveProperty('guid', slot=0)])

        self.override(time, 'time', lambda: 0)
        storage.merge('1', {'guid': ['1', 0]})
        storage.merge('2', {'guid': ['2', 0]})
        storage.merge('3', {'guid': ['3', 0]})

        self.assertEqual(
                sorted(['1', '2', '3']),
                sorted([i for i, __ in storage.walk(0)]))
        self.assertEqual(
                [],
                [i for i, __ in storage.walk(1)])

        self.override(time, 'time', lambda: 1)
        storage.merge('4', {'guid': ['4', 0]}, True)
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
        storage.put('5', {'guid': '5'})
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
        storage.put('1', {'guid': '1'})
        self.assertEqual(
                [{'guid': '1', 'seqno': storage.get('1').get('seqno')}],
                [props for __, props in storage.walk(0)])

    def test_walk_SkipGuidLess(self):
        storage = self.storage([
            ActiveProperty('guid', slot=0),
            ActiveProperty('prop', slot=1),
            ])
        storage.merge('1', {'prop': ['1', 0]})
        self.assertEqual(
                [],
                [props for __, props in storage.walk(0)])


if __name__ == '__main__':
    tests.main()
