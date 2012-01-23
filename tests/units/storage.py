#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import threading
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from active_document import env
from active_document.metadata import Metadata, ActiveProperty, GuidProperty
from active_document.metadata import AggregatorProperty, BlobProperty
from active_document.metadata import CounterProperty
from active_document.storage import Storage, _PAGE_SIZE


class StorageTest(tests.Test):

    def storage(self, props):
        metadata = Metadata()
        metadata.name = 'test'
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
            ActiveProperty('prop_1', slot=1),
            ActiveProperty('prop_2', slot=2),
            ])

        storage.put('1', {'prop_1': 'value_1', 'prop_2': 'value_2'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_2', record.get('prop_2'))

        storage.put('1', {'prop_2': 'value_3'})

        record = storage.get('1')
        self.assertEqual('value_1', record.get('prop_1'))
        self.assertEqual('value_3', record.get('prop_2'))

        storage.put('2', {'prop_1': 'value_4'})

        record = storage.get('2')
        self.assertEqual('value_4', record.get('prop_1'))

        self.assertEqual(
                sorted([
                    ('1', {'prop_1': 'value_1', 'prop_2': 'value_3'}),
                    ('2', {'prop_1': 'value_4'}),
                    ]),
                sorted([(guid, props) for guid, props in storage.walk(0)]))

        storage.delete('1')

        self.assertEqual(
                sorted([
                    ('2', {'prop_1': 'value_4'}),
                    ]),
                sorted([(guid, props) for guid, props in storage.walk(0)]))

        storage.delete('2')

        self.assertEqual(
                sorted([]),
                sorted([(guid, props) for guid, props in storage.walk(0)]))

    def test_BLOBs(self):
        storage = self.storage([])

        stream = StringIO('foo')
        storage.set_blob('guid', 'blob', stream)

        stream = StringIO()
        for i in storage.get_blob('guid', 'blob'):
            stream.write(i)
        self.assertEqual('foo', stream.getvalue())

        data = '!' * _PAGE_SIZE * 2
        stream = StringIO(data)
        storage.set_blob('guid', 'blob', stream)

        stream = StringIO()
        for i in storage.get_blob('guid', 'blob'):
            stream.write(i)
        self.assertEqual(data, stream.getvalue())

        stream = StringIO('12345')
        storage.set_blob('guid', 'blob', stream, 1)
        self.assertEqual('1', file('test/gu/guid/blob').read())

        storage.set_blob('guid', 'blob', stream, 2)
        self.assertEqual('23', file('test/gu/guid/blob').read())

    def test_Aggregates(self):
        storage = self.storage([])

        self.assertEqual(False, storage.is_aggregated('guid', 'prop', -1))
        self.assertEqual(0, storage.count_aggregated('guid', 'prop'))

        storage.aggregate('guid', 'prop', -1)
        self.assertEqual(True, storage.is_aggregated('guid', 'prop', -1))
        self.assertEqual(1, storage.count_aggregated('guid', 'prop'))

        storage.aggregate('guid', 'prop', -2)
        self.assertEqual(True, storage.is_aggregated('guid', 'prop', -1))
        self.assertEqual(True, storage.is_aggregated('guid', 'prop', -2))
        self.assertEqual(2, storage.count_aggregated('guid', 'prop'))

        storage.disaggregate('guid', 'prop', -1)
        self.assertEqual(False, storage.is_aggregated('guid', 'prop', -1))
        self.assertEqual(True, storage.is_aggregated('guid', 'prop', -2))
        self.assertEqual(1, storage.count_aggregated('guid', 'prop'))

        storage.disaggregate('guid', 'prop', -2)
        self.assertEqual(False, storage.is_aggregated('guid', 'prop', -1))
        self.assertEqual(False, storage.is_aggregated('guid', 'prop', -2))
        self.assertEqual(0, storage.count_aggregated('guid', 'prop'))

    def test_diff(self):
        storage = self.storage([
            ActiveProperty('prop_1', slot=1),
            AggregatorProperty('prop_2', 'counter'),
            BlobProperty('prop_3'),
            CounterProperty('prop_4', slot=2),
            ])

        storage.put('guid_1', {'prop_1': 'value_1', 'prop_4': '0'})
        storage.aggregate('guid_1', 'prop_2', 'enabled_1')
        storage.disaggregate('guid_1', 'prop_2', 'disabled_1')
        storage.set_blob('guid_1', 'prop_3', StringIO('blob_1'))

        os.utime('test/gu/guid_1/prop_1', (1, 1))
        os.utime('test/gu/guid_1/prop_2/enabled_1', (2, 2))
        os.utime('test/gu/guid_1/prop_2/disabled_1', (1, 1))
        os.utime('test/gu/guid_1/prop_3', (2, 2))

        traits, blobs = storage.diff('guid_1', 1)
        self.assertEqual(
                {
                    'prop_1': ('value_1', 1),
                    'prop_2': [(('disabled_1', False), 1), (('enabled_1', True), 2)],
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid_1/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid_1', 2)
        self.assertEqual(
                {
                    'prop_2': [(('enabled_1', True), 2)],
                    },
                traits)
        self.assertEqual(
                {
                    'prop_3': (tests.tmpdir + '/test/gu/guid_1/prop_3', 2),
                    },
                blobs)

        traits, blobs = storage.diff('guid_1', 3)
        self.assertEqual(
                { },
                traits)
        self.assertEqual(
                { },
                blobs)

    def test_merge(self):
        storage = self.storage([
            GuidProperty(),
            ActiveProperty('prop_1', slot=1),
            AggregatorProperty('prop_2', 'counter'),
            BlobProperty('prop_3'),
            ])

        diff = {
                'prop_1': ('value', 1),
                'prop_2': [(('enabled', True), 2), (('disabled', False), 3)],
                'prop_3': (StringIO('blob'), 4),
                }

        assert not storage.merge('guid_1', diff)
        assert not exists('test/gu/guid_1/.document')
        assert os.stat('test/gu/guid_1/prop_1').st_mtime == 1
        self.assertEqual('value', file('test/gu/guid_1/prop_1').read())
        assert os.stat('test/gu/guid_1/prop_2/enabled').st_mtime == 2
        assert storage.is_aggregated('guid_1', 'prop_2', 'enabled')
        assert os.stat('test/gu/guid_1/prop_2/disabled').st_mtime == 3
        assert not storage.is_aggregated('guid_1', 'prop_2', 'disabled')
        assert os.stat('test/gu/guid_1/prop_3').st_mtime == 4
        self.assertEqual('blob', file('test/gu/guid_1/prop_3').read())

        diff['guid'] = ('fake', 5)
        self.assertRaises(RuntimeError, storage.merge, 'guid_2', diff)

        diff['guid'] = ('guid_2', 5)
        assert storage.merge('guid_2', diff)
        assert exists('test/gu/guid_2/.document')
        assert os.stat('test/gu/guid_2/guid').st_mtime == 5
        self.assertEqual('guid_2', file('test/gu/guid_2/guid').read())

        ts = int(time.time())
        storage.put('guid_3', {'prop_1': 'value_2'})
        storage.disaggregate('guid_3', 'prop_2', 'enabled')
        storage.aggregate('guid_3', 'prop_2', 'disabled')
        storage.set_blob('guid_3', 'prop_3', StringIO('blob_2'))

        diff.pop('guid')
        assert not storage.merge('guid_3', diff)
        assert os.stat('test/gu/guid_3/prop_1').st_mtime >= ts
        self.assertEqual('value_2', file('test/gu/guid_3/prop_1').read())
        assert os.stat('test/gu/guid_3/prop_2/enabled').st_mtime >= ts
        assert not storage.is_aggregated('guid_3', 'prop_2', 'enabled')
        assert os.stat('test/gu/guid_3/prop_2/disabled').st_mtime >= ts
        assert storage.is_aggregated('guid_3', 'prop_2', 'disabled')
        assert os.stat('test/gu/guid_3/prop_3').st_mtime >= ts
        self.assertEqual('blob_2', file('test/gu/guid_3/prop_3').read())


if __name__ == '__main__':
    tests.main()
