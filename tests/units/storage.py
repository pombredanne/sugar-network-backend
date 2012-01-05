#!/usr/bin/env python
# sugar-lint: disable

import time
import threading
from cStringIO import StringIO

from __init__ import tests

from active_document import env
from active_document.metadata import Metadata, ActiveProperty
from active_document.storage import Storage, _PAGE_SIZE


class StorageTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

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
                sorted([(guid, props) for guid, props in storage.walk()]))

        storage.delete('1')

        self.assertEqual(
                sorted([
                    ('2', {'prop_1': 'value_4'}),
                    ]),
                sorted([(guid, props) for guid, props in storage.walk()]))

        storage.delete('2')

        self.assertEqual(
                sorted([]),
                sorted([(guid, props) for guid, props in storage.walk()]))

    def test_BLOBs(self):
        storage = self.storage([])

        stream = StringIO('foo')
        storage.receive('guid', 'blob', stream)

        stream = StringIO()
        storage.send('guid', 'blob', stream)
        self.assertEqual('foo', stream.getvalue())

        data = '!' * _PAGE_SIZE * 2
        stream = StringIO(data)
        storage.receive('guid', 'blob', stream)

        stream = StringIO()
        storage.send('guid', 'blob', stream)
        self.assertEqual(data, stream.getvalue())

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


if __name__ == '__main__':
    tests.main()
