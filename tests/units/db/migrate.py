#!/usr/bin/env python
# sugar-lint: disable

import os
import cPickle as pickle
from os.path import exists, lexists

from __init__ import tests

from sugar_network import db
from sugar_network.db import document, env
from sugar_network.db import directory as directory_
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter


class MigrateTest(tests.Test):

    def test_MissedProps(self):

        class Document(document.Document):

            @db.indexed_property(prefix='P')
            def prop1(self, value):
                return value

            @db.indexed_property(prefix='A', default='default')
            def prop2(self, value):
                return value

        self.touch(
                ('gu/guid/.seqno', ''),
                ('gu/guid/guid', '"guid"'),
                ('gu/guid/guid.seqno', ''),
                ('gu/guid/ctime', '1'),
                ('gu/guid/ctime.seqno', ''),
                ('gu/guid/mtime', '1'),
                ('gu/guid/mtime.seqno', ''),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass

        assert not exists('gu/guid/prop1')
        assert exists('gu/guid/prop2')

        doc = directory.get('guid')
        self.assertEqual(
                {'value': 'default', 'seqno': 0, 'mtime': int(os.stat('gu/guid/prop2').st_mtime)},
                doc.meta('prop2'))

    def test_ConvertToJson(self):

        class Document(document.Document):

            @db.indexed_property(prefix='P', default='value')
            def prop(self, value):
                return value

        guid_value = pickle.dumps({"value": "guid"})
        self.touch(('gu/guid/guid', guid_value))

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass

        doc = directory.get('guid')
        self.assertEqual(
                {'value': 'guid', 'mtime': int(os.stat('gu/guid/guid').st_mtime)},
                doc.meta('guid'))
        self.assertNotEqual(guid_value, file('gu/guid/guid').read())


if __name__ == '__main__':
    tests.main()
