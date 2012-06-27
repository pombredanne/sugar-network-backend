#!/usr/bin/env python
# sugar-lint: disable

import os
from os.path import exists, lexists

from __init__ import tests

from active_document import document, env
from active_document import directory as directory_
from active_document.directory import Directory
from active_document.metadata import active_property, BlobProperty
from active_document.index import IndexWriter


class MigrateTest(tests.Test):

    def test_To1(self):

        class Document(document.Document):

            @active_property(prefix='P', localized=True)
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.touch(
                ('gu/guid/.seqno', ''),
                ('gu/guid/guid', '"guid"'),
                ('gu/guid/guid.seqno', ''),
                ('gu/guid/ctime', '1'),
                ('gu/guid/ctime.seqno', ''),
                ('gu/guid/mtime', '1'),
                ('gu/guid/mtime.seqno', ''),
                ('gu/guid/layer', '["public"]'),
                ('gu/guid/layer.seqno', ''),
                ('gu/guid/user', '["me"]'),
                ('gu/guid/user.seqno', ''),
                ('gu/guid/prop', '"prop"'),
                ('gu/guid/prop.seqno', ''),
                ('gu/guid/blob', 'blob'),
                ('gu/guid/blob.seqno', ''),
                ('gu/guid/blob.sha1', 'digest'),
                )
        for i in os.listdir('gu/guid'):
            os.utime('gu/guid/%s' % i, (1, 1))

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass
        assert exists('layout')
        self.assertEqual(str(directory_._LAYOUT_VERSION), file('layout').read())

        assert not exists('gu/guid/.seqno')
        assert not exists('gu/guid/guid.seqno')
        assert not exists('gu/guid/ctime.seqno')
        assert not exists('gu/guid/mtime.seqno')
        assert not exists('gu/guid/layer.seqno')
        assert not exists('gu/guid/user.seqno')
        assert not exists('gu/guid/prop.seqno')
        assert not exists('gu/guid/blob.seqno')
        assert not exists('gu/guid/blob.sha1')
        assert exists('gu/guid/blob.blob')

        def test_meta():
            doc = directory.get('guid')
            self.assertEqual(
                    {'value': 'guid', 'mtime': 1, 'seqno': 1},
                    doc.meta('guid'))
            self.assertEqual(
                    {'value': 1, 'mtime': 1, 'seqno': 1},
                    doc.meta('ctime'))
            self.assertEqual(
                    {'value': 1, 'mtime': 1, 'seqno': 1},
                    doc.meta('mtime'))
            self.assertEqual(
                    {'value': ['public'], 'mtime': 1, 'seqno': 1},
                    doc.meta('layer'))
            self.assertEqual(
                    {'value': ['me'], 'mtime': 1, 'seqno': 1},
                    doc.meta('user'))
            self.assertEqual(
                    {'value': {env.DEFAULT_LANG: 'prop'}, 'mtime': 1, 'seqno': 1},
                    doc.meta('prop'))
            self.assertEqual(
                    {'digest': 'digest', 'mtime': 1, 'seqno': 1, 'mime_type': 'application/octet-stream', 'path': tests.tmpdir + '/gu/guid/blob.blob'},
                    doc.meta('blob'))
            self.assertEqual('blob', file('gu/guid/blob.blob').read())

        test_meta()

        directory.close()
        with file('layout', 'w') as f:
            f.write('*')
        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass
        self.assertEqual(str(directory_._LAYOUT_VERSION), file('layout').read())

        test_meta()

    def test_To1_MissedBlobs(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        self.touch(
                ('1/1/.seqno', ''),
                ('1/1/guid', '"guid"'),
                ('1/1/guid.seqno', ''),
                ('1/1/ctime', '1'),
                ('1/1/ctime.seqno', ''),
                ('1/1/mtime', '1'),
                ('1/1/mtime.seqno', ''),
                ('1/1/layer', '["public"]'),
                ('1/1/layer.seqno', ''),
                ('1/1/user', '["me"]'),
                ('1/1/user.seqno', ''),
                ('1/1/blob.seqno', ''),
                ('1/1/blob.sha1', 'digest'),

                ('2/2/.seqno', ''),
                ('2/2/guid', '"guid"'),
                ('2/2/guid.seqno', ''),
                ('2/2/ctime', '1'),
                ('2/2/ctime.seqno', ''),
                ('2/2/mtime', '1'),
                ('2/2/mtime.seqno', ''),
                ('2/2/layer', '["public"]'),
                ('2/2/layer.seqno', ''),
                ('2/2/user', '["me"]'),
                ('2/2/user.seqno', ''),
                ('2/2/blob.seqno', ''),
                ('2/2/blob.sha1', 'digest'),

                ('3/3/.seqno', ''),
                ('3/3/guid', '"guid"'),
                ('3/3/guid.seqno', ''),
                ('3/3/ctime', '1'),
                ('3/3/ctime.seqno', ''),
                ('3/3/mtime', '1'),
                ('3/3/mtime.seqno', ''),
                ('3/3/layer', '["public"]'),
                ('3/3/layer.seqno', ''),
                ('3/3/user', '["me"]'),
                ('3/3/user.seqno', ''),
                ('3/3/blob.seqno', ''),

                ('4/4/.seqno', ''),
                ('4/4/guid', '"guid"'),
                ('4/4/guid.seqno', ''),
                ('4/4/ctime', '1'),
                ('4/4/ctime.seqno', ''),
                ('4/4/mtime', '1'),
                ('4/4/mtime.seqno', ''),
                ('4/4/layer', '["public"]'),
                ('4/4/layer.seqno', ''),
                ('4/4/user', '["me"]'),
                ('4/4/user.seqno', ''),
                ('4/4/blob', 'blob'),
                ('4/4/blob.seqno', ''),
                )

        for i in os.listdir('1/1'):
            os.utime('1/1/%s' % i, (1, 1))
        for i in os.listdir('2/2'):
            os.utime('2/2/%s' % i, (2, 2))
        for i in os.listdir('3/3'):
            os.utime('3/3/%s' % i, (3, 3))
        os.symlink('/foo', '3/3/blob')
        for i in os.listdir('4/4'):
            os.utime('4/4/%s' % i, (4, 4))

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass

        assert not exists('1/1/blob')
        assert not exists('1/1/blob.seqno')
        assert not exists('1/1/blob.sha1')
        assert not exists('1/1/blob.blob')
        assert not exists('2/2/blob')
        assert not exists('2/2/blob.seqno')
        assert not exists('2/2/blob.sha1')
        assert not exists('2/2/blob.blob')
        assert not exists('3/3/blob')
        assert not lexists('3/3/blob')
        assert not exists('3/3/blob.seqno')
        assert not exists('3/3/blob.sha1')
        assert not exists('3/3/blob.blob')
        assert not lexists('3/3/blob.blob')
        assert exists('4/4/blob')
        assert not exists('4/4/blob.seqno')
        assert not exists('4/4/blob.sha1')
        assert exists('4/4/blob.blob')

        self.assertEqual(None, directory.get('1').meta('blob'))
        self.assertEqual(None, directory.get('2').meta('blob'))
        self.assertEqual(None, directory.get('3').meta('blob'))
        self.assertEqual(
                    {'digest': '', 'mtime': 4, 'seqno': 4, 'mime_type': 'application/octet-stream', 'path': tests.tmpdir + '/4/4/blob.blob'},
                    directory.get('4').meta('blob'))
        self.assertEqual('blob', file('4/4/blob.blob').read())

    def test_To1_MissedValues(self):

        class Document(document.Document):

            @active_property(prefix='P')
            def prop1(self, value):
                return value

            @active_property(prefix='A', default='default')
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
                ('gu/guid/layer', '["public"]'),
                ('gu/guid/layer.seqno', ''),
                ('gu/guid/user', '["me"]'),
                ('gu/guid/user.seqno', ''),
                ('gu/guid/prop1.seqno', ''),
                ('gu/guid/prop2.seqno', ''),
                )
        for i in os.listdir('gu/guid'):
            os.utime('gu/guid/%s' % i, (1, 1))

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass

        assert not exists('gu/guid/.seqno')
        assert not exists('gu/guid/guid.seqno')
        assert not exists('gu/guid/ctime.seqno')
        assert not exists('gu/guid/mtime.seqno')
        assert not exists('gu/guid/layer.seqno')
        assert not exists('gu/guid/user.seqno')
        assert exists('gu/guid/prop1')
        assert not exists('gu/guid/prop1.seqno')
        assert exists('gu/guid/prop2')
        assert not exists('gu/guid/prop2.seqno')

        doc = directory.get('guid')
        self.assertEqual(
                {'value': None, 'seqno': 1, 'mtime': os.stat('gu/guid/prop1').st_mtime},
                doc.meta('prop1'))
        assert os.stat('gu/guid/prop1').st_mtime > 1
        self.assertEqual(
                {'value': 'default', 'seqno': 1, 'mtime': os.stat('gu/guid/prop1').st_mtime},
                doc.meta('prop2'))
        assert os.stat('gu/guid/prop2').st_mtime > 1

    def test_MissedProps(self):

        class Document(document.Document):

            @active_property(prefix='P')
            def prop1(self, value):
                return value

            @active_property(prefix='A', default='default')
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
                ('gu/guid/layer', '["public"]'),
                ('gu/guid/layer.seqno', ''),
                ('gu/guid/user', '["me"]'),
                ('gu/guid/user.seqno', ''),
                )

        directory = Directory(tests.tmpdir, Document, IndexWriter)
        for i in directory.populate():
            pass

        assert exists('gu/guid/prop1')
        assert exists('gu/guid/prop2')

        doc = directory.get('guid')
        self.assertEqual(
                {'value': None, 'seqno': 0, 'mtime': os.stat('gu/guid/prop1').st_mtime},
                doc.meta('prop1'))
        self.assertEqual(
                {'value': 'default', 'seqno': 0, 'mtime': os.stat('gu/guid/prop1').st_mtime},
                doc.meta('prop2'))


if __name__ == '__main__':
    tests.main()
