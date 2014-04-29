#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import json
import sys
import stat
import time
import urllib2
import hashlib
from base64 import b64encode
from cStringIO import StringIO
from os.path import join, exists

import gobject

from __init__ import tests

from sugar_network import db
from sugar_network.db import storage, index
from sugar_network.db import directory as directory_
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, ranges


class VolumeTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        this.localcast = lambda x: x

    def test_EditLocalProps(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.PUBLIC | ACL.LOCAL)
            def prop2(self, value):
                return value

            @db.stored_property()
            def prop3(self, value):
                return value

        directory = db.Volume('.', [Document])['document']

        directory.create({'guid': '1', 'prop1': '1', 'prop2': '1', 'prop3': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document', 0)

        self.assertEqual(
                {'seqno': 1, 'value': 1, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '1', 'mtime': 0},
                directory['1'].meta('prop2'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop3'))

        directory.update('1', {'prop1': '2'})
        self.utime('db/document', 0)

        self.assertEqual(
                {'seqno': 2, 'value': 2, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 2, 'value': '2', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '1', 'mtime': 0},
                directory['1'].meta('prop2'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop3'))

        directory.update('1', {'prop2': '3'})
        self.utime('db/document', 0)

        self.assertEqual(
                {'seqno': 2, 'value': 2, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 2, 'value': '2', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '3', 'mtime': 0},
                directory['1'].meta('prop2'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop3'))

        directory.update('1', {'prop1': '4', 'prop2': '4', 'prop3': '4'})
        self.utime('db/document', 0)

        self.assertEqual(
                {'seqno': 3, 'value': 3, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 3, 'value': '4', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '4', 'mtime': 0},
                directory['1'].meta('prop2'))
        self.assertEqual(
                {'seqno': 3, 'value': '4', 'mtime': 0},
                directory['1'].meta('prop3'))

    def test_DoNotShiftSeqnoForLocalProps(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property(acl=ACL.PUBLIC | ACL.LOCAL)
            def prop2(self, value):
                return value

        directory = db.Volume('.', [Document])['document']

        directory.create({'guid': '1', 'prop1': '1', 'prop2': '1', 'ctime': 1, 'mtime': 1})
        self.utime('db/document', 0)
        self.assertEqual(
                {'seqno': 1, 'value': 1, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '1', 'mtime': 0},
                directory['1'].meta('prop2'))

        directory.update('1', {'prop2': '2'})
        self.utime('db/document', 0)
        self.assertEqual(
                {'seqno': 1, 'value': 1, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 1, 'value': '1', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '2', 'mtime': 0},
                directory['1'].meta('prop2'))

        directory.update('1', {'prop1': '2'})
        self.utime('db/document', 0)
        self.assertEqual(
                {'seqno': 2, 'value': 2, 'mtime': 0},
                directory['1'].meta('seqno'))
        self.assertEqual(
                {'seqno': 2, 'value': '2', 'mtime': 0},
                directory['1'].meta('prop1'))
        self.assertEqual(
                {'value': '2', 'mtime': 0},
                directory['1'].meta('prop2'))


class _SessionSeqno(object):

    def __init__(self):
        self._value = 0

    @property
    def value(self):
        return self._value

    def next(self):
        self._value += 1
        return self._value

    def commit(self):
        pass


if __name__ == '__main__':
    tests.main()
