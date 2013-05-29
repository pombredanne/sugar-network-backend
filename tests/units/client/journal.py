#!/usr/bin/env python
# sugar-lint: disable

import os
import time
from cStringIO import StringIO

import dbus
import gobject

from __init__ import tests

from sugar_network import db
from sugar_network.client import journal, ipc_port


class JournalTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        journal._ds_root = '.sugar/default/datastore'

        self.ds_pid = self.fork(os.execvp, 'datastore-service', ['datastore-service'])
        time.sleep(1)

        self.ds = dbus.Interface(
                dbus.SessionBus().get_object(
                    'org.laptop.sugar.DataStore',
                    '/org/laptop/sugar/DataStore'),
                'org.laptop.sugar.DataStore')

    def tearDown(self):
        self.waitpid(self.ds_pid, ignore_status=True)
        tests.Test.tearDown(self)

    def test_Exists(self):
        guid = self.ds.create({
            'activity': 'activity',
            'activity_id': 'activity_id',
            'creation_time': '-1',
            'description': 'description',
            'keep': '1',
            'mime_type': 'mime_type',
            'mtime': '-1',
            'tags': 'tags',
            'timestamp': '-1',
            'title': 'title',
            'filesize': '-1',
            },
            '', False, timeout=3)

        assert not journal.exists('fake')
        assert journal.exists(guid)

    def test_Get(self):
        self.touch(('data', 'data'))

        guid = self.ds.create({
            'activity': 'activity',
            'activity_id': 'activity_id',
            'creation_time': '-1',
            'description': 'description',
            'keep': '1',
            'mime_type': 'mime_type',
            'mtime': '-1',
            'tags': 'tags',
            'timestamp': '-1',
            'title': 'title',
            'filesize': '-1',
            },
            'data', True)

        self.assertEqual(None, journal.get(guid, 'fake'))
        self.assertEqual('title', journal.get(guid, 'title'))
        self.assertEqual('description', journal.get(guid, 'description'))
        self.assertEqual('data', file(self.ds.get_filename(guid)).read())

    def test_Update(self):
        ds = journal.Commands()
        self.touch(('preview', 'preview1'))
        ds.journal_update('guid', StringIO('data1'), title='title1', description='description1', preview={'blob': 'preview'})

        assert journal.exists('guid')
        self.assertEqual('title1', journal.get('guid', 'title'))
        self.assertEqual('description1', journal.get('guid', 'description'))
        self.assertEqual('preview1', journal.get('guid', 'preview'))
        self.assertEqual('data1', file(self.ds.get_filename('guid')).read())

        self.touch(('data', 'data2'))
        ds.journal_update('guid', {'blob': 'data'}, title='title2', description='description2', preview=StringIO('preview2'))
        assert journal.exists('guid')
        self.assertEqual('title2', journal.get('guid', 'title'))
        self.assertEqual('description2', journal.get('guid', 'description'))
        self.assertEqual('preview2', journal.get('guid', 'preview'))
        self.assertEqual('data2', file(self.ds.get_filename('guid')).read())

    def test_FindRequest(self):
        url = 'http://127.0.0.1:%s/journal/' % ipc_port.value

        ds = journal.Commands()
        ds.journal_update('guid1', StringIO('data1'), title='title1', description='description1', preview=StringIO('preview1'))
        ds.journal_update('guid2', StringIO('data2'), title='title2', description='description2', preview=StringIO('preview2'))
        ds.journal_update('guid3', StringIO('data3'), title='title3', description='description3', preview=StringIO('preview3'))

        request = db.Request()
        request.path = ['journal']
        response = db.Response()
        self.assertEqual([
            {'guid': 'guid1', 'title': 'title1', 'description': 'description1', 'preview': url + 'guid1/preview'},
            {'guid': 'guid2', 'title': 'title2', 'description': 'description2', 'preview': url + 'guid2/preview'},
            {'guid': 'guid3', 'title': 'title3', 'description': 'description3', 'preview': url + 'guid3/preview'},
            ],
            ds.journal(request, response)['result'])
        self.assertEqual('application/json', response.content_type)

        request = db.Request(offset=1, limit=1)
        request.path = ['journal']
        self.assertEqual([
            {'guid': 'guid2', 'title': 'title2', 'description': 'description2', 'preview': url + 'guid2/preview'},
            ],
            ds.journal(request, response)['result'])

        request = db.Request(query='title3')
        request.path = ['journal']
        self.assertEqual([
            {'guid': 'guid3', 'title': 'title3', 'description': 'description3', 'preview': url + 'guid3/preview'},
            ],
            ds.journal(request, response)['result'])

        request = db.Request(order_by='+title')
        request.path = ['journal']
        self.assertEqual([
            {'guid': 'guid3', 'title': 'title3', 'description': 'description3', 'preview': url + 'guid3/preview'},
            {'guid': 'guid2', 'title': 'title2', 'description': 'description2', 'preview': url + 'guid2/preview'},
            {'guid': 'guid1', 'title': 'title1', 'description': 'description1', 'preview': url + 'guid1/preview'},
            ],
            ds.journal(request, response)['result'])

    def test_GetRequest(self):
        url = 'http://127.0.0.1:%s/journal/' % ipc_port.value

        ds = journal.Commands()
        ds.journal_update('guid1', StringIO('data1'), title='title1', description='description1', preview=StringIO('preview1'))

        request = db.Request()
        request.path = ['journal', 'guid1']
        response = db.Response()
        self.assertEqual(
            {'guid': 'guid1', 'title': 'title1', 'description': 'description1', 'preview': url + 'guid1/preview'},
            ds.journal(request, response))
        self.assertEqual('application/json', response.content_type)

    def test_GetPropRequest(self):
        ds = journal.Commands()
        ds.journal_update('guid1', StringIO('data1'), title='title1', description='description1', preview=StringIO('preview1'))

        request = db.Request()
        request.path = ['journal', 'guid1', 'title']
        response = db.Response()
        self.assertEqual('title1', ds.journal(request, response))
        self.assertEqual('application/json', response.content_type)

        request = db.Request()
        request.path = ['journal', 'guid1', 'preview']
        response = db.Response()
        self.assertEqual({
            'mime_type': 'image/png',
            'blob': '.sugar/default/datastore/gu/guid1/metadata/preview',
            }, ds.journal(request, response))
        self.assertEqual(None, response.content_type)


if __name__ == '__main__':
    tests.main()
