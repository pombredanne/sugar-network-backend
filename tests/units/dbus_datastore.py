#!/usr/bin/env python
# sugar-lint: disable

import os
import sys
import time
from os.path import exists, abspath, dirname

arg0 = abspath(__file__)

import dbus
import gobject
from dbus.mainloop.glib import threads_init, DBusGMainLoop

from __init__ import tests

import active_document as ad
from sugar_network.resources.volume import Volume
from sugar_network.resources.artifact import Artifact
from sugar_network.local.mounts import HomeMount
from sugar_network.local.dbus_datastore import Datastore
from sugar_network.toolkit import dbus_thread
from active_toolkit import coroutine


gobject.threads_init()
threads_init()
DBusGMainLoop(set_as_default=True)


class DbusDatastoreTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)

        if fork_num:
            return

        self.fork(os.execvp, arg0, [arg0, self.id().split('.')[-1], 'fork'])
        self.ds = dbus.Interface(
                dbus.SessionBus().get_object(
                    'org.laptop.sugar.DataStore',
                    '/org/laptop/sugar/DataStore'),
                'org.laptop.sugar.DataStore')

    def test_create_Empty(self):
        guid = self.ds.create({}, '', False, timeout=3)

        self.assertEqual({
            'uid': guid,
            'activity': '',
            'keep': '0',
            'mime_type': '',
            'title': '',
            'description': '',
            'activity_id': '',
            'filesize': '0',
            'creation_time': '1',
            'timestamp': '1',
            'mtime': '1970-01-01T00:00:01',
            'tags': '',
            },
            self.ds.get_properties(guid, timeout=3))

    def test_create_SNProps(self):
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

        self.assertEqual({
            'uid': guid,
            'activity': 'activity',
            'keep': '1',
            'mime_type': 'mime_type',
            'title': 'title',
            'description': 'description',
            'activity_id': 'activity_id',
            'filesize': '0',
            'creation_time': '1',
            'timestamp': '1',
            'mtime': '1970-01-01T00:00:01',
            'tags': 'tags',
            },
            self.ds.get_properties(guid, timeout=3))

    def test_create_ExtraProps(self):
        guid = self.ds.create({
            'share-scope': 'share-scope',
            'title_set_by_user': '1',
            'arbitrary_prop': 'arbitrary_value',
            },
            '', False, timeout=3)

        self.assertEqual({
            'uid': guid,
            'activity': '',
            'keep': '0',
            'mime_type': '',
            'title': '',
            'description': '',
            'activity_id': '',
            'filesize': '0',
            'creation_time': '1',
            'timestamp': '1',
            'mtime': '1970-01-01T00:00:01',
            'tags': '',
            'share-scope': 'share-scope',
            'title_set_by_user': '1',
            'arbitrary_prop': 'arbitrary_value',
            },
            self.ds.get_properties(guid, timeout=3))

    def test_update_SNProps(self):
        guid = self.ds.create({}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual('', props['activity'])
        self.assertEqual('0', props['keep'])

        self.ds.update(guid, {'activity': 'activity-1', 'keep': '1'}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual('activity-1', props['activity'])
        self.assertEqual('1', props['keep'])

        self.ds.update(guid, {'activity': 'activity-2', 'keep': '0'}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual('activity-2', props['activity'])
        self.assertEqual('0', props['keep'])

    def test_update_ExtraProps(self):
        guid = self.ds.create({}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual(None, props.get('prop_1'))
        self.assertEqual(None, props.get('prop_2'))

        self.ds.update(guid, {'prop_1': 'value_1', 'prop_2': 'value_2'}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual('value_1', props.get('prop_1'))
        self.assertEqual('value_2', props.get('prop_2'))

        self.ds.update(guid, {'prop_1': 'value_3', 'prop_2': 'value_4'}, '', False, timeout=3)

        props = self.ds.get_properties(guid, timeout=3)
        self.assertEqual('value_3', props.get('prop_1'))
        self.assertEqual('value_4', props.get('prop_2'))

    def test_find(self):
        entries, total = self.ds.find({}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(0, total)
        self.assertEqual(
                sorted([
                    ]),
                entries)

        guid_1 = self.ds.create({'title': 'title-1', 'term': 'value-1'}, '', False, timeout=3)
        guid_2 = self.ds.create({'title': 'title-2', 'term': 'value-2'}, '', False, timeout=3)
        guid_3 = self.ds.create({'title': 'title-3', 'term': 'value-3'}, '', False, timeout=3)

        # All entries
        entries, total = self.ds.find({}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_1, 'title': 'title-1', 'term': 'value-1'},
                    {'uid': guid_2, 'title': 'title-2', 'term': 'value-2'},
                    {'uid': guid_3, 'title': 'title-3', 'term': 'value-3'},
                    ]),
                sorted(entries))

        # offset/limit
        entries, total = self.ds.find({'offset': 0, 'limit':2}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_1, 'title': 'title-1', 'term': 'value-1'},
                    {'uid': guid_2, 'title': 'title-2', 'term': 'value-2'},
                    ]),
                sorted(entries))
        entries, total = self.ds.find({'offset': 2, 'limit':2}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_3, 'title': 'title-3', 'term': 'value-3'},
                    ]),
                sorted(entries))

        # fulltext search
        entries, total = self.ds.find({'query': 'title'}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_1, 'title': 'title-1', 'term': 'value-1'},
                    {'uid': guid_2, 'title': 'title-2', 'term': 'value-2'},
                    {'uid': guid_3, 'title': 'title-3', 'term': 'value-3'},
                    ]),
                sorted(entries))
        entries, total = self.ds.find({'query': 'title-2'}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_2, 'title': 'title-2', 'term': 'value-2'},
                    ]),
                sorted(entries))

        # search by properties
        entries, total = self.ds.find({'title': 'title-1'}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_1, 'title': 'title-1', 'term': 'value-1'},
                    ]),
                sorted(entries))

        # search by guid
        entries, total = self.ds.find({'uid': guid_3}, ['uid', 'title', 'term'], timeout=3)
        self.assertEqual(1, total)
        self.assertEqual(
                sorted([
                    {'uid': guid_3, 'title': 'title-3', 'term': 'value-3'},
                    ]),
                sorted(entries))

        # order by mapped property
        entries, total = self.ds.find({'order_by': 'uid'}, ['uid'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                sorted([{'uid': guid_1}, {'uid': guid_2}, {'uid': guid_3}]),
                entries)
        entries, total = self.ds.find({'order_by': '-uid'}, ['uid'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                [i for i in reversed(sorted([{'uid': guid_1}, {'uid': guid_2}, {'uid': guid_3}]))],
                entries)

        # order by not mapped property
        entries, total = self.ds.find({'order_by': '+title'}, ['uid'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                [{'uid': guid_1}, {'uid': guid_2}, {'uid': guid_3}],
                entries)
        entries, total = self.ds.find({'order_by': '-title'}, ['uid'], timeout=3)
        self.assertEqual(3, total)
        self.assertEqual(
                [{'uid': guid_3}, {'uid': guid_2}, {'uid': guid_1}],
                entries)

    def test_get_uniquevaluesfor(self):
        guid_1 = self.ds.create({'title': '1', 'description': '3'}, '', False, timeout=3)
        guid_2 = self.ds.create({'title': '1', 'description': '3'}, '', False, timeout=3)
        guid_3 = self.ds.create({'title': '2', 'description': '4'}, '', False, timeout=3)

        self.assertEqual(
                sorted(['1', '2']),
                sorted(self.ds.get_uniquevaluesfor('title', {})))
        self.assertEqual(
                sorted(['3', '4']),
                sorted(self.ds.get_uniquevaluesfor('description', {})))
        self.assertEqual(
                sorted(['1']),
                sorted(self.ds.get_uniquevaluesfor('title', {'title': '1'})))

    def test_BLOBs(self):
        guid = self.ds.create({'preview': 'preview-1'}, '', False, timeout=3)
        self.assertEqual('preview-1', self.ds.get_properties(guid, timeout=3, byte_arrays=True)['preview'])
        self.ds.update(guid, {'preview': 'preview-2'}, '', False, timeout=3)
        self.assertEqual('preview-2', self.ds.get_properties(guid, timeout=3, byte_arrays=True)['preview'])

    def test_Data(self):
        data_1 = 'data-1'
        self.touch(('file-1', data_1))
        guid_1 = self.ds.create({}, tests.tmpdir + '/file-1', False, timeout=3)
        assert exists(tests.tmpdir + '/file-1')
        self.assertEqual(str(len(data_1)), self.ds.get_properties(guid_1, timeout=3, byte_arrays=True)['filesize'])
        data_path = self.ds.get_filename(guid_1)
        self.assertEqual(tests.tmpdir + '/.sugar/default/data', dirname(data_path))
        self.assertEqual(data_1, file(data_path).read())

        data_2 = 'data-2'
        self.touch(('file-2', data_2))
        self.ds.update(guid_1, {}, tests.tmpdir + '/file-2', False, timeout=3)
        assert exists(tests.tmpdir + '/file-2')
        self.assertEqual(str(len(data_2)), self.ds.get_properties(guid_1, timeout=3, byte_arrays=True)['filesize'])
        data_path = self.ds.get_filename(guid_1)
        self.assertEqual(tests.tmpdir + '/.sugar/default/data', dirname(data_path))
        self.assertEqual(data_2, file(data_path).read())

        self.touch(('file-1', data_1))
        guid_2 = self.ds.create({}, tests.tmpdir + '/file-1', True, timeout=3)
        assert not exists(tests.tmpdir + '/file-1')
        self.assertEqual(data_1, file(self.ds.get_filename(guid_2)).read())

        self.touch(('file-2', data_2))
        self.ds.update(guid_2, {}, tests.tmpdir + '/file-2', True, timeout=3)
        assert not exists(tests.tmpdir + '/file-2')
        self.assertEqual(data_2, file(self.ds.get_filename(guid_2)).read())

    def test_Delete(self):
        guid_1 = self.ds.create({}, '', False, timeout=3)
        guid_2 = self.ds.create({}, '', False, timeout=3)
        guid_3 = self.ds.create({}, '', False, timeout=3)

        self.assertEqual(
                sorted([{'uid': guid_1}, {'uid': guid_2}, {'uid': guid_3}]),
                sorted(self.ds.find({}, ['uid'], timeout=3)[0]))

        self.ds.delete(guid_2)

        self.assertEqual(
                sorted([{'uid': guid_1}, {'uid': guid_3}]),
                sorted(self.ds.find({}, ['uid'], timeout=3)[0]))

        self.ds.delete(guid_3)

        self.assertEqual(
                sorted([{'uid': guid_1}]),
                sorted(self.ds.find({}, ['uid'], timeout=3)[0]))

        self.ds.delete(guid_1)

        self.assertEqual(
                sorted([]),
                sorted(self.ds.find({}, ['uid'], timeout=3)[0]))

    def test_Events(self):
        Created = []
        Updated = []
        Deleted = []

        self.ds.connect_to_signal('Created', Created.append)
        self.ds.connect_to_signal('Updated', Updated.append)
        self.ds.connect_to_signal('Deleted', Deleted.append)

        guid = self.ds.create({}, '', False, timeout=3)
        self.ds.update(guid, {}, '', False, timeout=3)
        self.ds.delete(guid, timeout=3)

        mainloop = gobject.MainLoop()
        gobject.timeout_add(1000, mainloop.quit)
        mainloop.run()

        self.assertEqual([guid], Created)
        self.assertEqual([guid], Updated)
        self.assertEqual([guid], Deleted)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[-1] == 'fork':
        self = DbusDatastoreTest(sys.argv[1])
        self.setUp(fork_num=1)
        self.override(time, 'time', lambda: 1)
        self.create_mountset([Artifact])
        dbus_thread.spawn_service(Datastore)
        dbus_thread.start(self.mounts)
    else:
        tests.main()
