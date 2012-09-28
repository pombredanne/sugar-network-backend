#!/usr/bin/env python
# sugar-lint: disable

import os
from os.path import exists, join

from __init__ import tests

import active_document as ad
from sugar_network import local
from sugar_network.toolkit import sugar
from sugar_network.local import datastore
from sugar_network.resources.artifact import Artifact
from active_toolkit import coroutine


class DatastoreTest(tests.Test):

    def test_populate_NothingToImport(self):
        self.create_mountset([Artifact])
        artifacts = self.mounts.volume['artifact']
        ds_create('1', uid='1')

        datastore.populate(artifacts)
        assert not exists(sn_stamp())
        self.assertEqual(0, artifacts.find()[1])

        self.touch(sn_stamp())
        datastore.populate(artifacts)
        assert not exists(sn_stamp())
        self.assertEqual(0, artifacts.find()[1])

        self.touch(sn_stamp())
        os.utime(sn_stamp(), (0, 0))
        self.touch(ds_stamp())
        os.utime(ds_stamp(), (0, 0))
        datastore.populate(artifacts)
        assert exists(sn_stamp())
        assert exists(ds_stamp())
        self.assertEqual(0, artifacts.find()[1])

    def test_populate(self):
        self.create_mountset([Artifact])
        artifacts = self.mounts.volume['artifact']

        ds_create('1',
                data='data-1',
                activity='activity-1',
                activity_id='activity_id-1',
                creation_time='1',
                description='description-1',
                keep='0',
                mime_type='mime_type-1',
                mtime='fake',
                tags='tag1 tag2 tag3',
                timestamp='11',
                title='title-1',
                filesize='1',
                preview='preview-1',
                title_set_by_user='1',
                prop='value-1',
                )
        ds_create('2',
                data='data-2',
                activity='activity-2',
                activity_id='activity_id-2',
                creation_time='3',
                description='description-2',
                keep='1',
                mime_type='mime_type-2',
                mtime='fake',
                tags='tag4 tag5',
                timestamp='4',
                title='title-2',
                filesize='2',
                preview='preview-2',
                title_set_by_user='2',
                prop='value-2',
                )

        self.touch(ds_stamp())
        datastore.populate(artifacts)
        assert exists(sn_stamp())
        assert os.stat(ds_stamp()).st_mtime == os.stat(sn_stamp()).st_mtime
        self.assertEqual(
                sorted(['1', '2']),
                sorted([i.guid for i in artifacts.find()[0]]))

        self.assertEqual({
            'guid': '1',
            'context': 'activity-1',
            'activity_id': 'activity_id-1',
            'ctime': 1,
            'description': {'en': 'description-1'},
            'keep': False,
            'mime_type': 'mime_type-1',
            'tags': ['tag1', 'tag2', 'tag3'],
            'timestamp': 11,
            'mtime': 0,
            'title': {'en': 'title-1'},
            'filesize': 1,
            'traits': {'title_set_by_user': '1', 'prop': 'value-1'},
            'layer': ['public'],
            'user': [],
            'author': [],
            },
            artifacts.get('1').properties(['guid', 'context', 'activity_id', 'ctime', 'description', 'keep', 'mime_type', 'tags', 'timestamp', 'mtime', 'title', 'filesize', 'traits', 'layer', 'user', 'author']))
        self.assertEqual(
                'preview-1',
                file(artifacts.get('1').meta('preview')['path']).read())
        self.assertEqual(
                'data-1',
                file(artifacts.get('1').meta('data')['path']).read())

        self.assertEqual({
            'guid': '2',
            'context': 'activity-2',
            'activity_id': 'activity_id-2',
            'ctime': 3,
            'description': {'en': 'description-2'},
            'keep': True,
            'mime_type': 'mime_type-2',
            'tags': ['tag4', 'tag5'],
            'timestamp': 4,
            'mtime': 0,
            'title': {'en': 'title-2'},
            'filesize': 2,
            'traits': {'title_set_by_user': '2', 'prop': 'value-2'},
            'layer': ['public'],
            'user': [],
            'author': [],
            },
            artifacts.get('2').properties(['guid', 'context', 'activity_id', 'ctime', 'description', 'keep', 'mime_type', 'tags', 'timestamp', 'mtime', 'title', 'filesize', 'traits', 'layer', 'user', 'author']))
        self.assertEqual(
                'preview-2',
                file(artifacts.get('2').meta('preview')['path']).read())
        self.assertEqual(
                'data-2',
                file(artifacts.get('2').meta('data')['path']).read())


def sn_stamp():
    return local.path('datastore.index_updated')


def ds_stamp():
    return sugar.profile_path('datastore', 'index_updated')


def ds_create(guid, data=None, **props):
    root = sugar.profile_path('datastore', guid[:2], guid, 'metadata', '')

    for key, value in props.items():
        with file(join(root, key), 'wb') as f:
            f.write(value)

    if data is not None:
        with file(join(root, '..', 'data'), 'w') as f:
            f.write(data)


if __name__ == '__main__':
    tests.main()
