#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import zipfile
from os.path import exists, abspath, join

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.local.mounts import HomeMount
from sugar_network.local.mountset import Mountset
from sugar_network.local.bus import IPCServer
from sugar_network.local import activities
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network import local, Client, sugar
from sugar_network.resources.volume import Volume


class NodeMountTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        local.server_mode.value = True

    def tearDown(self):
        tests.Test.tearDown(self)

    def start_server(self, ipc=False):
        local.mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context])
        mounts = Mountset(volume)
        if ipc:
            self.server = IPCServer(mounts)
            coroutine.spawn(self.server.serve_forever)
            coroutine.dispatch()
        else:
            Client._connection = mounts
        self.got_event = coroutine.Event()

        def events_cb(event):
            if event['event'] in ('mount', 'unmount') and \
                    event['mountpoint'].startswith(local.mounts_root.value):
                self.events.append((event['event'], event['mountpoint']))
                self.got_event.set()

        self.events = []
        Client.connect(events_cb)

        mounts.open()
        mounts.opened.wait()
        # Let `open()` start processing spawned jobs
        coroutine.dispatch()

        return mounts

    def test_GetKeep(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        mounts = self.start_server()
        self.got_event.wait()

        remote = Client(tests.tmpdir + '/mnt')

        guid = remote.Context(
                type='activity',
                title='remote',
                summary='summary',
                description='description').post()

        context = remote.Context(guid, ['keep', 'keep_impl'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(0, context['keep_impl'])
        self.assertEqual(
                [(guid, False, False)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in remote.Context.cursor(reply=['keep', 'keep_impl'])])

        mounts.home_volume['context'].create(guid=guid, type='activity',
                title={'en': 'local'}, summary={'en': 'summary'},
                description={'en': 'description'}, keep=True, keep_impl=2,
                user=[sugar.uid()])

        context = remote.Context(guid, ['keep', 'keep_impl'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(2, context['keep_impl'])
        self.assertEqual(
                [(guid, True, 2)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in remote.Context.cursor(reply=['keep', 'keep_impl'])])

    def test_SetKeep(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        mounts = self.start_server()
        mounts['~'] = HomeMount(mounts.home_volume)
        self.got_event.wait()
        remote = Client(tests.tmpdir + '/mnt')
        local = Client('~')

        guid_1 = remote.Context(
                type=['activity'],
                title='remote',
                summary='summary',
                description='description').post()
        guid_2 = remote.Context(
                type=['activity'],
                title='remote-2',
                summary='summary',
                description='description').post()

        self.assertRaises(ad.NotFound, lambda: local.Context(guid_1, reply=['title'])['title'])
        self.assertRaises(ad.NotFound, lambda: local.Context(guid_2, reply=['title'])['title'])

        remote.Context(guid_1, keep=True).post()

        cursor = local.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'remote', True, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))
        cursor = remote.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'remote', True, 0),
                    (guid_2, 'remote-2', False, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))

        remote.Context(guid_1, keep=False).post()

        cursor = local.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'remote', False, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))
        cursor = remote.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'remote', False, 0),
                    (guid_2, 'remote-2', False, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))

        context = local.Context(guid_1)
        context['title'] = 'local'
        context.post()
        context = local.Context(guid_1, reply=['keep', 'keep_impl', 'title'])
        self.assertEqual('local', context['title'])

        remote.Context(guid_1, keep=True).post()

        cursor = local.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'local', True, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))
        cursor = remote.Context.cursor(reply=['keep', 'keep_impl', 'title'])
        self.assertEqual(
                sorted([
                    (guid_1, 'remote', True, 0),
                    (guid_2, 'remote-2', False, 0),
                    ]),
                sorted([(i.guid, i['title'], i['keep'], i['keep_impl']) for i in cursor]))

    def test_SetKeepImpl(self):
        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.context',
                'sugar_network.resources.implementation',
                ]

        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        mounts = self.start_server(ipc=True)
        mounts['~'] = HomeMount(mounts.home_volume)
        self.got_event.wait()
        remote = Client(tests.tmpdir + '/mnt')
        local = Client('~')
        coroutine.spawn(activities.monitor, mounts.home_volume, ['Activities'])

        context = remote.Context(
                type=['activity'],
                title='remote',
                summary='summary',
                description='description').post()
        impl = remote.Implementation(
                context=context,
                license=['GPLv3+'],
                version='1',
                date=0,
                stability='stable',
                notes='').post()
        with file('mnt/context/%s/%s/feed' % (context[:2], context), 'w') as f:
            json.dump({
                'seqno': 0,
                'mime_type': 'application/octet-stream',
                'digest': 'digest',
                }, f)
        with file('mnt/context/%s/%s/feed.blob' % (context[:2], context), 'w') as f:
            json.dump({
                '1': {
                    '*-*': {
                        'commands': {
                            'activity': {
                                'exec': 'echo',
                                },
                            },
                        'stability': 'stable',
                        'guid': impl,
                        'size': 0,
                        'extract': 'TestActivitry',
                        },
                    },
                }, f)
        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()
        remote.Implementation(impl).upload_blob('data', 'bundle')

        remote.Context(context, keep_impl=1).post()

        cursor = local.Context.cursor(reply=['keep_impl', 'title'])
        self.assertEqual([
            (context, 'remote', 2),
            ],
            [(i.guid, i['title'], i['keep_impl']) for i in cursor])
        assert exists('Activities/TestActivitry/activity/activity.info')

    def test_Events(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        self.start_server()
        self.got_event.wait()
        self.got_event.clear()
        client = Client(tests.tmpdir + '/mnt')

        def events_cb(event):
            if 'props' in event:
                event.pop('props')
            events.append(event)
            got_commit.set()
            got_commit.clear()

        events = []
        got_commit = coroutine.Event()
        Client.connect(events_cb)

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        got_commit.wait()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'create', 'guid': guid, 'seqno': 1},
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'commit', 'seqno': 1},
            ],
            events)
        del events[:]

        client.Context(guid, title='new-title').post()
        got_commit.wait()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 2},
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'commit', 'seqno': 2},
            ],
            events)
        del events[:]

        guid_path = 'mnt/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)
        client.Context.delete(guid)
        assert not exists(guid_path)
        got_commit.wait()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'delete', 'guid': guid},
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'commit', 'seqno': 2},
            ],
            events)
        del events[:]

    def test_upload_blob(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        self.start_server()
        self.got_event.wait()
        remote = Client(tests.tmpdir + '/mnt')

        guid = remote.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.touch(('file', 'blob'))
        remote.Context(guid).upload_blob('preview', 'file')
        self.assertEqual('blob', remote.Context(guid).get_blob('preview').read())

        self.touch(('file2', 'blob2'))
        remote.Context(guid).upload_blob('preview', 'file2', pass_ownership=True)
        self.assertEqual('blob2', remote.Context(guid).get_blob('preview').read())
        assert not exists('file2')

    def test_GetAbsetnBLOB(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        self.start_server()
        self.got_event.wait()
        client = Client(tests.tmpdir + '/mnt')

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        path, mime_type = client.Context(guid).get_blob_path('icon')
        self.assertEqual(None, path)
        self.assertEqual(True, client.Context(guid).get_blob('icon').closed)

    def test_get_blob_ExtractImplementations(self):
        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.implementation',
                ]

        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        self.start_server()
        self.got_event.wait()
        remote = Client(tests.tmpdir + '/mnt')

        guid = remote.Implementation(
                context='context',
                license=['GPLv3+'],
                version='1',
                date=0,
                stability='stable',
                notes='').post()

        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()
        remote.Implementation(guid).upload_blob('data', 'bundle')

        path, __ = remote.Implementation(guid).get_blob_path('data')
        self.assertEqual(abspath('cache/implementation/%s/%s/data' % (guid[:2], guid)), path)
        self.assertEqual('probe', file(join(path, 'probe')).read())


if __name__ == '__main__':
    tests.main()
