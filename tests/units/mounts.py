#!/usr/bin/env python
# sugar-lint: disable

import os
import cgi
import time
import json
import socket
import hashlib
import subprocess
from email.message import Message
from cStringIO import StringIO
from os.path import join, exists

from __init__ import tests

import restful_document as rd
import active_document as ad
from active_toolkit import sockets, coroutine
from sugar_network import Client
from sugar_network.client.bus import ServerError, Request
from sugar_network import local
from sugar_network.local import mounts as mounts_, activities_registry
from sugar_network.local.mounts import Mounts
from sugar_network.local.bus import IPCServer
from sugar_network.toolkit import sugar, http
from sugar_network.resources.user import User
from sugar_network.resources.context import Context


class MountsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        rd.only_sync_notification.value = False
        mounts_._RECONNECTION_TIMEOUT = 1

    def test_OfflineMount_create(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        self.assertNotEqual(None, guid)

        res = local.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])
        self.assertEqual(False, res['keep'])
        self.assertEqual(0, res['keep_impl'])
        self.assertEqual([-1, -1], res['position'])

    def test_OfflineMount_update(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        context = local.Context(guid)
        context['title'] = 'title_2'
        context['keep'] = True
        context['position'] = (2, 3)
        context.post()

        context = local.Context(guid, ['title', 'keep', 'position'])
        self.assertEqual('title_2', context['title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual([2, 3], context['position'])

    def test_OfflineMount_get(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        context = local.Context(guid, ['title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, context['guid'])
        self.assertEqual('title', context['title'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(0, context['keep_impl'])
        self.assertEqual([-1, -1], context['position'])

    def test_OfflineMount_find(self):
        self.start_server()
        local = Client('~')

        guid_1 = local.Context(
                type='activity',
                title='title_1',
                summary='summary',
                description='description').post()
        guid_2 = local.Context(
                type='activity',
                title='title_2',
                summary='summary',
                description='description').post()
        guid_3 = local.Context(
                type='activity',
                title='title_3',
                summary='summary',
                description='description').post()

        cursor = local.Context.cursor(reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(3, cursor.total)
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', False, 0, [-1, -1]),
                    (guid_2, 'title_2', False, 0, [-1, -1]),
                    (guid_3, 'title_3', False, 0, [-1, -1]),
                    ]),
                sorted([(i['guid'], i['title'], i['keep'], i['keep_impl'], i['position']) for i in cursor]))

    def test_OfflineMount_upload_blob(self):
        self.start_server()
        local = Client('~')

        guid = local.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.touch(('file', 'blob'))
        local.Context(guid).upload_blob('preview', 'file')
        self.assertEqual('blob', local.Context(guid).get_blob('preview').read())

        self.touch(('file2', 'blob2'))
        local.Context(guid).upload_blob('preview', 'file2', pass_ownership=True)
        self.assertEqual('blob2', local.Context(guid).get_blob('preview').read())
        assert not exists('file2')

    def test_OfflineMount_GetAbsetnBLOB(self):
        self.start_server()
        client = Client('~')

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        path, mime_type = client.Context(guid).get_blob_path('icon')
        self.assertEqual(None, path)
        self.assertEqual(True, client.Context(guid).get_blob('icon').closed)

    def test_OnlineMount_GetKeep(self):
        self.start_ipc_and_restful_server()

        remote = Client('/')

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

        self.mounts.home_volume['context'].create_with_guid(guid, {
            'type': 'activity',
            'title': {'en': 'local'},
            'summary': {'en': 'summary'},
            'description': {'en': 'description'},
            'keep': True,
            'keep_impl': 2,
            'user': [sugar.uid()],
            })

        context = remote.Context(guid, ['keep', 'keep_impl'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(2, context['keep_impl'])
        self.assertEqual(
                [(guid, True, 2)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in remote.Context.cursor(reply=['keep', 'keep_impl'])])

    def test_OnlineMount_SetKeep(self):
        self.start_ipc_and_restful_server()

        remote = Client('/')
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

        self.assertRaises(ServerError, lambda: local.Context(guid_1, reply=['title'])['title'])
        self.assertRaises(ServerError, lambda: local.Context(guid_2, reply=['title'])['title'])

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

    def test_OfflineSubscription(self):
        self.start_server()
        client = Client('~')

        subscription = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        coroutine.sleep()

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'create', 'guid': guid},
                event)
        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit', 'seqno': 1},
                subscription.read_message())

        client.Context(guid, title='new-title').post()

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid},
                event)
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit', 'seqno': 2},
                subscription.read_message())

        client.Context.delete(guid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit', 'seqno': 2},
                subscription.read_message())

    def test_OnlineSubscription(self):
        self.fork(self.restful_server)
        coroutine.sleep(1)

        self.start_server()
        client = Client('/')

        subscription = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        coroutine.sleep(1)

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect', 'document': '*'},
                subscription.read_message())
        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'create', 'guid': guid},
                event)

        client.Context(guid, title='new-title').post()

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                event)

        client.Context.delete(guid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'delete', 'guid': guid},
                subscription.read_message())

    def test_OfflineSubscription_NotifyOnline(self):
        self.start_ipc_and_restful_server()

        local = Client('~')
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title={'en': 'title'},
                summary={'en': 'summary'},
                description={'en': 'description'},
                keep=True).post()

        subscription = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')
        coroutine.sleep(1)

        local.Context(guid, keep=False).post()
        coroutine.sleep(1)

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid},
                event)
        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                subscription.read_message())

    def test_OfflineConnect(self):
        self.start_server()
        client = Client('~')

        self.assertEqual(True, client.connected)

    def test_OfflineMount_Localize(self):
        os.environ['LANG'] = 'en_US'
        self.start_server()
        client = Client('~')

        guid = client.Context(
                type='activity',
                title='title_en',
                summary='summary_en',
                description='description_en').post()

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'ru_RU'
        self.start_server()
        client = Client('~')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        res['title'] = 'title_ru'
        res['summary'] = 'summary_ru'
        res['description'] = 'description_ru'
        res.post()

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_ru', res['title'])
        self.assertEqual('summary_ru', res['summary'])
        self.assertEqual('description_ru', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'es_ES'
        self.start_server()
        client = Client('~')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'ru_RU'
        self.start_server()
        client = Client('~')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_ru', res['title'])
        self.assertEqual('summary_ru', res['summary'])
        self.assertEqual('description_ru', res['description'])

    def test_OnlineConnect(self):
        pid = self.fork(self.restful_server)
        volume = ad.SingleVolume('local', [User, Context])
        mounts = Mounts(volume)
        ipc_server = IPCServer(mounts)
        mounts.connect(ipc_server.publish)
        coroutine.spawn(ipc_server.serve_forever)

        client = Client('/')
        subscription = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')

        mounts.open()
        coroutine.dispatch()

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect', 'document': '*'},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'disconnect', 'document': '*'},
                subscription.read_message())
        self.assertEqual(False, client.connected)

        pid = self.fork(self.restful_server)
        coroutine.sleep(1)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'connect', 'document': '*'},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'disconnect', 'document': '*'},
                subscription.read_message())
        self.assertEqual(False, client.connected)

    def test_OnlineMount_upload_blob(self):
        self.start_ipc_and_restful_server()
        remote = Client('/')

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

    def test_OnlineMount_StaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.touch(('file', 'blob-1'))
        remote.Context(guid).upload_blob('preview', 'file')
        self.assertEqual('blob-1', remote.Context(guid).get_blob('preview').read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.touch((cache_path, 'blob-2'))
        self.assertEqual('blob-2', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(2, json.load(file(cache_path + '.meta'))['seqno'])

        self.touch(('file', 'blob-3'))
        remote.Context(guid).upload_blob('preview', 'file')
        self.assertEqual('blob-3', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(3, json.load(file(cache_path + '.meta'))['seqno'])

    def test_OnlineMount_DoNotStaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        self.touch(('file', 'blob'))
        remote.Context(guid).upload_blob('preview', 'file')
        self.assertEqual('blob', remote.Context(guid).get_blob('preview').read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.assertEqual(2, json.load(file(cache_path + '.meta'))['seqno'])

        # Shift seqno
        remote.Context(guid, title='title-1').post()
        remote.Context(guid, title='title-2').post()
        remote.Context(guid, title='title-3').post()
        coroutine.sleep(1)

        remote.close()
        self.stop_servers()
        self.start_ipc_and_restful_server()
        remote = Client('/')

        self.assertEqual('blob', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(5, json.load(file(cache_path + '.meta'))['seqno'])

    def test_OnlineMount_GetAbsetnBLOB(self):
        self.start_ipc_and_restful_server()
        client = Client('/')

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        path, mime_type = client.Context(guid).get_blob_path('icon')
        self.assertEqual(None, path)
        self.assertEqual(True, client.Context(guid).get_blob('icon').closed)

    def test_OnlineMount_Localize(self):
        os.environ['LANG'] = 'en_US'
        self.start_ipc_and_restful_server()
        client = Client('/')

        guid = client.Context(
                type='activity',
                title='title_en',
                summary='summary_en',
                description='description_en').post()

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'ru_RU'
        self.start_ipc_and_restful_server()
        client = Client('/')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        res['title'] = 'title_ru'
        res['summary'] = 'summary_ru'
        res['description'] = 'description_ru'
        res.post()

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_ru', res['title'])
        self.assertEqual('summary_ru', res['summary'])
        self.assertEqual('description_ru', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'es_ES'
        self.start_ipc_and_restful_server()
        client = Client('/')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_en', res['title'])
        self.assertEqual('summary_en', res['summary'])
        self.assertEqual('description_en', res['description'])

        self.stop_servers()
        os.environ['LANG'] = 'ru_RU'
        self.start_ipc_and_restful_server()
        client = Client('/')

        res = client.Context(guid, ['title', 'summary', 'description'])
        self.assertEqual('title_ru', res['title'])
        self.assertEqual('summary_ru', res['summary'])
        self.assertEqual('description_ru', res['description'])

    def test_ServerMode(self):
        local.api_url.value = 'http://localhost:8881'
        volume = ad.SingleVolume('local', [Context, User])
        mounts = Mounts(volume)

        http_server = coroutine.WSGIServer(
                ('localhost', 8881), rd.Router(mounts['~']))
        coroutine.spawn(http_server.serve_forever)
        http_subscriber = rd.SubscribeSocket(mounts.home_volume, 'localhost', 8882)
        coroutine.spawn(http_subscriber.serve_forever)

        monitor = coroutine.spawn(activities_registry.monitor,
                mounts.home_volume, ['Activities'])

        ipc_server = IPCServer(mounts)
        mounts.connect(ipc_server.publish)
        coroutine.spawn(ipc_server.serve_forever)
        coroutine.dispatch()

        def wait_connect(event):
            if event['event'] == 'connect':
                connected.set()

        connected = coroutine.Event()
        Request('/').connect(wait_connect)
        mounts.open()
        connected.wait()

        #coroutine.sleep(1)
        local_client = Client('~')
        remote = Client('/')

        guid = local_client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        self.assertEqual(
                'title',
                remote.Context(guid, reply=['title'])['title'])
        self.assertEqual(
                {'en-US': 'title'},
                http.request('GET', ['context', guid])['title'])

        self.touch(('Activities/activity/activity/activity.info', [
            '[Activity]',
            'name = HelloWorld',
            'activity_version = 1',
            'bundle_id = %s' % guid,
            'exec = true',
            'icon = icon',
            'license = GPLv2+',
            ]))
        coroutine.sleep(1)

        self.assertEqual(2, local_client.Context(guid, reply=['keep_impl'])['keep_impl'])
        self.assertEqual(2, remote.Context(guid, reply=['keep_impl'])['keep_impl'])

        feed = {
                '1': {
                    '*-*': {
                        'guid': tests.tmpdir + '/Activities/activity',
                        'stability': 'stable',
                        'commands': {
                            'activity': {
                                'exec': 'true',
                                },
                            },
                        },
                    },
                }
        self.assertEqual(
                feed,
                json.loads(local_client.Context(guid).get_blob('feed').read()))
        impl_id = feed['1']['*-*']['guid'] = \
                hashlib.sha1(feed['1']['*-*']['guid']).hexdigest()
        self.assertEqual(
                feed,
                json.loads(remote.Context(guid).get_blob('feed').read()))

        self.touch('Activities/activity/1/2/3',
                   'Activities/activity/4/5',
                   'Activities/activity/6')

        mime_type = http.download(['implementation', impl_id, 'bundle'], './downloaded_blob', False)
        content_type, params = cgi.parse_header(mime_type)
        self.assertEqual('multipart/mixed', content_type)
        subprocess.check_call('diff -r downloaded_blob Activities/activity', shell=True)


if __name__ == '__main__':
    tests.main()
