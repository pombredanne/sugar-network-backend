#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from active_toolkit import sockets, coroutine
from sugar_network import Client, ServerError
from sugar_network.local.mounts import RemoteMount
from sugar_network.local.mountset import Mountset
from sugar_network.local.bus import IPCServer
from sugar_network.toolkit import sugar, http
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.volume import Volume


class RemoteMountTest(tests.Test):

    def test_GetKeep(self):
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

        self.mounts.home_volume['context'].create(guid=guid, type='activity',
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

    def test_Subscription(self):
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
                {'mountpoint': '/', 'event': 'mount', 'document': '*', 'name': 'Network', 'private': False},
                subscription.read_message())
        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        event.pop('seqno')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'create', 'guid': guid},
                event)

        client.Context(guid, title='new-title').post()

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        event.pop('seqno')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'update', 'guid': guid},
                event)

        client.Context.delete(guid)

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('seqno')
        self.assertEqual(
                {'mountpoint': '/', 'document': 'context', 'event': 'delete', 'guid': guid},
                event)

    def test_Connect(self):
        pid = self.fork(self.restful_server)
        volume = Volume('local', [User, Context])
        mounts = Mountset(volume)
        mounts['/'] = RemoteMount(volume)
        ipc_server = IPCServer(mounts)
        coroutine.spawn(ipc_server.serve_forever)

        client = Client('/')
        subscription = sockets.SocketFile(coroutine.socket(socket.AF_UNIX))
        subscription.connect('run/subscribe')

        mounts.open()
        coroutine.dispatch()

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'mount', 'document': '*', 'name': 'Network', 'private': False},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'unmount', 'document': '*', 'name': 'Network', 'private': False},
                subscription.read_message())
        self.assertEqual(False, client.connected)

        pid = self.fork(self.restful_server)
        coroutine.sleep(1)

        self.assertEqual(False, client.connected)
        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'mount', 'document': '*', 'name': 'Network', 'private': False},
                subscription.read_message())
        self.assertEqual(True, client.connected)

        self.waitpid(pid)

        coroutine.select([subscription.fileno()], [], [])
        self.assertEqual(
                {'mountpoint': '/', 'event': 'unmount', 'document': '*', 'name': 'Network', 'private': False},
                subscription.read_message())
        self.assertEqual(False, client.connected)

    def test_upload_blob(self):
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

    def test_StaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = Client('/')

        guid = remote.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        http.request('PUT', ['context', guid, 'preview'], files={'file': StringIO('blob-1')})
        self.assertEqual('blob-1', remote.Context(guid).get_blob('preview').read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.touch((cache_path, 'blob-2'))
        self.assertEqual('blob-2', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(3, json.load(file(cache_path + '.meta'))['seqno'])

        http.request('PUT', ['context', guid, 'preview'], files={'file': StringIO('blob-3')})
        self.assertEqual('blob-3', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(4, json.load(file(cache_path + '.meta'))['seqno'])

    def test_DoNotStaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = Client('/')

        guid = http.request('POST', ['context'],
                headers={'Content-Type': 'application/json'},
                data={
                    'type': 'activity',
                    'title': 'title',
                    'summary': 'summary',
                    'description': 'description',
                    })

        http.request('PUT', ['context', guid, 'preview'], files={'file': StringIO('blob')})
        self.assertEqual('blob', remote.Context(guid).get_blob('preview').read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.assertEqual(3, json.load(file(cache_path + '.meta'))['seqno'])

        # Shift seqno
        connected = coroutine.Event()
        self.mounts.connect(lambda event: connected.set(), event='create', mountpoint='/')
        http.request('POST', ['context'],
                headers={'Content-Type': 'application/json'},
                data={
                    'type': 'activity',
                    'title': 'title2',
                    'summary': 'summary2',
                    'description': 'description2',
                    })
        connected.wait()

        self.assertEqual('blob', remote.Context(guid).get_blob('preview').read())
        self.assertEqual(4, json.load(file(cache_path + '.meta'))['seqno'])

    def test_GetAbsentBLOB(self):
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

    def test_Localize(self):
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


if __name__ == '__main__':
    tests.main()
