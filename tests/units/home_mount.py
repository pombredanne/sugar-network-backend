#!/usr/bin/env python
# sugar-lint: disable

import os
import socket
from os.path import exists

from __init__ import tests

from active_toolkit import sockets, coroutine
from sugar_network import Client


class HomeMountTest(tests.Test):

    def test_create(self):
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

    def test_update(self):
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

    def test_get(self):
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

    def test_find(self):
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

    def test_upload_blob(self):
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

    def test_GetAbsetnBLOB(self):
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

    def test_Subscription(self):
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
                {'document': 'context', 'event': 'create', 'guid': guid, 'seqno': 1},
                event)
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'commit', 'seqno': 1},
                subscription.read_message())

        client.Context(guid, title='new-title').post()

        coroutine.select([subscription.fileno()], [], [])
        event = subscription.read_message()
        event.pop('props')
        self.assertEqual(
                {'mountpoint': '~', 'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 2},
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

    def test_Subscription_NotifyOnline(self):
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
                {'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 2},
                event)

    def test_Connect(self):
        self.start_server()
        client = Client('~')

        self.assertEqual(True, client.connected)

    def test_Localize(self):
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


if __name__ == '__main__':
    tests.main()
