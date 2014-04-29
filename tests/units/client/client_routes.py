#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import json
import time
import hashlib
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, client, toolkit
from sugar_network.client import journal, Connection, IPCConnection, cache_limit, cache_lifetime, api, injector, routes
from sugar_network.client.model import Volume
from sugar_network.client.injector import Injector
from sugar_network.client.routes import ClientRoutes
from sugar_network.client.auth import SugarCreds
from sugar_network.node.model import User
from sugar_network.node.master import MasterRoutes
from sugar_network.toolkit.router import Router, Request, Response, route
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import coroutine, i18n, packets, http

import requests


class ClientRoutesTest(tests.Test):

    def test_Hub(self):
        volume = Volume('db')
        cp = ClientRoutes(volume, SugarCreds(client.keyfile.value))
        server = coroutine.WSGIServer(
                ('127.0.0.1', client.ipc_port.value), Router(cp))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()

        url = 'http://127.0.0.1:%s' % client.ipc_port.value

        response = requests.request('GET', url + '/hub', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual('/hub/', response.headers['Location'])

        client.hub_root.value = '.'
        index_html = '<html><body>index</body></html>'
        self.touch(('index.html', index_html))

        response = requests.request('GET', url + '/hub', allow_redirects=True)
        self.assertEqual(index_html, response.content)

        response = requests.request('GET', url + '/hub/', allow_redirects=False)
        self.assertEqual(index_html, response.content)

    def test_I18nQuery(self):
        os.environ['LANGUAGE'] = 'foo'
        self.start_online_client()
        ipc = IPCConnection()

        ipc.request('POST', [], ''.join(packets.encode([
            ('push', None, [
                {'resource': 'context'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'type': {'value': ['activity'], 'mtime': 1},
                    'summary': {'value': {}, 'mtime': 1},
                    'description': {'value': {}, 'mtime': 1},
                    'title': {'value': {'en-US': 'qwe', 'ru-RU': 'йцу'}, 'mtime': 1},
                    }},
                {'guid': '2', 'patch': {
                    'guid': {'value': '2', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'type': {'value': ['activity'], 'mtime': 1},
                    'summary': {'value': {}, 'mtime': 1},
                    'description': {'value': {}, 'mtime': 1},
                    'title': {'value': {'en-US': 'qwerty', 'ru-RU': 'йцукен'}, 'mtime': 1},
                    }},
                ]),
            ], header={'to': '127.0.0.1:7777', 'from': 'slave'})), params={'cmd': 'push'})

        self.assertEqual(
                sorted(['1', '2']),
                sorted([i['guid'] for i in ipc.get(['context'], query='йцу')['result']]))
        self.assertEqual(
                sorted(['1', '2']),
                sorted([i['guid'] for i in ipc.get(['context'], query='qwe')['result']]))

        self.assertEqual(
                sorted(['2']),
                sorted([i['guid'] for i in ipc.get(['context'], query='йцукен')['result']]))
        self.assertEqual(
                sorted(['2']),
                sorted([i['guid'] for i in ipc.get(['context'], query='qwerty')['result']]))

    def test_LanguagesFallbackInRequests(self):
        self.start_online_client()
        ipc = IPCConnection()

        ipc.request('POST', [], ''.join(packets.encode([
            ('push', None, [
                {'resource': 'context'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'type': {'value': ['activity'], 'mtime': 1},
                    'summary': {'value': {}, 'mtime': 1},
                    'description': {'value': {}, 'mtime': 1},
                    'title': {'value': {'en': '1', 'ru': '2', 'es': '3'}, 'mtime': 1},
                    }},
                {'guid': '2', 'patch': {
                    'guid': {'value': '2', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'type': {'value': ['activity'], 'mtime': 1},
                    'summary': {'value': {}, 'mtime': 1},
                    'description': {'value': {}, 'mtime': 1},
                    'title': {'value': {'en': '1', 'ru': '2'}, 'mtime': 1},
                    }},
                {'guid': '3', 'patch': {
                    'guid': {'value': '3', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    'type': {'value': ['activity'], 'mtime': 1},
                    'summary': {'value': {}, 'mtime': 1},
                    'description': {'value': {}, 'mtime': 1},
                    'title': {'value': {'en': '1'}, 'mtime': 1},
                    }},
                ]),
            ], header={'to': '127.0.0.1:7777', 'from': 'slave'})), params={'cmd': 'push'})

        i18n._default_langs = None
        os.environ['LANGUAGE'] = 'es:ru:en'
        ipc = IPCConnection()
        self.assertEqual('3', ipc.get(['context', '1', 'title']))
        self.assertEqual('2', ipc.get(['context', '2', 'title']))
        self.assertEqual('1', ipc.get(['context', '3', 'title']))

        i18n._default_langs = None
        os.environ['LANGUAGE'] = 'ru:en'
        ipc = IPCConnection()
        self.assertEqual('2', ipc.get(['context', '1', 'title']))
        self.assertEqual('2', ipc.get(['context', '2', 'title']))
        self.assertEqual('1', ipc.get(['context', '3', 'title']))

        i18n._default_langs = None
        os.environ['LANGUAGE'] = 'en'
        ipc = IPCConnection()
        self.assertEqual('1', ipc.get(['context', '1', 'title']))
        self.assertEqual('1', ipc.get(['context', '2', 'title']))
        self.assertEqual('1', ipc.get(['context', '3', 'title']))

        i18n._default_langs = None
        os.environ['LANGUAGE'] = 'foo'
        ipc = IPCConnection()
        self.assertEqual('1', ipc.get(['context', '1', 'title']))
        self.assertEqual('1', ipc.get(['context', '2', 'title']))
        self.assertEqual('1', ipc.get(['context', '3', 'title']))

    def test_whoami(self):
        self.start_offline_client()
        ipc = IPCConnection()

        self.assertEqual(
                {'guid': tests.UID, 'route': 'offline'},
                ipc.get(cmd='whoami'))

        self.fork_master()
        self.wait_for_events(event='inline', state='online').wait()

        self.assertEqual(
                {'guid': tests.UID, 'route': 'proxy'},
                ipc.get(cmd='whoami'))

    def test_Events(self):
        self.override(time, 'time', lambda: 0)
        self.start_offline_client()
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe():
                if event['event'] not in ('commit', 'pong'):
                    events.append(event)
        coroutine.spawn(read_events)
        coroutine.dispatch()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        ipc.delete(['context', guid])
        coroutine.sleep(.1)

        self.assertEqual([
            {'event': 'create', 'guid': guid, 'resource': 'context'},
            {'event': 'update', 'guid': guid, 'resource': 'context', 'props': {'mtime': 0, 'title': {'en-us': 'title_2'}}},
            {'event': 'delete', 'guid': guid, 'resource': 'context'},
            ],
            events)
        del events[:]

        self.fork_master()
        self.wait_for_events(event='inline', state='online').wait()
        coroutine.sleep(.1)

        self.assertEqual([
            {'event': 'inline', 'state': 'connecting'},
            {'event': 'inline', 'state': 'online'},
            ],
            events)
        del events[:]

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.sleep(.1)
        ipc.delete(['context', guid])
        coroutine.sleep(.1)

        self.assertEqual([
            {'event': 'create', 'guid': tests.UID, 'resource': 'user'},
            {'event': 'create', 'guid': guid, 'resource': 'context'},
            {'event': 'update', 'guid': guid, 'resource': 'context', 'props': {'mtime': 0, 'title': {'en-us': 'title_2'}}},
            {'event': 'delete', 'guid': guid, 'resource': 'context'},
            ],
            events)
        del events[:]

    def test_HomeVolumeEventsOnlyInOffline(self):
        home_volume = self.start_offline_client()
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe():
                if event['event'] not in ('commit', 'pong'):
                    events.append(event)
        coroutine.spawn(read_events)
        coroutine.sleep(.1)

        guid = home_volume['context'].create({
            'type': ['activity'],
            'title': {},
            'summary': {},
            'description': {},
            })
        home_volume['context'].update(guid, {
            'title': {'en': 'title_2'},
            })
        home_volume['context'].delete(guid)
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update', 'props': {'title': {'en': 'title_2'}}},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)
        del events[:]

        self.fork_master()
        self.wait_for_events(event='inline', state='online').wait()
        coroutine.sleep(.1)
        del events[:]

        guid = home_volume['context'].create({
            'type': ['activity'],
            'title': {},
            'summary': {},
            'description': {},
            })
        home_volume['context'].update(guid, {
            'title': {'en': 'title_2'},
            })
        coroutine.sleep(.1)
        home_volume['context'].delete(guid)
        coroutine.sleep(.1)

        self.assertEqual([], events)

    def test_BLOBs(self):
        self.start_offline_client()
        ipc = IPCConnection()

        blob = 'blob_value'
        digest = hashlib.sha1(blob).hexdigest()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.request('PUT', ['context', guid, 'logo'], blob, headers={'content-type': 'image/png'})

        self.assertEqual(
                blob,
                ipc.request('GET', ['context', guid, 'logo']).content)
        self.assertEqual({
            'logo': 'http://127.0.0.1:5555/blobs/%s' % digest,
            },
            ipc.get(['context', guid], reply=['logo']))
        self.assertEqual([{
            'logo': 'http://127.0.0.1:5555/blobs/%s' % digest,
            }],
            ipc.get(['context'], reply=['logo'])['result'])

        self.fork_master()
        self.wait_for_events(event='inline', state='online').wait()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.request('PUT', ['context', guid, 'logo'], blob, headers={'content-type': 'image/png'})

        self.assertEqual(
                blob,
                ipc.request('GET', ['context', guid, 'logo']).content)
        self.assertEqual(
                'http://127.0.0.1:7777/blobs/%s' % digest,
                ipc.get(['context', guid], reply=['logo'])['logo'])
        self.assertEqual(
                ['http://127.0.0.1:7777/blobs/%s' % digest],
                [i['logo'] for i in ipc.get(['context'], reply=['logo'])['result']])

    def test_OnlinePins(self):
        home_volume = self.start_online_client()
        ipc = IPCConnection()

        guid1 = ipc.post(['context'], {
            'type': 'activity',
            'title': '1',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.upload(['context'], self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = 2',
            'bundle_id = context2',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]]), cmd='submit', initial=True)
        guid2 = 'context2'
        ipc.upload(['context'], self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = 3',
            'bundle_id = context3',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = stable',
            ]]), cmd='submit', initial=True)
        guid3 = 'context3'
        guid4 = ipc.post(['context'], {
            'type': 'activity',
            'title': '4',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                sorted([(guid1, []), (guid2, []), (guid3, []), (guid4, [])]),
                sorted([(i['guid'], i['pins']) for i in ipc.get(['context'], reply=['pins'])['result']]))
        self.assertEqual([
            ],
            ipc.get(['context'], reply=['guid', 'title'], pins='favorite')['result'])
        self.assertEqual([
            ],
            ipc.get(['context'], reply=['guid', 'title'], pins='checkin')['result'])

        ipc.put(['context', guid1], True, cmd='favorite')
        ipc.put(['context', guid2], True, cmd='favorite')
        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'event': 'checkin', 'state': 'download'},
            {'event': 'checkin', 'state': 'ready'},
            ],
            [i for i in ipc.put(['context', guid2], True, cmd='checkin')])
        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'event': 'checkin', 'state': 'download'},
            {'event': 'checkin', 'state': 'ready'},
            ],
            [i for i in ipc.put(['context', guid3], True, cmd='checkin')])
        home_volume['context'].update(guid1, {'title': {i18n.default_lang(): '1_'}})
        home_volume['context'].update(guid2, {'title': {i18n.default_lang(): '2_'}})
        home_volume['context'].update(guid3, {'title': {i18n.default_lang(): '3_'}})

        self.assertEqual(
                sorted([(guid1, ['favorite']), (guid2, ['checkin', 'favorite']), (guid3, ['checkin']), (guid4, [])]),
                sorted([(i['guid'], i['pins']) for i in ipc.get(['context'], reply=['pins'])['result']]))
        self.assertEqual(
                sorted([
                    {'guid': guid1, 'title': '1_'},
                    {'guid': guid2, 'title': '2_'},
                    ]),
                sorted(ipc.get(['context'], reply=['guid', 'title'], pins='favorite')['result']))

        self.assertEqual(
                sorted([(guid2, '2_'), (guid3, '3_')]),
                sorted([(i['guid'], i['title']) for i in ipc.get(['context'], reply=['guid', 'title'], pins='checkin')['result']]))

        ipc.delete(['context', guid1], cmd='favorite')
        ipc.delete(['context', guid2], cmd='checkin')

        self.assertEqual(
                sorted([(guid1, []), (guid2, ['favorite']), (guid3, ['checkin']), (guid4, [])]),
                sorted([(i['guid'], i['pins']) for i in ipc.get(['context'], reply=['pins'])['result']]))
        self.assertEqual([
            {'guid': guid2, 'pins': ['favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='favorite')['result'])
        self.assertEqual([
            {'guid': guid3, 'pins': ['checkin']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='checkin')['result'])

    def test_OfflinePins(self):
        self.start_online_client()
        ipc = IPCConnection()

        ipc.upload(['context'], self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = 1',
            'bundle_id = 1',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]]), cmd='submit', initial=True)
        return
        ipc.upload(['context'], self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = 2',
            'bundle_id = 2',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ]]), cmd='submit', initial=True)
        ipc.upload(['context'], self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = 3',
            'bundle_id = 3',
            'exec = true',
            'icon = icon',
            'activity_version = 3',
            'license = Public Domain',
            ]]), cmd='submit', initial=True)

        ipc.put(['context', '1'], None, cmd='favorite')
        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'event': 'checkin', 'state': 'download'},
            {'event': 'checkin', 'state': 'ready'},
            ],
            [i for i in ipc.put(['context', '2'], None, cmd='checkin')])
        self.assertEqual(
                sorted([('1', ['favorite']), ('2', ['checkin']), ('3', [])]),
                sorted([(i['guid'], i['pins']) for i in ipc.get(['context'], reply=['guid', 'pins'])['result']]))

        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()

        self.assertEqual([
            {'guid': '1', 'pins': ['favorite']},
            {'guid': '2', 'pins': ['checkin']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'])['result'])
        self.assertEqual([
            {'guid': '1', 'pins': ['favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='favorite')['result'])
        self.assertEqual([
            {'guid': '2', 'pins': ['checkin']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='checkin')['result'])

        ipc.delete(['context', '1'], cmd='favorite')
        ipc.put(['context', '2'], None, cmd='favorite')
        self.assertRaises(http.ServiceUnavailable, ipc.put, ['context', '3'], None, cmd='favorite')

        self.assertEqual([
            {'guid': '1', 'pins': []},
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'])['result'])
        self.assertEqual([
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='favorite')['result'])
        self.assertEqual([
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='checkin')['result'])

        ipc.delete(['context', '2'], cmd='checkin')
        ipc.delete(['context', '2'], cmd='favorite')

        self.assertEqual([
            {'guid': '1', 'pins': []},
            {'guid': '2', 'pins': []},
            ],
            ipc.get(['context'], reply=['guid', 'pins'])['result'])
        self.assertEqual([
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='favorite')['result'])
        self.assertEqual([
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='checkin')['result'])

        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'event': 'failure', 'error': 'Not available in offline', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.put(['context', '1'], None, cmd='checkin')])
        ipc.put(['context', '1'], None, cmd='favorite')
        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'event': 'checkin', 'state': 'ready'},
            ],
            [i for i in ipc.put(['context', '2'], None, cmd='checkin')])
        ipc.put(['context', '2'], None, cmd='favorite')
        self.assertEqual([
            {'event': 'failure', 'error': 'Not available in offline', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.put(['context', '3'], None, cmd='checkin')])
        self.assertRaises(http.ServiceUnavailable, ipc.put, ['context', '3'], None, cmd='favorite')

        self.assertEqual([
            {'guid': '1', 'pins': ['favorite']},
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'])['result'])
        self.assertEqual([
            {'guid': '1', 'pins': ['favorite']},
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='favorite')['result'])
        self.assertEqual([
            {'guid': '2', 'pins': ['checkin', 'favorite']},
            ],
            ipc.get(['context'], reply=['guid', 'pins'], pins='checkin')['result'])

    def test_checkin_Notificaitons(self):
        self.start_online_client()
        ipc = IPCConnection()

        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = context',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        activity_bundle = self.zips(('topdir/activity/activity.info', activity_info))
        release = ipc.upload(['context'], activity_bundle, cmd='submit', initial=True)

        def subscribe():
            for i in ipc.subscribe():
                if i.get('event') != 'commit':
                    events.append(i)
        events = []
        coroutine.spawn(subscribe)
        coroutine.sleep(.1)
        del events[:]

        assert {'event': 'checkin', 'state': 'ready'} in [i for i in ipc.put(['context', 'context'], None, cmd='checkin')]
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['inprogress']}, 'resource': 'context'},
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['checkin']}, 'resource': 'context'},
            ], events)
        del events[:]

        ipc.put(['context', 'context'], None, cmd='favorite')
        ipc.delete(['context', 'context'], cmd='checkin')
        coroutine.sleep(.1)
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['favorite']}, 'resource': 'context'},
            ], events)

        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()
        coroutine.sleep(.1)
        del events[:]

        assert {'event': 'checkin', 'state': 'ready'} in [i for i in ipc.put(['context', 'context'], None, cmd='checkin')]
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['favorite', 'inprogress']}, 'resource': 'context'},
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['checkin', 'favorite']}, 'resource': 'context'},
            ], events)
        del events[:]

        ipc.delete(['context', 'context'], cmd='checkin')
        coroutine.sleep(.1)
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['favorite']}, 'resource': 'context'},
            ], events)

    def test_launch_Notificaitons(self):
        self.start_online_client()
        ipc = IPCConnection()

        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = context',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        activity_bundle = self.zips(('topdir/activity/activity.info', activity_info))
        release = ipc.upload(['context'], activity_bundle, cmd='submit', initial=True)

        def subscribe():
            for i in ipc.subscribe():
                if i.get('event') != 'commit':
                    events.append(i)
        events = []
        coroutine.spawn(subscribe)
        coroutine.sleep(.1)
        del events[:]

        assert {'event': 'launch', 'state': 'exit'} in [i for i in ipc.get(['context', 'context'], cmd='launch')]
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['inprogress']}, 'resource': 'context'},
            {'event': 'update', 'guid': 'context', 'props': {'pins': []}, 'resource': 'context'},
            ], events)

        ipc.put(['context', 'context'], None, cmd='favorite')
        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()
        coroutine.sleep(.1)
        del events[:]

        assert {'event': 'launch', 'state': 'exit'} in [i for i in ipc.get(['context', 'context'], None, cmd='launch')]
        self.assertEqual([
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['favorite', 'inprogress']}, 'resource': 'context'},
            {'event': 'update', 'guid': 'context', 'props': {'pins': ['favorite']}, 'resource': 'context'},
            ], events)
        del events[:]

    def test_checkin_Fails(self):
        self.start_online_client()
        ipc = IPCConnection()

        self.assertEqual([
            {'error': 'Resource not found', 'event': 'failure', 'exception': 'NotFound'},
            ],
            [i for i in ipc.put(['context', 'context'], None, cmd='checkin')])

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'error': 'Failed to solve', 'event': 'failure', 'exception': 'RuntimeError'},
            ],
            [i for i in ipc.put(['context', guid], None, cmd='checkin')])

        ipc.put(['context', guid], None, cmd='favorite')
        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()
        coroutine.sleep(.1)

        self.assertEqual([
            {'error': 'Not available in offline', 'event': 'failure', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.put(['context', 'context'], None, cmd='checkin')])

        self.assertEqual([
            {'event': 'checkin', 'state': 'solve'},
            {'error': 'Not available in offline', 'event': 'failure', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.put(['context', guid], None, cmd='checkin')])

    def test_launch_Fails(self):
        self.override(injector, '_activity_id_new', lambda: 'activity_id')
        self.start_online_client()
        ipc = IPCConnection()

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'error': 'Context not found', 'event': 'failure', 'exception': 'NotFound'},
            ],
            [i for i in ipc.get(['context', 'context'], cmd='launch')])

        guid1 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'error': 'Failed to solve', 'event': 'failure', 'exception': 'RuntimeError'},
            ],
            [i for i in ipc.get(['context', guid1], cmd='launch')])

        activity_info = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = context2',
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        activity_bundle = self.zips(('topdir/activity/activity.info', activity_info))
        release = ipc.upload(['context'], activity_bundle, cmd='submit', initial=True)

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'event': 'launch', 'state': 'download'},
            {'event': 'launch', 'state': 'exec'},
            {'context': 'context2',
                'args': ['false', '-b', 'context2', '-a', 'activity_id'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    tests.tmpdir + '/.sugar/default/logs/context2.log',
                    ],
                'solution': {
                    'context2': {
                        'blob': release,
                        'command': ['activity', 'false'],
                        'content-type': 'application/vnd.olpc-sugar',
                        'size': len(activity_bundle),
                        'title': 'Activity',
                        'unpack_size': len(activity_info),
                        'version': [[1], 0],
                        },
                    },
                },
            {'error': 'Process exited with 1 status', 'event': 'failure', 'exception': 'RuntimeError'},
            ],
            [i for i in ipc.get(['context', 'context2'], cmd='launch')])

        ipc.put(['context', guid1], None, cmd='favorite')
        ipc.put(['context', 'context2'], None, cmd='favorite')
        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()
        coroutine.sleep(.1)

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'error': 'Not available in offline', 'event': 'failure', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.get(['context', 'context'], cmd='launch')])

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'error': 'Not available in offline', 'event': 'failure', 'exception': 'ServiceUnavailable'},
            ],
            [i for i in ipc.get(['context', guid1], cmd='launch')])

        self.assertEqual([
            {'activity_id': 'activity_id'},
            {'event': 'launch', 'state': 'init'},
            {'event': 'launch', 'state': 'solve'},
            {'event': 'launch', 'state': 'exec'},
            {'context': 'context2',
                'args': ['false', '-b', 'context2', '-a', 'activity_id'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    tests.tmpdir + '/.sugar/default/logs/context2_1.log',
                    ],
                'solution': {
                    'context2': {
                        'blob': release,
                        'command': ['activity', 'false'],
                        'content-type': 'application/vnd.olpc-sugar',
                        'size': len(activity_bundle),
                        'title': 'Activity',
                        'unpack_size': len(activity_info),
                        'version': [[1], 0],
                        },
                    },
                },
            {'error': 'Process exited with 1 status', 'event': 'failure', 'exception': 'RuntimeError'},
            ],
            [i for i in ipc.get(['context', 'context2'], cmd='launch')])

    def test_SubmitReport(self):
        home_volume = self.start_online_client()
        ipc = IPCConnection()

        self.touch(
                ['file1', 'content1'],
                ['file2', 'content2'],
                ['file3', 'content3'],
                )
        events = [i for i in ipc.post(['report'], {'context': 'context', 'error': 'error', 'logs': [
            tests.tmpdir + '/file1',
            tests.tmpdir + '/file2',
            tests.tmpdir + '/file3',
            ]}, cmd='submit')]
        self.assertEqual('done', events[-1]['event'])
        guid = events[-1]['guid']

        report = ipc.get(['report', guid], reply=['context', 'error'])
        self.assertEqual('context', report['context'])
        self.assertEqual('error', report['error'])

        self.assertEqual(sorted([
            'content1',
            'content2',
            'content3',
            ]),
            sorted([''.join(ipc.download(i[1])) for i in ipc.get(['report', guid, 'logs'])]))
        assert not home_volume['report'][guid].exists

        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()

        events = [i for i in ipc.post(['report'], {'context': 'context', 'error': 'error', 'logs': [
            tests.tmpdir + '/file1',
            tests.tmpdir + '/file2',
            tests.tmpdir + '/file3',
            ]}, cmd='submit')]
        self.assertEqual('done', events[-1]['event'])
        guid = events[-1]['guid']

        self.assertEqual({
            'context': 'context',
            'error': 'error',
            },
            ipc.get(['report', guid], reply=['context', 'error']))
        self.assertEqual(sorted([
            'content1',
            'content2',
            'content3',
            ]),
            sorted([''.join(ipc.download(i[1])) for i in ipc.get(['report', guid, 'logs'])]))
        assert home_volume['report'][guid].exists

    def test_inline(self):
        routes._RECONNECT_TIMEOUT = 2

        this.injector = Injector('client')
        cp = ClientRoutes(Volume('client'), SugarCreds(client.keyfile.value))
        cp.connect(client.api.value)
        assert not cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='online')
        coroutine.sleep(.5)
        self.fork_master()
        trigger.wait(.5)
        assert trigger.value is None
        assert not cp.inline()

        trigger.wait()
        assert cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='offline')
        self.stop_master()
        trigger.wait()
        assert not cp.inline()

    def test_DoNotSwitchToOfflineOnRedirectFails(self):

        class Document(db.Resource):

            @db.stored_property(db.Blob)
            def blob1(self, value):
                raise http.Redirect(prefix + '/blob2')

            @db.stored_property(db.Blob)
            def blob2(self, value):
                raise http._ConnectionError()

        local_volume = self.start_online_client([User, Document])
        ipc = IPCConnection()
        guid = ipc.post(['document'], {})
        prefix = client.api.value + '/document/' + guid + '/'
        local_volume['document'].create({'guid': guid})

        trigger = self.wait_for_events(ipc, event='inline', state='connecting')
        try:
            ipc.get(['document', guid, 'blob1'])
        except Exception:
            pass
        assert trigger.wait(.1) is None

        trigger = self.wait_for_events(ipc, event='inline', state='connecting')
        try:
            ipc.get(['document', guid, 'blob2'])
        except Exception:
            pass
        assert trigger.wait(.1) is not None

    def test_FallbackToLocalOnRemoteTransportFails(self):

        class LocalRoutes(routes._LocalRoutes):

            @route('GET', cmd='sleep')
            def sleep(self):
                return 'local'

            @route('GET', cmd='yield_raw_and_sleep',
                    mime_type='application/octet-stream')
            def yield_raw_and_sleep(self):
                yield 'local'

            @route('GET', cmd='yield_json_and_sleep',
                    mime_type='application/json')
            def yield_json_and_sleep(self):
                yield '"local"'

        self.override(routes, '_LocalRoutes', LocalRoutes)
        this.injector = Injector('client')
        home_volume = self.start_client()
        ipc = IPCConnection()

        self.assertEqual('local', ipc.get(cmd='sleep'))
        self.assertEqual('local', ipc.get(cmd='yield_raw_and_sleep'))
        self.assertEqual('local', ipc.get(cmd='yield_json_and_sleep'))

        class NodeRoutes(MasterRoutes):

            @route('GET', cmd='sleep')
            def sleep(self):
                coroutine.sleep(.5)
                return 'remote'

            @route('GET', cmd='yield_raw_and_sleep',
                    mime_type='application/octet-stream')
            def yield_raw_and_sleep(self):
                for __ in range(33):
                    yield "remote\n"
                coroutine.sleep(.5)
                for __ in range(33):
                    yield "remote\n"

            @route('GET', cmd='yield_json_and_sleep',
                    mime_type='application/json')
            def yield_json_and_sleep(self):
                yield '"'
                yield 'r'
                coroutine.sleep(1)
                yield 'emote"'

        trigger = self.wait_for_events(ipc, event='inline', state='online')
        coroutine.dispatch()
        node_pid = self.fork_master([User], NodeRoutes)
        self.client_routes._remote_connect()
        trigger.wait()

        ts = time.time()
        self.assertEqual('remote', ipc.get(cmd='sleep'))
        self.assertEqual('remote\n' * 66, ipc.get(cmd='yield_raw_and_sleep'))
        self.assertEqual('remote', ipc.get(cmd='yield_json_and_sleep'))
        assert time.time() - ts >= 2

        def kill():
            coroutine.sleep(.4)
            self.waitpid(node_pid)

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='sleep'))
        assert not ipc.get(cmd='inline')

        node_pid = self.fork_master([User], NodeRoutes)
        self.client_routes._remote_connect()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='yield_raw_and_sleep'))
        assert not ipc.get(cmd='inline')

        node_pid = self.fork_master([User], NodeRoutes)
        self.client_routes._remote_connect()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='yield_json_and_sleep'))
        assert not ipc.get(cmd='inline')

    def test_ReconnectOnServerFall(self):
        routes._RECONNECT_TIMEOUT = 1

        this.injector = Injector('client')
        node_pid = self.fork_master()
        self.start_client()
        ipc = IPCConnection()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        def shutdown():
            coroutine.sleep(.1)
            self.waitpid(node_pid)
        coroutine.spawn(shutdown)
        self.wait_for_events(ipc, event='inline', state='offline').wait()

        self.fork_master()
        self.wait_for_events(ipc, event='inline', state='online').wait()

    def test_SilentReconnectOnGatewayErrors(self):

        class Routes(object):

            subscribe_tries = 0

            def __init__(self, master_api, volume, auth, *args):
                pass

            @route('GET', cmd='status', mime_type='application/json')
            def info(self):
                return {'resources': {}}

            @route('GET', cmd='subscribe', mime_type='text/event-stream')
            def subscribe(self, request=None, response=None, **condition):
                Routes.subscribe_tries += 1
                coroutine.sleep(.1)
                if Routes.subscribe_tries % 2:
                    raise http.BadGateway()
                else:
                    raise http.GatewayTimeout()

        this.injector = Injector('client')
        node_pid = self.start_master(None, Routes)
        self.start_client()
        ipc = IPCConnection()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        def read_events():
            for event in ipc.subscribe():
                events.append(event)
        events = []
        coroutine.spawn(read_events)

        coroutine.sleep(1)
        self.assertEqual([{'event': 'pong'}], events)
        assert Routes.subscribe_tries > 2

    def test_PullCheckinsOnGets(self):
        local_volume = self.start_online_client()
        local = IPCConnection()
        remote = Connection()

        self.assertEqual([[1, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': '1',
            'summary': '',
            'description': '',
            })
        local.put(['context', guid], None, cmd='favorite')
        self.assertEqual('1', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])
        coroutine.sleep(1.1)

        self.assertEqual([[1, 1], [6, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        remote.put(['context', guid, 'title'], '2')
        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])

        self.assertEqual([[1, 1], [6, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        self.assertEqual('2', local.get(['context'], reply='title')['result'][0]['title'])
        coroutine.sleep(.1)
        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('2', local.get(['context', guid])['title'])

        self.assertEqual([[1, 1], [7, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

    def test_PullCheckinsOnGettingOnline(self):
        routes._RECONNECT_TIMEOUT = 1
        routes._SYNC_TIMEOUT = 0

        local_volume = self.start_online_client()
        local = IPCConnection()
        remote = Connection()

        self.assertEqual([[1, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': '1',
            'summary': '',
            'description': '',
            })
        local.put(['context', guid], None, cmd='favorite')
        self.assertEqual('1', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])
        coroutine.sleep(1.1)

        remote.put(['context', guid, 'title'], '2')
        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])
        self.assertEqual([[1, 1], [6, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()
        self.fork_master()
        self.wait_for_events(event='sync', state='done').wait()

        self.assertEqual('2', local.get(['context', guid])['title'])
        self.assertEqual([[1, 1], [7, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

    def test_PullCheckinsOnUpdates(self):
        local_volume = self.start_online_client()
        local = IPCConnection()
        remote = Connection()

        self.assertEqual([[1, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': '1',
            'summary': '1',
            'description': '',
            })
        local.put(['context', guid], None, cmd='favorite')
        self.assertEqual('1', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])
        coroutine.sleep(1.1)

        remote.put(['context', guid, 'title'], '2')
        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('1', remote.get(['context', guid, 'summary']))
        self.assertEqual('1', local.get(['context', guid])['title'])
        self.assertEqual('1', local.get(['context', guid])['summary'])
        self.assertEqual([[1, 1], [6, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        local.put(['context', guid, 'summary'], '2')
        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('2', remote.get(['context', guid, 'summary']))
        self.assertEqual('2', local.get(['context', guid])['title'])
        self.assertEqual('2', local.get(['context', guid])['summary'])
        self.assertEqual([[1, 1], [8, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

    def test_PushOfflineChanges(self):
        routes._RECONNECT_TIMEOUT = 1
        routes._SYNC_TIMEOUT = 0

        local_volume = self.start_offline_client()
        local = IPCConnection()
        remote = Connection()

        guid1 = local.post(['context'], {'type': 'activity', 'title': '1', 'summary': '1', 'description': '1'})
        guid2 = local.post(['context'], {'type': 'activity', 'title': '2', 'summary': '2', 'description': '2'})
        local.put(['context', guid2], {'summary': '2_'})
        guid3 = local.post(['context'], {'type': 'activity', 'title': '3', 'summary': '3', 'description': '3'})
        local.delete(['context', guid3])

        assert not local_volume.empty
        assert [i for i in local_volume.blobs.walk()]

        self.fork_master()
        self.wait_for_events(event='sync', state='done').wait()

        self.assertEqual(
                sorted([
                    {'title': '1', 'summary': '1'},
                    {'title': '2', 'summary': '2_'},
                    ]),
                sorted([i for i in remote.get(['context'], reply=['title', 'summary'])['result']]))
        self.assertRaises(http.NotFound, remote.get, ['context', guid1])
        self.assertRaises(http.NotFound, remote.get, ['context', guid2])
        self.assertRaises(http.NotFound, remote.get, ['context', guid3])

        assert local_volume.empty
        assert not [i for i in local_volume.blobs.walk()]

    def test_PushOfflineChangesOfCheckins(self):
        routes._RECONNECT_TIMEOUT = 1
        routes._SYNC_TIMEOUT = 0

        local_volume = self.start_online_client()
        local = IPCConnection()
        remote = Connection()

        self.assertEqual([[1, None]], self.client_routes._pull_r.value)
        self.assertEqual(0, local_volume.seqno.value)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': '1',
            'summary': '',
            'description': '',
            })
        local.put(['context', guid], None, cmd='favorite')
        self.assertEqual('1', remote.get(['context', guid, 'title']))
        self.assertEqual('1', local.get(['context', guid])['title'])

        self.stop_master()
        self.wait_for_events(event='inline', state='offline').wait()

        local.put(['context', guid, 'title'], '2')
        self.assertNotEqual(0, local_volume['context'][guid]['seqno'])
        assert local_volume.has_noseqno
        assert local_volume.has_seqno

        self.fork_master()
        self.wait_for_events(event='sync', state='done').wait()

        self.assertEqual('2', remote.get(['context', guid, 'title']))
        self.assertEqual('2', local.get(['context', guid])['title'])
        self.assertEqual(0, local_volume['context'][guid]['seqno'])
        assert local_volume.has_noseqno
        assert not local_volume.has_seqno


if __name__ == '__main__':
    tests.main()
