#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import socket
import urllib2
from cStringIO import StringIO
from os.path import exists, abspath

from __init__ import tests

from sugar_network import db, client as local
from sugar_network.toolkit.router import IPCRouter, Redirect
from sugar_network.client.mounts import RemoteMount
from sugar_network.client.mountset import Mountset
from sugar_network.toolkit import sugar, http, coroutine
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.volume import Volume, Resource
from sugar_network.zerosugar import injector
from sugar_network.client import IPCClient

import requests


class RemoteMountTest(tests.Test):

    def test_Subscription(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient()
        events = []

        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
        job = coroutine.spawn(read_events)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch()
        remote.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch()
        remote.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'event': 'handshake'},
            {'event': 'handshake', 'mountpoint': '/'},
            {'guid': guid, 'document': 'context', 'event': 'create', 'mountpoint': '/'},
            {'guid': guid, 'document': 'context', 'event': 'update', 'mountpoint': '/'},
            {'guid': guid, 'event': 'delete', 'document': 'context', 'mountpoint': '/'},
            ],
            events)

    def test_Mount(self):
        pid = self.fork(self.restful_server)

        volume = Volume('local', [User, Context])
        self.mounts = Mountset(volume)
        self.mounts['/'] = RemoteMount(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', local.ipc_port.value), IPCRouter(self.mounts))
        coroutine.spawn(self.server.serve_forever)
        coroutine.dispatch()
        remote = IPCClient()

        events = []
        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
        job = coroutine.spawn(read_events)

        self.assertEqual(False, remote.get(cmd='mounted'))
        self.mounts.open()
        self.mounts['/'].mounted.wait()
        coroutine.sleep(1)

        self.assertEqual(True, remote.get(cmd='mounted'))
        self.assertEqual([
            {'event': 'handshake'},
            {'mountpoint': '/', 'event': 'mount', 'name': 'Network', 'private': False},
            {'event': 'handshake', 'mountpoint': '/'},
            ],
            events)
        del events[:]

        self.waitpid(pid)
        coroutine.sleep(1)

        self.assertEqual(False, remote.get(cmd='mounted'))
        self.assertEqual([
            {'mountpoint': '/', 'event': 'unmount', 'name': 'Network', 'private': False},
            ],
            events)
        del events[:]

        pid = self.fork(self.restful_server)
        # Ping to trigger re-connection
        self.assertEqual(False, remote.get(cmd='mounted'))
        coroutine.sleep(1)

        self.assertEqual(True, remote.get(cmd='mounted'))
        self.assertEqual([
            {'mountpoint': '/', 'event': 'mount', 'name': 'Network', 'private': False},
            {'event': 'handshake', 'mountpoint': '/'},
            ],
            events)
        del events[:]

    def test_upload_blob(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient()

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        self.assertEqual('blob', remote.request('GEt', ['context', guid, 'preview']).content)

        self.touch(('file2', 'blob2'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        self.assertEqual('blob2', remote.request('GET', ['context', guid, 'preview']).content)
        assert not exists('file2')

    def test_GetBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient()

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'icon-blob'))
        remote.put(['context', guid, 'icon'], cmd='upload_blob', path=abspath('file'))
        self.touch(('file', 'preview-blob'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))

        self.assertEqual(
                'preview-blob',
                remote.request('GET', ['context', guid, 'preview']).content)
        url_prefix = local.api_url.value + '/context/' + guid
        self.assertEqual(
                [{'guid': guid, 'icon': url_prefix + '/icon', 'preview': url_prefix + '/preview'}],
                remote.get(['context'], reply=['guid', 'icon', 'preview'])['result'])
        self.assertEqual(
                {'icon': url_prefix + '/icon', 'preview': url_prefix + '/preview'},
                remote.get(['context', guid], reply=['icon', 'preview']))
        self.assertEqual(
                'icon-blob',
                urllib2.urlopen(url_prefix + '/icon').read())

    def test_GetAbsentBLOBs(self):
        self.start_ipc_and_restful_server([User, Context, Artifact, Implementation])
        remote = IPCClient()

        guid = remote.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, remote.get, ['artifact', guid, 'data'])
        blob_url = local.api_url.value + '/artifact/%s/data' % guid
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                remote.get(['artifact'], reply=['guid', 'data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                remote.get(['artifact', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_Feed(self):
        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        remote = IPCClient()

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl1 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {}},
            })
        impl2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                }},
            })

        self.assertEqual([
            {
                'version': '1',
                'arch': '*-*',
                'stability': 'stable',
                'guid': impl1,
                },
            {
                'version': '2',
                'arch': '*-*',
                'stability': 'stable',
                'guid': impl2,
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                },
            ],
            remote.get(['context', context, 'versions']))

    def test_RestrictLayers(self):
        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        remote = IPCClient()

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {}},
            })
        artifact = remote.post(['artifact'], {
            'type': 'instance',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })

        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['context'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['context'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['context'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['implementation'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['implementation'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['implementation'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['artifact'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['artifact'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['artifact'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                remote.get(['context', context, 'versions']))
        self.assertEqual(
                [],
                remote.get(['context', context, 'versions'], layer='foo'))
        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                remote.get(['context', context, 'versions'], layer='public'))

        local.layers.value = ['foo', 'bar']

        self.assertEqual(
                [],
                remote.get(['context'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['context'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['context'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [],
                remote.get(['implementation'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['implementation'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['implementation'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['artifact'], reply='layer')['result'])
        self.assertEqual(
                [],
                remote.get(['artifact'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                remote.get(['artifact'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [],
                remote.get(['context', context, 'versions']))
        self.assertEqual(
                [],
                remote.get(['context', context, 'versions'], layer='foo'))
        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                remote.get(['context', context, 'versions'], layer='public'))

    def test_InvalidateSolutions(self):
        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        remote = IPCClient()
        self.assertNotEqual(None, injector._mtime)

        mtime = injector._mtime
        coroutine.sleep(1.5)

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        assert injector._mtime == mtime

        impl1 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {}},
            })
        assert injector._mtime > mtime

        mtime = injector._mtime
        coroutine.sleep(1.5)

        impl2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                }},
            })
        assert injector._mtime > mtime

    def test_ContentDisposition(self):
        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        remote = IPCClient()

        artifact = remote.post(['artifact'], {
            'type': 'instance',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })
        remote.request('PUT', ['artifact', artifact, 'data'], 'blob', headers={'Content-Type': 'image/png'})

        response = remote.request('GET', ['artifact', artifact, 'data'])
        self.assertEqual(
                'attachment; filename="Title.png"',
                response.headers.get('Content-Disposition'))

    def test_Redirects(self):
        URL = 'http://sugarlabs.org'

        class Document(Resource):

            @db.blob_property()
            def blob(self, value):
                raise Redirect(URL)

        self.start_ipc_and_restful_server([User, Document])
        remote = IPCClient()
        guid = remote.post(['document'], {})

        response = requests.request('GET', local.api_url.value + '/document/' + guid + '/blob', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual(URL, response.headers['Location'])

    def test_ConnectOnDemand(self):
        local.connect_timeout.value = 1
        pid = self.fork(self.restful_server)
        self.start_server()

        client = IPCClient()
        self.assertRaises(RuntimeError, client.post, ['context'], {'type': 'activity', 'title': 'title', 'summary': 'summary', 'description': 'description'})

        client = IPCClient(sync=True)
        guid = client.post(['context'], {'type': 'activity', 'title': 'title', 'summary': 'summary', 'description': 'description'})
        self.assertEqual(guid, client.get(['context', guid, 'guid']))

        self.waitpid(pid)
        ts = time.time()
        self.assertRaises(RuntimeError, client.get, ['context', guid, 'guid'])
        assert time.time() - ts >= 1

        ts = time.time()
        self.assertRaises(RuntimeError, client.get, ['context', guid, 'guid'])
        assert time.time() - ts >= 1

        pid = self.fork(self.restful_server)
        self.assertEqual(guid, client.get(['context', guid, 'guid']))
        self.assertEqual(guid, client.get(['context', guid, 'guid']))


if __name__ == '__main__':
    tests.main()
