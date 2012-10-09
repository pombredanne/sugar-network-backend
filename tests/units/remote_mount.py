#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
from cStringIO import StringIO
from os.path import exists, abspath

from __init__ import tests

from active_toolkit import sockets, coroutine
from sugar_network import local
from sugar_network.local.ipc_client import Router as IPCRouter
from sugar_network.local.mounts import RemoteMount
from sugar_network.local.mountset import Mountset
from sugar_network.toolkit import sugar, http
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.volume import Volume
from sugar_network import IPCClient


class RemoteMountTest(tests.Test):

    def test_GetKeep(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        context = remote.get(['context', guid], reply=['keep', 'keep_impl'])
        self.assertEqual(False, context['keep'])
        self.assertEqual(0, context['keep_impl'])
        cursor = remote.get(['context'], reply=['keep', 'keep_impl'])['result']
        self.assertEqual(
                [(guid, False, False)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in cursor])

        self.mounts.volume['context'].create(guid=guid, type='activity',
                title={'en': 'local'}, summary={'en': 'summary'},
                description={'en': 'description'}, keep=True, keep_impl=2,
                user=[sugar.uid()])

        context = remote.get(['context', guid], reply=['keep', 'keep_impl'])
        self.assertEqual(True, context['keep'])
        self.assertEqual(2, context['keep_impl'])
        cursor = remote.get(['context'], reply=['keep', 'keep_impl'])['result']
        self.assertEqual(
                [(guid, True, 2)],
                [(i['guid'], i['keep'], i['keep_impl']) for i in cursor])

    def test_SetKeep(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')
        local = IPCClient(mountpoint='~')

        guid_1 = remote.post(['context'], {
            'type': 'activity',
            'title': 'remote',
            'summary': 'summary',
            'description': 'description',
            })
        guid_2 = remote.post(['context'], {
            'type': 'activity',
            'title': 'remote-2',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, local.get, ['context', guid_1])
        self.assertRaises(RuntimeError, local.get, ['context', guid_2])

        remote.put(['context', guid_1], {'keep': True})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

        remote.put(['context', guid_1], {'keep': False})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': False, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

        local.put(['context', guid_1], {'title': 'local'})

        self.assertEqual(
                {'title': 'local'},
                local.get(['context', guid_1], reply=['title']))

        remote.put(['context', guid_1], {'keep': True})

        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'local', 'keep': True, 'keep_impl': 0},
                    ]),
                sorted(local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'title': 'remote', 'keep': True, 'keep_impl': 0},
                    {'guid': guid_2, 'title': 'remote-2', 'keep': False, 'keep_impl': 0},
                    ]),
                sorted(remote.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl'])['result']))

    def test_Subscription(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')
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
            {'guid': guid, 'seqno': 2, 'document': 'context', 'event': 'create', 'mountpoint': '/'},
            {'guid': guid, 'seqno': 3, 'document': 'context', 'event': 'update', 'mountpoint': '/'},
            {'guid': guid, 'seqno': 4, 'event': 'delete', 'document': 'context', 'mountpoint': '/'},
            ],
            events)

    def test_Subscription_NotifyOnline(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')
        local = IPCClient(mountpoint='~')
        events = []

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'keep': True,
            })

        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)

        coroutine.sleep(1)
        job = coroutine.spawn(read_events)
        local.put(['context', guid], {'keep': False})
        coroutine.sleep(1)
        job.kill()

        self.assertEqual([
            {'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 5},
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
        remote = IPCClient(mountpoint='/')

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
            {'mountpoint': '/', 'event': 'mount', 'name': 'Network', 'private': False},
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
            ],
            events)
        del events[:]

    def test_upload_blob(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')

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
        remote = IPCClient(mountpoint='/')

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
        assert local.ipc_port.value != 8800
        url_prefix = 'http://localhost:8800/context/' + guid
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
        self.start_ipc_and_restful_server([User, Artifact])
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['artifact'], {})

        self.assertRaises(RuntimeError, remote.get, ['artifact', guid, 'data'])
        blob_url = 'http://localhost:8800/artifact/%s/data' % guid
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                remote.get(['artifact'], reply=['guid', 'data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                remote.get(['artifact', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_Feed(self):
        self.start_ipc_and_restful_server([User, Context, Implementation, Artifact])
        remote = IPCClient(mountpoint='/')

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
            'date': 0,
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {}},
            })
        impl2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'date': 0,
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
        remote = IPCClient(mountpoint='/')

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
            'date': 0,
            'stability': 'stable',
            'notes': '',
            'spec': {'*-*': {}},
            })
        artifact = remote.post(['artifact'], {})

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


if __name__ == '__main__':
    tests.main()
