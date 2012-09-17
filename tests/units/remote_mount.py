#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
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
from sugar_network.resources.report import Report
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

        self.mounts.home_volume['context'].create(guid=guid, type='activity',
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
            })

        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)

        job = coroutine.spawn(read_events)
        local.put(['context', guid], {'keep': False})
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 1},
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
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob', file(blob['path']).read())

        self.touch(('file2', 'blob2'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob2', file(blob['path']).read())
        assert not exists('file2')

    def test_GetAbsentBLOB(self):
        self.start_ipc_and_restful_server([User, Report])
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['report'], {
            'context': 'context',
            'implementation': 'implementation',
            'description': 'description',
            })

        self.assertEqual(None, remote.get(['report', guid, 'data'], cmd='get_blob'))

    def test_GetDefaultBLOB(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        blob = remote.get(['context', guid, 'icon'], cmd='get_blob')
        assert not blob['path'].endswith('missing.png')
        assert exists(blob['path'])
        assert file(blob['path'], 'rb').read() == file('../../../sugar_network/static/images/missing.png', 'rb').read()

    def test_StaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob-1'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob-1', file(blob['path']).read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.touch((cache_path, 'blob-2'))
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob-2', file(blob['path']).read())
        self.assertEqual(3, json.load(file(cache_path + '.meta'))['seqno'])

        self.touch(('file', 'blob-3'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob-3', file(blob['path']).read())
        self.assertEqual(4, json.load(file(cache_path + '.meta'))['seqno'])

    def test_DoNotStaleBLOBs(self):
        self.start_ipc_and_restful_server()
        remote = IPCClient(mountpoint='/')

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob-1'))
        remote.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob-1', file(blob['path']).read())

        cache_path = 'cache/context/%s/%s/preview' % (guid[:2], guid)
        self.touch((cache_path, 'blob-2'))
        self.assertEqual(3, json.load(file(cache_path + '.meta'))['seqno'])

        # Shift seqno
        remote.put(['context', guid], {'title': 'title-2'})
        coroutine.sleep(1)

        blob = remote.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob-2', file(blob['path']).read())
        self.assertEqual(4, json.load(file(cache_path + '.meta'))['seqno'])


if __name__ == '__main__':
    tests.main()
