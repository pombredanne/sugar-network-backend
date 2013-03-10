#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
import zipfile
from os.path import exists, abspath, join

from __init__ import tests

from sugar_network.client.mounts import HomeMount
from sugar_network.client.mountset import Mountset
from sugar_network.toolkit import coroutine, mountpoints
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.volume import Volume
from sugar_network.resources.artifact import Artifact
from sugar_network.zerosugar import clones
from sugar_network.toolkit.router import IPCRouter
from sugar_network.client import IPCClient, server_mode, mounts_root, ipc_port


class NetworkMountTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.events_job = None
        server_mode.value = True

    def tearDown(self):
        if self.events_job is not None:
            self.events_job.kill()
        tests.Test.tearDown(self)

    def start_server(self):
        self.touch(('mnt/sugar-network/node', 'node'))
        mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context, Artifact])
        self.mounts = Mountset(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', ipc_port.value), IPCRouter(self.mounts))
        coroutine.spawn(self.server.serve_forever)
        self.mounts.open()
        mountpoints.populate(tests.tmpdir)
        coroutine.spawn(mountpoints.monitor, tests.tmpdir)
        self.mounts.opened.wait()

        return self.mounts

    def test_Events(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(params={'mountpoint': tests.tmpdir + '/mnt'})

        events = []
        got_event = coroutine.Event()

        def read_events():
            for event in remote.subscribe(event='!commit'):
                if 'props' in event:
                    event.pop('props')
                events.append(event)
                got_event.set()
        job = coroutine.spawn(read_events)

        guid = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'create', 'guid': guid},
            ],
            events)
        del events[:]

        remote.put(['context', guid], {'title': 'new-title'})
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'update', 'guid': guid},
            ],
            events)
        del events[:]

        guid_path = 'mnt/sugar-network/db/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)
        remote.delete(['context', guid])
        assert not exists(guid_path)
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'delete', 'guid': guid},
            ],
            events)
        del events[:]

    def test_upload_blob(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(params={'mountpoint': tests.tmpdir + '/mnt'})

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        client.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        self.assertEqual('blob', client.request('GET', ['context', guid, 'preview']).content)

        self.touch(('file2', 'blob2'))
        client.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        self.assertEqual('blob2', client.request('GET', ['context', guid, 'preview']).content)
        assert not exists('file2')

    def test_GetBLOBs(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(params={'mountpoint': tests.tmpdir + '/mnt'})

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'icon-blob'))
        client.put(['context', guid, 'icon'], cmd='upload_blob', path=abspath('file'))

        self.assertEqual(
                'icon-blob',
                client.request('GEt', ['context', guid, 'icon']).content)
        blob_url = 'http://localhost:%s/context/%s/icon?mountpoint=%s' % (ipc_port.value, guid, tests.tmpdir + '/mnt')
        self.assertEqual(
                [{'guid': guid, 'icon': blob_url}],
                client.get(['context'], reply=['guid', 'icon'])['result'])
        self.assertEqual(
                {'icon': blob_url},
                client.get(['context', guid], reply=['icon']))
        self.assertEqual(
                'icon-blob',
                urllib2.urlopen(blob_url).read())

    def test_GetAbsentBLOBs(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        client = IPCClient(params={'mountpoint': tests.tmpdir + '/mnt'})

        guid = client.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, client.get, ['artifact', guid, 'data'])
        blob_url = 'http://localhost:%s/artifact/%s/data?mountpoint=%s' % (ipc_port.value, guid, tests.tmpdir + '/mnt')
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                client.get(['artifact'], reply=['guid', 'data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                client.get(['artifact', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)


if __name__ == '__main__':
    tests.main()
