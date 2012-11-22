#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import socket
import urllib2
import zipfile
from os.path import exists, abspath, join

from __init__ import tests

import active_document as ad
from active_toolkit import coroutine, sockets
from sugar_network.client.mounts import HomeMount
from sugar_network.client.mountset import Mountset
from sugar_network.toolkit import mountpoints
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network import client as local, sugar
from sugar_network.resources.volume import Volume
from sugar_network.resources.artifact import Artifact
from sugar_network.zerosugar import clones
from sugar_network.toolkit.router import IPCRouter
from sugar_network import IPCClient


class NodeMountTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.events_job = None
        local.server_mode.value = True

    def tearDown(self):
        if self.events_job is not None:
            self.events_job.kill()
        tests.Test.tearDown(self)

    def start_server(self):
        self.touch('mnt/.sugar-network')
        self.touch(('mnt/node', 'node'))
        local.mounts_root.value = tests.tmpdir

        volume = Volume('local', [User, Context, Artifact])
        self.mounts = Mountset(volume)
        self.server = coroutine.WSGIServer(
                ('localhost', local.ipc_port.value), IPCRouter(self.mounts))
        coroutine.spawn(self.server.serve_forever)
        self.mounts.open()
        mountpoints.populate(tests.tmpdir)
        coroutine.spawn(mountpoints.monitor, tests.tmpdir)
        self.mounts.opened.wait()

        return self.mounts

    def test_Events(self):
        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        events = []
        got_event = coroutine.Event()

        def read_events():
            for event in remote.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
                if event['event'] != 'handshake':
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
            {'event': 'handshake'},
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'create', 'guid': guid, 'seqno': 1},
            ],
            events)
        del events[:]

        remote.put(['context', guid], {'title': 'new-title'})
        got_event.wait()
        got_event.clear()
        self.assertEqual([
            {'mountpoint': tests.tmpdir + '/mnt', 'document': 'context', 'event': 'update', 'guid': guid, 'seqno': 2},
            ],
            events)
        del events[:]

        guid_path = 'mnt/context/%s/%s' % (guid[:2], guid)
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
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

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
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

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
        blob_url = 'http://localhost:%s/context/%s/icon?mountpoint=%s' % (local.ipc_port.value, guid, tests.tmpdir + '/mnt')
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
        client = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = client.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, client.get, ['artifact', guid, 'data'])
        blob_url = 'http://localhost:%s/artifact/%s/data?mountpoint=%s' % (local.ipc_port.value, guid, tests.tmpdir + '/mnt')
        self.assertEqual(
                [{'guid': guid, 'data': blob_url}],
                client.get(['artifact'], reply=['guid', 'data'])['result'])
        self.assertEqual(
                {'data': blob_url},
                client.get(['artifact', guid], reply=['data']))
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, blob_url)

    def test_get_blob_ExtractImplementations(self):
        Volume.RESOURCES = [
                'sugar_network.resources.user',
                'sugar_network.resources.implementation',
                ]

        mounts = self.start_server()
        mounts[tests.tmpdir + '/mnt'].mounted.wait()
        remote = IPCClient(mountpoint=tests.tmpdir + '/mnt')

        guid = remote.post(['implementation'], {
            'context': 'context',
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })

        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()
        remote.put(['implementation', guid, 'data'], cmd='upload_blob', path=abspath('bundle'))

        blob = remote.get(['implementation', guid, 'data'], cmd='get_blob')
        self.assertEqual(abspath('cache/implementation/%s/%s/data' % (guid[:2], guid)), blob['path'])
        self.assertEqual('probe', file(join(blob['path'], 'probe')).read())


if __name__ == '__main__':
    tests.main()
