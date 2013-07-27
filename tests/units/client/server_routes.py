#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
from os.path import exists

from __init__ import tests, src_root

from sugar_network import db, client, model
from sugar_network.client import IPCClient
from sugar_network.client.routes import ClientRoutes
from sugar_network.db import Volume
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import mountpoints, coroutine


class ServerCommandsTest(tests.Test):

    def start_node(self):
        os.makedirs('disk/sugar-network')
        self.node_volume = Volume('db', model.RESOURCES)
        cp = ClientRoutes(self.node_volume)
        trigger = self.wait_for_events(cp, event='inline', state='online')
        coroutine.spawn(mountpoints.monitor, tests.tmpdir)
        trigger.wait()
        server = coroutine.WSGIServer(('127.0.0.1', client.ipc_port.value), Router(cp))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()
        return cp

    def test_PopulateNode(self):
        os.makedirs('disk/sugar-network')
        volume = Volume('db', model.RESOURCES)
        cp = ClientRoutes(volume)

        assert not cp.inline()
        trigger = self.wait_for_events(cp, event='inline', state='online')
        mountpoints.populate('.')
        assert trigger.value is not None
        assert cp.inline()

    def test_MountNode(self):
        volume = Volume('db', model.RESOURCES)
        cp = ClientRoutes(volume)

        trigger = self.wait_for_events(cp, event='inline', state='online')
        mountpoints.populate('.')
        assert not cp.inline()
        assert trigger.value is None

        coroutine.spawn(mountpoints.monitor, '.')
        coroutine.dispatch()
        os.makedirs('disk/sugar-network')
        trigger.wait()
        assert cp.inline()

    def test_UnmountNode(self):
        cp = self.start_node()
        assert cp.inline()
        trigger = self.wait_for_events(cp, event='inline', state='offline')
        shutil.rmtree('disk')
        trigger.wait()
        assert not cp.inline()

    def test_whoami(self):
        self.start_node()
        ipc = IPCClient()

        self.assertEqual(
                {'guid': tests.UID, 'roles': []},
                ipc.get(cmd='whoami'))

    def test_subscribe(self):
        self.start_node()
        ipc = IPCClient()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        job = coroutine.spawn(read_events)
        coroutine.dispatch()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch()
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch()
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            ],
            events)

    def test_BLOBs(self):
        self.start_node()
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.request('PUT', ['context', guid, 'preview'], 'image')

        self.assertEqual(
                'image',
                ipc.request('GET', ['context', guid, 'preview']).content)
        self.assertEqual(
                {'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://127.0.0.1:5555/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://127.0.0.1:5555/static/images/missing.png'}],
                ipc.get(['context'], reply=['icon'])['result'])


if __name__ == '__main__':
    tests.main()
