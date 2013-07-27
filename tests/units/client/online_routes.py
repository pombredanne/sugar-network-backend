#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
import shutil
import zipfile
from cStringIO import StringIO
from os.path import exists

from __init__ import tests, src_root

from sugar_network import client, db, model
from sugar_network.client import IPCConnection, journal, clones, injector, routes
from sugar_network.toolkit import coroutine, http
from sugar_network.toolkit.spec import Spec
from sugar_network.client.routes import ClientRoutes, Request, Response
from sugar_network.node.master import MasterRoutes
from sugar_network.db import Volume, Resource
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.model.artifact import Artifact
from sugar_network.toolkit.router import route

import requests


class OnlineRoutes(tests.Test):

    def test_inline(self):
        cp = ClientRoutes(Volume('client', model.RESOURCES), client.api_url.value)
        assert not cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='online')
        coroutine.sleep(1)
        self.start_master()
        trigger.wait(1)
        assert trigger.value is None
        assert not cp.inline()

        request = Request(method='GET', cmd='whoami')
        cp.whoami(request, Response())
        trigger.wait()
        assert cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert not cp.inline()

    def test_whoami(self):
        self.start_online_client()
        ipc = IPCConnection()

        self.assertEqual(
                {'guid': tests.UID, 'roles': []},
                ipc.get(cmd='whoami'))

    def test_clone_Activities(self):
        self.home_volume = self.start_online_client()
        ipc = IPCConnection()
        coroutine.spawn(clones.monitor, self.home_volume['context'], ['Activities'])

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl, {'data': {
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'extract': 'TestActivitry',
                    },
                },
            'blob': StringIO(self.zips(['TestActivitry/activity/activity.info', [
                '[Activity]',
                'name = TestActivitry',
                'bundle_id = %s' % context,
                'exec = false',
                'icon = icon',
                'activity_version = 1',
                'license=Public Domain',
                ]])),
            }})

        assert not exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 2, cmd='clone')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 2, cmd='clone')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 1, cmd='clone', force=1)
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 0, cmd='clone')
        coroutine.sleep(.5)

        assert not exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 0},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 1, cmd='clone')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context], reply=['clone']))

        trigger = self.wait_for_events(ipc, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert ipc.get(cmd='status')['route'] == 'offline'

        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context], reply=['clone']))

    def test_clone_ActivityImpl(self):
        self.home_volume = self.start_online_client()
        ipc = IPCConnection()
        coroutine.spawn(clones.monitor, self.home_volume['context'], ['Activities'])

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        impl1 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl1, {'data': {
            'blob': StringIO(self.zips(('TestActivity/activity/activity.info', [
                '[Activity]',
                'name = TestActivity',
                'bundle_id = %s' % context,
                'exec = true',
                'icon = icon',
                'activity_version = 1',
                'license=GPLv3+',
                ]))),
            'spec': {
                '*-*': {
                    'extract': 'TestActivity',
                    'commands': {'activity': {'exec': 'true'}},
                    'requires': {
                        'dep1': {},
                        },
                    },
                },
            }})

        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl2, {'data': {
            'blob': StringIO(self.zips(('TestActivity/activity/activity.info', [
                '[Activity]',
                'name = TestActivity',
                'bundle_id = %s' % context,
                'exec = true',
                'icon = icon',
                'activity_version = 2',
                'license=GPLv3+',
                ]))),
            'spec': {
                '*-*': {
                    'extract': 'TestActivity',
                    'commands': {'activity': {'exec': 'true'}},
                    'requires': {
                        'dep2': {},
                        },
                    },
                },
            }})

        impl3 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '3',
            'stability': 'developer',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl3, {'data': {
            'blob': StringIO(self.zips(('TestActivity/activity/activity.info', [
                '[Activity]',
                'name = TestActivity',
                'bundle_id = %s' % context,
                'exec = true',
                'icon = icon',
                'activity_version = 3',
                'license=GPLv3+',
                ]))),
            'spec': {
                '*-*': {
                    'extract': 'TestActivity',
                    'commands': {'activity': {'exec': 'true'}},
                    'requires': {
                        'dep3': {},
                        },
                    },
                },
            }})

        ipc.put(['context', context], 2, cmd='clone', nodeps=1, requires='dep4')
        coroutine.sleep(.1)
        self.assertEqual({'clone': 0}, ipc.get(['context', context], reply=['clone']))
        assert not exists('Activities/TestActivity/activity/activity.info')

        ipc.put(['context', context], 2, cmd='clone', nodeps=1)
        coroutine.sleep(.1)
        self.assertEqual({'clone': 2}, ipc.get(['context', context], reply=['clone']))
        self.assertEqual('2', Spec('Activities/TestActivity/activity/activity.info')['version'])

        ipc.put(['context', context], 0, cmd='clone')
        coroutine.sleep(.1)
        self.assertEqual({'clone': 0}, ipc.get(['context', context], reply=['clone']))
        assert not exists('Activities/TestActivity/activity/activity.info')

        ipc.put(['context', context], 2, cmd='clone', nodeps=1, stability='developer')
        coroutine.sleep(.1)
        self.assertEqual({'clone': 2}, ipc.get(['context', context], reply=['clone']))
        self.assertEqual('3', Spec('Activities/TestActivity/activity/activity.info')['version'])

        ipc.put(['context', context], 0, cmd='clone')
        coroutine.sleep(.1)
        self.assertEqual({'clone': 0}, ipc.get(['context', context], reply=['clone']))
        assert not exists('Activities/TestActivity/activity/activity.info')

        ipc.put(['context', context], 2, cmd='clone', nodeps=1, requires='dep1')
        coroutine.sleep(.1)
        self.assertEqual({'clone': 2}, ipc.get(['context', context], reply=['clone']))
        self.assertEqual('1', Spec('Activities/TestActivity/activity/activity.info')['version'])

    def test_clone_Content(self):
        self.start_online_client()
        updates = []

        def journal_update(self, guid, data=None, preview=None, **kwargs):
            if data is not None:
                kwargs['data'] = data.read()
            updates.append((guid, kwargs))

        self.override(journal.Routes, '__init__', lambda *args: None)
        self.override(journal.Routes, 'journal_update', journal_update)
        self.override(journal.Routes, 'journal_delete', lambda self, guid: updates.append((guid,)))

        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'content',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        ipc.request('PUT', ['implementation', impl, 'data'], 'version_1')

        self.assertEqual({'clone': 0, 'type': ['content']}, ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 2, cmd='clone')
        self.touch('datastore/%s/%s/metadata/uid' % (context[:2], context))

        self.assertEqual([
            (context, {'activity_id': impl, 'data': 'version_1', 'description': 'description', 'title': 'title', 'mime_type': 'application/octet-stream'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                ipc.get(['context', context], reply=['clone']))
        del updates[:]

        ipc.request('PUT', ['implementation', impl, 'data'], 'version_2',
                headers={'Content-Type': 'foo/bar'})
        ipc.put(['context', context], 2, cmd='clone')

        self.assertEqual(
                [],
                updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 1, cmd='clone', force=1)

        self.assertEqual([
            (context, {'activity_id': impl, 'data': 'version_2', 'description': 'description', 'title': 'title', 'mime_type': 'foo/bar'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2, 'type': ['content']},
                ipc.get(['context', context], reply=['clone']))
        del updates[:]

        ipc.put(['context', context], 0, cmd='clone')
        shutil.rmtree('datastore/%s/%s' % (context[:2], context))

        self.assertEqual([
            (context,),
            ],
            updates)
        self.assertEqual(
                {'clone': 0, 'type': ['content']},
                ipc.get(['context', context], reply=['clone']))
        del updates[:]

    def test_clone_Artifacts(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        updates = []

        def journal_update(self, guid, data=None, preview=None, **kwargs):
            if data is not None:
                kwargs['data'] = data.read()
            updates.append((guid, kwargs))

        self.override(journal.Routes, '__init__', lambda *args: None)
        self.override(journal.Routes, 'journal_update', journal_update)
        self.override(journal.Routes, 'journal_delete', lambda self, guid: updates.append((guid,)))

        ipc = IPCConnection()

        artifact = ipc.post(['artifact'], {
            'context': 'context',
            'type': 'instance',
            'title': 'title',
            'description': 'description',
            })
        ipc.request('PUT', ['artifact', artifact, 'data'], 'data')

        self.assertEqual({'clone': 0}, ipc.get(['artifact', artifact], reply=['clone']))

        ipc.put(['artifact', artifact], 2, cmd='clone')
        self.touch('datastore/%s/%s/metadata/uid' % (artifact[:2], artifact))

        self.assertEqual([
            (artifact, {'data': 'data', 'description': 'description', 'title': 'title', 'activity': 'context'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2},
                ipc.get(['artifact', artifact], reply=['clone']))
        del updates[:]

        ipc.put(['artifact', artifact], 2, cmd='clone')

        self.assertEqual(
                [],
                updates)
        self.assertEqual(
                {'clone': 2},
                ipc.get(['artifact', artifact], reply=['clone']))

        ipc.request('PUT', ['artifact', artifact, 'data'], 'data_2')
        ipc.put(['artifact', artifact], 1, cmd='clone', force=1)

        self.assertEqual([
            (artifact, {'data': 'data_2', 'description': 'description', 'title': 'title', 'activity': 'context'}),
            ],
            updates)
        self.assertEqual(
                {'clone': 2},
                ipc.get(['artifact', artifact], reply=['clone']))
        del updates[:]

        ipc.put(['artifact', artifact], 0, cmd='clone')
        shutil.rmtree('datastore/%s/%s' % (artifact[:2], artifact))

        self.assertEqual([
            (artifact,),
            ],
            updates)
        self.assertEqual(
                {'clone': 0},
                ipc.get(['artifact', artifact], reply=['clone']))
        del updates[:]

    def test_favorite(self):
        self.start_online_client()
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                {'favorite': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], True, cmd='favorite')
        coroutine.sleep(.5)
        self.assertEqual(
                {'favorite': True},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], False, cmd='favorite')
        self.assertEqual(
                {'favorite': False},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], True, cmd='favorite')
        coroutine.sleep(.5)
        self.assertEqual(
                {'favorite': True},
                ipc.get(['context', context], reply=['favorite']))

        trigger = self.wait_for_events(ipc, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert ipc.get(cmd='status')['route'] == 'offline'

        self.assertEqual(
                {'favorite': True},
                ipc.get(['context', context], reply=['favorite']))

    def test_subscribe(self):
        self.start_online_client()
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        job = coroutine.spawn(read_events)
        coroutine.dispatch(.1)

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch(.1)
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch(.1)
        ipc.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)
        del events[:]

        job = coroutine.spawn(read_events)
        coroutine.dispatch(.1)
        guid = self.node_volume['context'].create({
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch(.1)
        self.node_volume['context'].update(guid, {
            'title': 'title_2',
            })
        coroutine.dispatch(.1)
        self.node_volume['context'].delete(guid)
        coroutine.dispatch(.1)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)

    def test_BLOBs(self):
        self.start_online_client()
        ipc = IPCConnection()

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
                {'preview': 'http://127.0.0.1:8888/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://127.0.0.1:8888/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://127.0.0.1:8888/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://127.0.0.1:8888/static/images/missing.png'}],
                ipc.get(['context'], reply=['icon'])['result'])

    def test_Feeds(self):
        self.start_online_client()
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl1 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl1, {'data': {
            'spec': {'*-*': {}},
            }})
        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl2, {'data': {
            'spec': {'*-*': {
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                }},
            }})

        self.assertEqual({
            'name': 'title',
            'implementations': [
                {
                    'version': '1',
                    'arch': '*-*',
                    'stability': 'stable',
                    'guid': impl1,
                    'unpack_size': None,
                    'blob_size': None,
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
                    'unpack_size': None,
                    'blob_size': None,
                    },
                ],
            },
            ipc.get(['context', context], cmd='feed'))

    def test_Feeds_RestrictLayers(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl, {'data': {
            'spec': {'*-*': {}},
            }})
        artifact = ipc.post(['artifact'], {
            'type': 'instance',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })

        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['context'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['context'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['context'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['implementation'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['implementation'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['implementation'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['artifact'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['artifact'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['artifact'], reply='layer', layer='public')['result'])

        self.assertEqual({
            'name': 'title',
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'unpack_size': None,
                'blob_size': None,
                }],
            },
            ipc.get(['context', context], cmd='feed'))
        self.assertEqual({
            'name': 'title',
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed', layer='foo'))
        self.assertEqual({
            'name': 'title',
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'unpack_size': None,
                'blob_size': None,
                }],
            },
            ipc.get(['context', context], cmd='feed', layer='public'))

        client.layers.value = ['foo', 'bar']

        self.assertEqual(
                [],
                ipc.get(['context'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['context'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['context'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [],
                ipc.get(['implementation'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['implementation'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['implementation'], reply='layer', layer='public')['result'])

        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['artifact'], reply='layer')['result'])
        self.assertEqual(
                [],
                ipc.get(['artifact'], reply='layer', layer='foo')['result'])
        self.assertEqual(
                [{'layer': ['public']}],
                ipc.get(['artifact'], reply='layer', layer='public')['result'])

        self.assertEqual({
            'name': 'title',
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed'))
        self.assertEqual({
            'name': 'title',
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed', layer='foo'))
        self.assertEqual({
            'name': 'title',
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'unpack_size': None,
                'blob_size': None,
                }],
            },
            ipc.get(['context', context], cmd='feed', layer='public'))

    def test_Feeds_PreferLocalFeeds(self):
        home_volume = self.start_online_client()
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl, {'data': {
            'spec': {'*-*': {}},
            }})

        self.assertEqual({
            'name': 'title',
            'implementations': [
                {
                    'version': '2',
                    'arch': '*-*',
                    'stability': 'stable',
                    'guid': impl,
                    'unpack_size': None,
                    'blob_size': None,
                    },
                ],
            },
            ipc.get(['context', context], cmd='feed'))

        self.touch(('Activities/activity-1/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = ' + context,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ]))
        monitor = coroutine.spawn(clones.monitor, home_volume['context'], ['Activities'])
        coroutine.dispatch()

        self.assertEqual({
            'name': 'TestActivity',
            'implementations': [
                {
                    'version': '1',
                    'arch': '*-*',
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'stability': 'stable',
                    'guid': tests.tmpdir + '/Activities/activity-1',
                    'requires': {},
                    },
                ],
            },
            ipc.get(['context', context], cmd='feed'))

    def test_InvalidateSolutions(self):
        self.start_online_client()
        ipc = IPCConnection()
        self.assertNotEqual(None, injector._mtime)

        mtime = injector._mtime
        coroutine.sleep(1.5)

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        assert injector._mtime == mtime

        impl1 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl1, {'data': {
            'spec': {'*-*': {}},
            }})
        coroutine.sleep(.5)
        assert injector._mtime > mtime

        mtime = injector._mtime
        coroutine.sleep(1)

        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl2, {'data': {
            'spec': {'*-*': {
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                }},
            }})
        assert injector._mtime > mtime

    def test_ContentDisposition(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCConnection()

        artifact = ipc.post(['artifact'], {
            'type': 'instance',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })
        ipc.request('PUT', ['artifact', artifact, 'data'], 'blob', headers={'Content-Type': 'image/png'})

        response = ipc.request('GET', ['artifact', artifact, 'data'])
        self.assertEqual(
                'attachment; filename="Title.png"',
                response.headers.get('Content-Disposition'))

    def test_Redirects(self):
        URL = 'http://sugarlabs.org'

        class Document(Resource):

            @db.blob_property()
            def blob(self, value):
                raise http.Redirect(URL)

        self.start_online_client([User, Document])
        ipc = IPCConnection()
        guid = ipc.post(['document'], {})

        response = requests.request('GET', client.api_url.value + '/document/' + guid + '/blob', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual(URL, response.headers['Location'])

    def test_Proxy_Activities(self):
        home_volume = self.start_online_client()
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                [{'guid': context, 'favorite': False, 'clone': 0, 'type': ['activity']}],
                ipc.get(['context'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': False, 'clone': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite', 'clone']))

        home_volume['context'].create({
            'guid': context,
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'favorite': True,
            'clone': 2,
            })

        self.assertEqual(
                [{'guid': context, 'favorite': True, 'clone': 2, 'type': ['activity']}],
                ipc.get(['context'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': True, 'clone': 2},
                ipc.get(['context', context], reply=['favorite', 'clone']))

    def test_Proxy_Content(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'type': 'content',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                [{'guid': guid, 'favorite': False, 'clone': 0, 'type': ['content']}],
                ipc.get(['context'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': False, 'clone': 0, 'type': ['content']},
                ipc.get(['context', guid], reply=['favorite', 'clone']))

        self.touch(('datastore/%s/%s/metadata/keep' % (guid[:2], guid), '0'))

        self.assertEqual(
                [{'guid': guid, 'favorite': False, 'clone': 2, 'type': ['content']}],
                ipc.get(['context'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': False, 'clone': 2, 'type': ['content']},
                ipc.get(['context', guid], reply=['favorite', 'clone']))

        self.touch(('datastore/%s/%s/metadata/keep' % (guid[:2], guid), '1'))

        self.assertEqual(
                [{'guid': guid, 'favorite': True, 'clone': 2, 'type': ['content']}],
                ipc.get(['context'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': True, 'clone': 2, 'type': ['content']},
                ipc.get(['context', guid], reply=['favorite', 'clone']))

    def test_Proxy_Artifacts(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCConnection()

        guid = ipc.post(['artifact'], {
            'type': 'instance',
            'context': 'context',
            'title': 'title',
            'description': 'description',
            })

        self.assertEqual(
                [{'guid': guid, 'favorite': False, 'clone': 0}],
                ipc.get(['artifact'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': False, 'clone': 0},
                ipc.get(['artifact', guid], reply=['favorite', 'clone']))

        self.touch(('datastore/%s/%s/metadata/keep' % (guid[:2], guid), '0'))

        self.assertEqual(
                [{'guid': guid, 'favorite': False, 'clone': 2}],
                ipc.get(['artifact'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': False, 'clone': 2},
                ipc.get(['artifact', guid], reply=['favorite', 'clone']))

        self.touch(('datastore/%s/%s/metadata/keep' % (guid[:2], guid), '1'))

        self.assertEqual(
                [{'guid': guid, 'favorite': True, 'clone': 2}],
                ipc.get(['artifact'], reply=['favorite', 'clone'])['result'])
        self.assertEqual(
                {'favorite': True, 'clone': 2},
                ipc.get(['artifact', guid], reply=['favorite', 'clone']))

    def test_Proxy_NoNeedlessRemoteRequests(self):
        home_volume = self.start_online_client()
        ipc = IPCConnection()

        guid = ipc.post(['context'], {
            'type': 'content',
            'title': 'remote',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {'title': 'remote'},
                ipc.get(['context', guid], reply=['title']))

        home_volume['context'].create({
            'guid': guid,
            'type': 'activity',
            'title': 'local',
            'summary': 'summary',
            'description': 'description',
            'favorite': True,
            })
        self.assertEqual(
                {'title': 'local'},
                ipc.get(['context', guid], reply=['title']))

    def test_HomeVolumeEvents(self):
        self.home_volume = self.start_online_client()
        ipc = IPCConnection()
        coroutine.spawn(clones.monitor, self.home_volume['context'], ['Activities'])

        context1 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context1,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.node_volume['implementation'].update(impl, {'data': {
            'spec': {
                '*-*': {
                    'commands': {
                        'activity': {
                            'exec': 'true',
                            },
                        },
                    'stability': 'stable',
                    'size': 0,
                    'extract': 'TestActivitry',
                    },
                },
            'blob': StringIO(self.zips(['TestActivitry/activity/activity.info', [
                '[Activity]',
                'name = TestActivitry',
                'bundle_id = %s' % context1,
                'exec = false',
                'icon = icon',
                'activity_version = 1',
                'license=Public Domain',
                ]])),

            }})

        trigger = self.wait_for_events(ipc, event='update', resource='context', guid=context1)
        ipc.put(['context', context1], 2, cmd='clone')
        trigger.wait()
        self.assertEqual(
                {'clone': 2},
                ipc.get(['context', context1], reply=['clone']))

        context2 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        trigger = self.wait_for_events(ipc, event='create', resource='context', guid=context2)
        ipc.put(['context', context2], True, cmd='favorite')
        trigger.wait()
        self.assertEqual(
                {'favorite': True},
                ipc.get(['context', context2], reply=['favorite']))

    def test_FallbackToLocalSNOnRemoteTransportFails(self):

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
        home_volume = self.start_client([User])
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
                coroutine.sleep(.5)
                yield 'emote"'

        node_pid = self.fork_master([User], NodeRoutes)
        ipc.get(cmd='inline')
        self.wait_for_events(ipc, event='inline', state='online').wait()

        ts = time.time()
        self.assertEqual('remote', ipc.get(cmd='sleep'))
        self.assertEqual('remote\n' * 66, ipc.get(cmd='yield_raw_and_sleep'))
        self.assertEqual('remote', ipc.get(cmd='yield_json_and_sleep'))
        assert time.time() - ts >= 1.5

        def kill():
            coroutine.sleep(.25)
            self.waitpid(node_pid)

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='sleep'))
        assert not ipc.get(cmd='inline')

        node_pid = self.fork_master([User], NodeRoutes)
        ipc.get(cmd='inline')
        self.wait_for_events(ipc, event='inline', state='online').wait()

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='yield_raw_and_sleep'))
        assert not ipc.get(cmd='inline')

        node_pid = self.fork_master([User], NodeRoutes)
        ipc.get(cmd='inline')
        self.wait_for_events(ipc, event='inline', state='online').wait()

        coroutine.spawn(kill)
        self.assertEqual('local', ipc.get(cmd='yield_json_and_sleep'))
        assert not ipc.get(cmd='inline')

    def test_ReconnectOnServerFall(self):
        routes._RECONNECT_TIMEOUT = 1

        node_pid = self.fork_master([User])
        self.start_client([User])
        ipc = IPCConnection()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        def shutdown():
            coroutine.sleep(.1)
            self.waitpid(node_pid)
        coroutine.spawn(shutdown)
        self.wait_for_events(ipc, event='inline', state='offline').wait()

        self.fork_master([User])
        self.wait_for_events(ipc, event='inline', state='online').wait()


if __name__ == '__main__':
    tests.main()
