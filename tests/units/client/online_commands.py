#!/usr/bin/env python
# sugar-lint: disable

import shutil
import zipfile
from os.path import exists

from __init__ import tests, src_root

from sugar_network import client, db
from sugar_network.client import IPCClient, journal
from sugar_network.zerosugar import clones, injector
from sugar_network.toolkit import coroutine
from sugar_network.toolkit.router import Request, Redirect
from sugar_network.client.commands import ClientCommands
from sugar_network.resources.volume import Volume, Resource
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.artifact import Artifact

import requests


class OnlineCommandsTest(tests.Test):

    def test_inline(self):
        cp = ClientCommands(Volume('client'))
        assert not cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='online')
        self.start_master()
        trigger.wait(1)
        assert trigger.value is None
        assert not cp.inline()

        request = Request(method='GET', cmd='whoami')
        cp.call(request)
        trigger.wait()
        assert cp.inline()

        trigger = self.wait_for_events(cp, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert not cp.inline()

    def test_whoami(self):
        self.start_online_client()
        ipc = IPCClient()

        self.assertEqual(
                {'guid': tests.UID, 'roles': [], 'route': 'proxy'},
                ipc.get(cmd='whoami'))

    def test_clone_Activities(self):
        self.home_volume = self.start_online_client()
        ipc = IPCClient()
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
            })
        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()
        ipc.request('PUT', ['implementation', impl, 'data'], file('bundle', 'rb').read())

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
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 2, cmd='clone')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 1, cmd='clone', force=1)
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        assert exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 0, cmd='clone')
        coroutine.sleep(.5)

        assert not exists('Activities/TestActivitry/activity/activity.info')
        assert not exists('Activities/TestActivitry_1/activity/activity.info')
        self.assertEqual(
                {'clone': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 1, cmd='clone')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        trigger = self.wait_for_events(ipc, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert not ipc.get(cmd='inline')

        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

    def test_clone_ActivityImpl(self):
        self.home_volume = self.start_online_client()
        ipc = IPCClient()
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
            'requires': ['foo'],
            })

        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            'requires': ['bar'],
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
                    'requires': {'dep': {}},
                    },
                },
            })
        bundle = zipfile.ZipFile('bundle', 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()
        ipc.request('PUT', ['implementation', impl2, 'data'], file('bundle', 'rb').read())

        impl3 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '3',
            'stability': 'developer',
            'notes': '',
            'requires': ['bar'],
            })

        assert not exists('Activities/TestActivitry/activity/activity.info')
        self.assertEqual(
                {'clone': 0, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

        ipc.put(['context', context], 2, cmd='clone', nodeps=1, stability='stable', requires='bar')
        coroutine.sleep(.5)

        assert exists('Activities/TestActivitry/activity/activity.info')
        self.assertEqual(
                {'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['clone']))

    def test_clone_Content(self):
        self.start_online_client()
        updates = []

        def journal_update(self, guid, data=None, preview=None, **kwargs):
            if data is not None:
                kwargs['data'] = data.read()
            updates.append((guid, kwargs))

        self.override(journal.Commands, '__init__', lambda *args: None)
        self.override(journal.Commands, 'journal_update', journal_update)
        self.override(journal.Commands, 'journal_delete', lambda self, guid: updates.append((guid,)))

        ipc = IPCClient()

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

        self.override(journal.Commands, '__init__', lambda *args: None)
        self.override(journal.Commands, 'journal_update', journal_update)
        self.override(journal.Commands, 'journal_delete', lambda self, guid: updates.append((guid,)))

        ipc = IPCClient()

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
        ipc = IPCClient()

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
                {'favorite': True, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], False, cmd='favorite')
        self.assertEqual(
                {'favorite': False, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        ipc.put(['context', context], True, cmd='favorite')
        coroutine.sleep(.5)
        self.assertEqual(
                {'favorite': True, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

        trigger = self.wait_for_events(ipc, event='inline', state='offline')
        self.node.stop()
        trigger.wait()
        assert not ipc.get(cmd='inline')

        self.assertEqual(
                {'favorite': True, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite']))

    def test_subscribe(self):
        self.start_online_client()
        ipc = IPCClient()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                if 'props' in event:
                    event.pop('props')
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
        ipc.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'document': 'context', 'event': 'create'},
            {'guid': guid, 'document': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'document': 'context'},
            ],
            events)
        del events[:]

        job = coroutine.spawn(read_events)
        coroutine.dispatch()
        guid = self.node_volume['context'].create({
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.node_volume['context'].update(guid, {
            'title': 'title_2',
            })
        self.node_volume['context'].delete(guid)
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'document': 'context', 'event': 'create'},
            {'guid': guid, 'document': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'document': 'context'},
            ],
            events)

    def test_BLOBs(self):
        self.start_online_client()
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
                {'preview': 'http://localhost:8888/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://localhost:8888/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://localhost:8888/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://localhost:8888/static/images/missing.png'}],
                ipc.get(['context'], reply=['icon'])['result'])

    def test_Feeds(self):
        self.start_online_client()
        ipc = IPCClient()

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
            'spec': {'*-*': {}},
            })
        impl2 = ipc.post(['implementation'], {
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
            ipc.get(['context', context, 'versions']))

    def test_RestrictLayers(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCClient()

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
            'spec': {'*-*': {}},
            })
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

        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                ipc.get(['context', context, 'versions']))
        self.assertEqual(
                [],
                ipc.get(['context', context, 'versions'], layer='foo'))
        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                ipc.get(['context', context, 'versions'], layer='public'))

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

        self.assertEqual(
                [],
                ipc.get(['context', context, 'versions']))
        self.assertEqual(
                [],
                ipc.get(['context', context, 'versions'], layer='foo'))
        self.assertEqual(
                [{'stability': 'stable', 'guid': impl, 'arch': '*-*', 'version': '1'}],
                ipc.get(['context', context, 'versions'], layer='public'))

    def test_InvalidateSolutions(self):
        self.start_online_client()
        ipc = IPCClient()
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
            'spec': {'*-*': {}},
            })
        assert injector._mtime > mtime

        mtime = injector._mtime
        coroutine.sleep(1.5)

        impl2 = ipc.post(['implementation'], {
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
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCClient()

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
                raise Redirect(URL)

        self.start_online_client([User, Document])
        ipc = IPCClient()
        guid = ipc.post(['document'], {})

        response = requests.request('GET', client.api_url.value + '/document/' + guid + '/blob', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual(URL, response.headers['Location'])

    def test_Proxy_Activities(self):
        home_volume = self.start_online_client()
        ipc = IPCClient()

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
                {'favorite': True, 'clone': 2, 'type': ['activity']},
                ipc.get(['context', context], reply=['favorite', 'clone']))

    def test_Proxy_Content(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCClient()

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
        ipc = IPCClient()

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


if __name__ == '__main__':
    tests.main()
