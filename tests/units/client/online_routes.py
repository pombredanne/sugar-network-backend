#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
import copy
import shutil
import zipfile
from zipfile import ZipFile
from cStringIO import StringIO
from os.path import exists, lexists, basename

from __init__ import tests, src_root

from sugar_network import client, db, model
from sugar_network.client import IPCConnection, journal, routes, implementations
from sugar_network.toolkit import coroutine, http
from sugar_network.toolkit.spec import Spec
from sugar_network.client.routes import ClientRoutes, Request, Response
from sugar_network.node.master import MasterRoutes
from sugar_network.db import Volume, Resource
from sugar_network.model.user import User
from sugar_network.model.report import Report
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.model.artifact import Artifact
from sugar_network.toolkit.router import route
from sugar_network.toolkit import Option

import requests


class OnlineRoutes(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        self.override(implementations, '_activity_id_new', lambda: 'activity_id')

    def test_whoami(self):
        self.start_online_client()
        ipc = IPCConnection()

        self.assertEqual(
                {'guid': tests.UID, 'roles': []},
                ipc.get(cmd='whoami'))

    def test_Events(self):
        local_volume = self.start_online_client()
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
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
        coroutine.sleep(.1)
        ipc.delete(['context', guid])
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)
        del events[:]

        guid = self.node_volume['context'].create({
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.node_volume['context'].update(guid, {
            'title': 'title_2',
            })
        coroutine.sleep(.1)
        self.node_volume['context'].delete(guid)
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)
        del events[:]

        guid = local_volume['context'].create({
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        local_volume['context'].update(guid, {
            'title': 'title_2',
            })
        coroutine.sleep(.1)
        local_volume['context'].delete(guid)
        coroutine.sleep(.1)

        self.assertEqual([], events)

        self.node.stop()
        coroutine.sleep(.1)
        del events[:]

        guid = local_volume['context'].create({
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        local_volume['context'].update(guid, {
            'title': 'title_2',
            })
        coroutine.sleep(.1)
        local_volume['context'].delete(guid)
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)
        del events[:]

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
            'implementations': [
                {
                    'version': '1',
                    'arch': '*-*',
                    'stability': 'stable',
                    'guid': impl1,
                    'license': ['GPLv3+'],
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
                    'license': ['GPLv3+'],
                    },
                ],
            },
            ipc.get(['context', context], cmd='feed'))

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

    def test_favorite(self):
        local = self.start_online_client()
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        coroutine.spawn(read_events)
        coroutine.dispatch()

        context1 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            'layer': ['foo'],
            })
        context2 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            'layer': ['foo'],
            })

        self.assertEqual(
                sorted([]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='foo')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['foo']}, {'guid': context2, 'layer': ['foo']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['foo']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['foo'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['foo']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['foo'], ipc.get(['context', context2, 'layer']))
        self.assertEqual(
                sorted([]),
                sorted([i['layer'] for i in local['context'].find(reply='layer')[0]]))

        del events[:]
        ipc.put(['context', context1], True, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': context1, 'resource': 'context', 'event': 'update'},
            ],
            events)
        self.assertEqual(
                sorted([{'guid': context1}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='foo')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['foo', 'favorite']}, {'guid': context2, 'layer': ['foo']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['foo', 'favorite']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['foo', 'favorite'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['foo']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['foo'], ipc.get(['context', context2, 'layer']))
        self.assertEqual(
                sorted([['foo', 'favorite']]),
                sorted([i['layer'] for i in local['context'].find(reply='layer')[0]]))

        del events[:]
        ipc.put(['context', context2], True, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': context2, 'resource': 'context', 'event': 'update'},
            ],
            events)
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='foo')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['foo', 'favorite']}, {'guid': context2, 'layer': ['foo', 'favorite']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['foo', 'favorite']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['foo', 'favorite'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['foo', 'favorite']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['foo', 'favorite'], ipc.get(['context', context2, 'layer']))
        self.assertEqual(
                sorted([(context1, ['foo', 'favorite']), (context2, ['foo', 'favorite'])]),
                sorted([(i.guid, i['layer']) for i in local['context'].find(reply='layer')[0]]))

        del events[:]
        ipc.put(['context', context1], False, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual([
            {'guid': context1, 'resource': 'context', 'event': 'update'},
            ],
            events)
        self.assertEqual(
                sorted([{'guid': context2}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='foo')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['foo']}, {'guid': context2, 'layer': ['foo', 'favorite']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['foo']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['foo'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['foo', 'favorite']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['foo', 'favorite'], ipc.get(['context', context2, 'layer']))
        self.assertEqual(
                sorted([(context1, ['foo']), (context2, ['foo', 'favorite'])]),
                sorted([(i.guid, i['layer']) for i in local['context'].find(reply='layer')[0]]))

    def test_clone_Fails(self):
        self.start_online_client([User, Context, Implementation])
        conn = IPCConnection()

        self.assertEqual([
            {'event': 'failure', 'exception': 'NotFound', 'error': "Resource 'foo' does not exist in 'context'"},
            ],
            [i for i in conn.put(['context', 'foo'], True, cmd='clone')])

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual([
            {'event': 'failure', 'exception': 'NotFound',
                'stability': ['stable'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                'error': """\
Can't find all required implementations:
- %s -> (problem)
    No known implementations at all""" % context},
            ],
            [i for i in conn.put(['context', context], True, cmd='clone')])

        assert not exists('solutions/%s/%s' % (context[:2], context))

        impl = conn.post(['implementation'], {
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
                            'exec': 'echo',
                            },
                        },
                    },
                },
            }})

        self.assertEqual([
            {'event': 'failure', 'exception': 'NotFound', 'error': 'BLOB does not exist',
                'stability': ['stable'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                'solution': [{
                    'command': ['echo'],
                    'context': context,
                    'guid': impl,
                    'license': ['GPLv3+'],
                    'stability': 'stable',
                    'version': '1',
                    'path': tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl),
                    }],
                },
            ],
            [i for i in conn.put(['context', context], True, cmd='clone')])
        assert not exists('solutions/%s/%s' % (context[:2], context))

    def test_clone_Content(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        coroutine.spawn(read_events)
        coroutine.dispatch()

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
        blob = 'content'
        self.node_volume['implementation'].update(impl, {'data': {'blob': StringIO(blob), 'foo': 'bar'}})
        clone_path = 'client/context/%s/%s/.clone' % (context[:2], context)

        self.assertEqual([
            {'event': 'ready'},
            ],
            [i for i in ipc.put(['context', context], True, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': context,
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': context}]),
                sorted(ipc.get(['context'], layer='clone')['result']))
        self.assertEqual(
                sorted([{'guid': context}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context, 'layer': ['clone']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['clone']}, ipc.get(['context', context], reply='layer'))
        self.assertEqual(['clone'], ipc.get(['context', context, 'layer']))
        self.assertEqual(
                [(context, ['clone'])],
                [(i.guid, i['layer']) for i in local['context'].find(reply='layer')[0]])
        self.assertEqual({
            'layer': ['clone'],
            'type': ['content'],
            'author': {tests.UID: {'role': 3, 'name': 'test', 'order': 0}},
            'title': {'en-us': 'title'},
            },
            local['context'].get(context).properties(['layer', 'type', 'author', 'title']))
        self.assertEqual({
            'context': context,
            'license': ['GPLv3+'],
            'version': '1',
            'stability': 'stable',
            },
            local['implementation'].get(impl).properties(['context', 'license', 'version', 'stability']))
        blob_path = 'client/implementation/%s/%s/data.blob' % (impl[:2], impl)
        self.assertEqual({
            'seqno': 5,
            'blob_size': len(blob),
            'blob': tests.tmpdir + '/' + blob_path,
            'mtime': int(os.stat(blob_path[:-5]).st_mtime),
            'foo': 'bar',
            },
            local['implementation'].get(impl).meta('data'))
        self.assertEqual('content', file(blob_path).read())
        assert exists(clone_path + '/data.blob')
        assert not exists('solutions/%s/%s' % (context[:2], context))

        self.assertEqual([
            ],
            [i for i in ipc.put(['context', context], False, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': context,
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': context, 'layer': []}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': []}, ipc.get(['context', context], reply='layer'))
        self.assertEqual([], ipc.get(['context', context, 'layer']))
        self.assertEqual({
            'layer': [],
            'type': ['content'],
            'author': {tests.UID: {'role': 3, 'name': 'test', 'order': 0}},
            'title': {'en-us': 'title'},
            },
            local['context'].get(context).properties(['layer', 'type', 'author', 'title']))
        blob_path = 'client/implementation/%s/%s/data.blob' % (impl[:2], impl)
        self.assertEqual({
            'seqno': 5,
            'blob_size': len(blob),
            'blob': tests.tmpdir + '/' + blob_path,
            'mtime': int(os.stat(blob_path[:-5]).st_mtime),
            'foo': 'bar',
            },
            local['implementation'].get(impl).meta('data'))
        self.assertEqual('content', file(blob_path).read())
        assert not lexists(clone_path)
        assert not exists('solutions/%s/%s' % (context[:2], context))

        self.assertEqual([
            {'event': 'ready'},
            ],
            [i for i in ipc.put(['context', context], True, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': context,
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': context, 'layer': ['clone']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        assert exists(clone_path + '/data.blob')
        assert not exists('solutions/%s/%s' % (context[:2], context))

    def test_clone_Activity(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        coroutine.spawn(read_events)
        coroutine.dispatch()

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ])
        blob = self.zips(['TestActivity/activity/activity.info', activity_info])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)
        clone_path = 'client/context/bu/bundle_id/.clone'
        blob_path = tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl)
        solution = [{
            'guid': impl,
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['true'],
            }]
        downloaded_solution = copy.deepcopy(solution)
        downloaded_solution[0]['path'] = blob_path

        self.assertEqual([
            {'event': 'ready'},
            ],
            [i for i in ipc.put(['context', 'bundle_id'], True, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': 'bundle_id',
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': 'bundle_id'}]),
                sorted(ipc.get(['context'], layer='clone')['result']))
        self.assertEqual(
                sorted([{'guid': 'bundle_id'}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': 'bundle_id', 'layer': ['clone']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['clone']}, ipc.get(['context', 'bundle_id'], reply='layer'))
        self.assertEqual(['clone'], ipc.get(['context', 'bundle_id', 'layer']))
        self.assertEqual({
            'layer': ['clone'],
            'type': ['activity'],
            'author': {tests.UID: {'role': 3, 'name': 'test', 'order': 0}},
            'title': {'en-us': 'TestActivity'},
            },
            local['context'].get('bundle_id').properties(['layer', 'type', 'author', 'title']))
        self.assertEqual({
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'version': '1',
            'stability': 'stable',
            },
            local['implementation'].get(impl).properties(['context', 'license', 'version', 'stability']))
        self.assertEqual({
            'seqno': 5,
            'unpack_size': len(activity_info),
            'blob_size': len(blob),
            'blob': blob_path,
            'mtime': int(os.stat(blob_path[:-5]).st_mtime),
            'mime_type': 'application/vnd.olpc-sugar',
            'spec': {
                '*-*': {
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    },
                },
            },
            local['implementation'].get(impl).meta('data'))
        self.assertEqual(activity_info, file(blob_path + '/activity/activity.info').read())
        assert exists(clone_path + '/data.blob/activity/activity.info')
        self.assertEqual(
                [client.api_url.value, ['stable'], downloaded_solution],
                json.load(file('solutions/bu/bundle_id')))

        self.assertEqual([
            ],
            [i for i in ipc.put(['context', 'bundle_id'], False, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': 'bundle_id',
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': 'bundle_id', 'layer': []}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': []}, ipc.get(['context', 'bundle_id'], reply='layer'))
        self.assertEqual([], ipc.get(['context', 'bundle_id', 'layer']))
        self.assertEqual({
            'layer': [],
            'type': ['activity'],
            'author': {tests.UID: {'role': 3, 'name': 'test', 'order': 0}},
            'title': {'en-us': 'TestActivity'},
            },
            local['context'].get('bundle_id').properties(['layer', 'type', 'author', 'title']))
        self.assertEqual({
            'seqno': 5,
            'unpack_size': len(activity_info),
            'blob_size': len(blob),
            'blob': blob_path,
            'mtime': int(os.stat(blob_path[:-5]).st_mtime),
            'mime_type': 'application/vnd.olpc-sugar',
            'spec': {
                '*-*': {
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    },
                },
            },
            local['implementation'].get(impl).meta('data'))
        self.assertEqual(activity_info, file(blob_path + '/activity/activity.info').read())
        assert not exists(clone_path)
        self.assertEqual(
                [client.api_url.value, ['stable'], downloaded_solution],
                json.load(file('solutions/bu/bundle_id')))

        self.assertEqual([
            {'event': 'ready'},
            ],
            [i for i in ipc.put(['context', 'bundle_id'], True, cmd='clone')])

        self.assertEqual({
            'event': 'update',
            'guid': 'bundle_id',
            'resource': 'context',
            },
            events[-1])
        self.assertEqual(
                sorted([{'guid': 'bundle_id', 'layer': ['clone']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        assert exists(clone_path + '/data.blob/activity/activity.info')
        self.assertEqual(
                [client.api_url.value, ['stable'], downloaded_solution],
                json.load(file('solutions/bu/bundle_id')))

    def test_clone_ActivityWithStabilityPreferences(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()

        activity_info1 = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        blob1 = self.zips(['TestActivity/activity/activity.info', activity_info1])
        impl1 = ipc.upload(['implementation'], StringIO(blob1), cmd='submit', initial=True)

        activity_info2 = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = buggy',
            ])
        blob2 = self.zips(['TestActivity/activity/activity.info', activity_info2])
        impl2 = ipc.upload(['implementation'], StringIO(blob2), cmd='submit', initial=True)

        self.assertEqual(
                'ready',
                [i for i in ipc.put(['context', 'bundle_id'], True, cmd='clone')][-1]['event'])

        coroutine.dispatch()
        self.assertEqual({'layer': ['clone']}, ipc.get(['context', 'bundle_id'], reply='layer'))
        self.assertEqual([impl1], [i.guid for i in local['implementation'].find()[0]])
        self.assertEqual(impl1, basename(os.readlink('client/context/bu/bundle_id/.clone')))

        self.touch(('config', [
            '[stabilities]',
            'bundle_id = buggy stable',
            ]))
        Option.load(['config'])

        self.assertEqual(
                [],
                [i for i in ipc.put(['context', 'bundle_id'], False, cmd='clone')])
        self.assertEqual(
                'ready',
                [i for i in ipc.put(['context', 'bundle_id'], True, cmd='clone')][-1]['event'])

        coroutine.dispatch()
        self.assertEqual({'layer': ['clone']}, ipc.get(['context', 'bundle_id'], reply='layer'))
        self.assertEqual([impl1, impl2], [i.guid for i in local['implementation'].find()[0]])
        self.assertEqual(impl2, basename(os.readlink('client/context/bu/bundle_id/.clone')))

    def test_clone_Head(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        blob = self.zips(['TestActivity/activity/activity.info', activity_info])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)
        blob_path = 'master/implementation/%s/%s/data.blob' % (impl[:2], impl)

        self.assertEqual({
            'guid': impl,
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'context': 'bundle_id',
            'data': {
                'blob_size': len(blob),
                'mime_type': 'application/vnd.olpc-sugar',
                'mtime': int(os.stat(blob_path[:-5]).st_mtime),
                'seqno': 3,
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                'unpack_size': len(activity_info),
                },
            },
            ipc.head(['context', 'bundle_id'], cmd='clone'))

        self.assertEqual(
                'ready',
                [i for i in ipc.put(['context', 'bundle_id'], True, cmd='clone')][-1]['event'])
        blob_path = tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl)

        self.assertEqual({
            'guid': impl,
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'context': 'bundle_id',
            'data': {
                'blob': blob_path,
                'blob_size': len(blob),
                'mime_type': 'application/vnd.olpc-sugar',
                'mtime': int(os.stat(blob_path[:-5]).st_mtime),
                'seqno': 5,
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                'unpack_size': len(activity_info),
                },
            },
            ipc.head(['context', 'bundle_id'], cmd='clone'))

    def test_launch_Activity(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()

        blob = self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)
        coroutine.sleep(.1)

        solution = [{
            'guid': impl,
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['true'],
            }]
        downloaded_solution = copy.deepcopy(solution)
        downloaded_solution[0]['path'] = tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl)
        log_path = tests.tmpdir + '/.sugar/default/logs/bundle_id.log'
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'exit', 'activity_id': 'activity_id'},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], downloaded_solution],
                json.load(file('solutions/bu/bundle_id')))

        blob = self.zips(['TestActivity/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license=Public Domain',
            ]])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit')
        coroutine.sleep(.1)

        shutil.rmtree('solutions')
        solution = [{
            'guid': impl,
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '2',
            'command': ['true'],
            'path': tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl),
            }]
        log_path = tests.tmpdir + '/.sugar/default/logs/bundle_id_1.log'
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'exit', 'activity_id': 'activity_id'},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

        self.node.stop()
        coroutine.sleep(.1)

        log_path = tests.tmpdir + '/.sugar/default/logs/bundle_id_2.log'
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'exit', 'activity_id': 'activity_id'},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

        shutil.rmtree('solutions')
        log_path = tests.tmpdir + '/.sugar/default/logs/bundle_id_3.log'
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'exit', 'activity_id': 'activity_id'},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

    def test_launch_Fails(self):
        local = self.start_online_client([User, Context, Implementation])
        ipc = IPCConnection()

        self.assertEqual([
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'NotFound', 'error': "Resource 'foo' does not exist in 'context'"},
            ],
            [i for i in ipc.get(['context', 'foo'], cmd='launch')])

        ipc.post(['context'], {
            'guid': 'bundle_id',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            {'event': 'launch', 'activity_id': 'activity_id', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'NotFound',
                'stability': ['stable'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                'error': """\
Can't find all required implementations:
- bundle_id -> (problem)
    No known implementations at all"""},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ])
        blob = self.zips(['TestActivity/activity/activity.info', activity_info])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)

        solution = [{
            'guid': impl,
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'command': ['false'],
            'path': tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl),
            }]
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'RuntimeError', 'error': 'Process exited with 1 status',
                'stability': ['stable'],
                'args': ['false', '-b', 'bundle_id', '-a', 'activity_id'],
                'solution': solution,
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    tests.tmpdir + '/.sugar/default/logs/bundle_id.log',
                    ]},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

    def test_InvalidateSolutions(self):
        self.start_online_client()
        ipc = IPCConnection()
        self.assertNotEqual(None, self.client_routes._node_mtime)

        mtime = self.client_routes._node_mtime
        coroutine.sleep(1.1)

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        assert self.client_routes._node_mtime == mtime

        coroutine.sleep(1.1)

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
        assert self.client_routes._node_mtime > mtime

        mtime = self.client_routes._node_mtime
        coroutine.sleep(1.1)

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
        assert self.client_routes._node_mtime > mtime

    def test_NoNeedlessRemoteRequests(self):
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
            })
        self.assertEqual(
                {'title': 'local'},
                ipc.get(['context', guid], reply=['title']))

    def test_RestrictLayers(self):
        self.start_online_client([User, Context, Implementation, Artifact])
        ipc = IPCConnection()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'layer': 'public',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            'layer': 'public',
            })
        self.node_volume['implementation'].update(impl, {'data': {
            'spec': {'*-*': {}},
            }})

        self.assertEqual(
                [{'guid': context, 'layer': ['public']}],
                ipc.get(['context'], reply=['guid', 'layer'])['result'])
        self.assertEqual(
                [],
                ipc.get(['context'], reply=['guid', 'layer'], layer='foo')['result'])
        self.assertEqual(
                [{'guid': context, 'layer': ['public']}],
                ipc.get(['context'], reply=['guid', 'layer'], layer='public')['result'])

        self.assertEqual(
                [{'guid': impl, 'layer': ['public']}],
                ipc.get(['implementation'], reply=['guid', 'layer'])['result'])
        self.assertEqual(
                [],
                ipc.get(['implementation'], reply=['guid', 'layer'], layer='foo')['result'])
        self.assertEqual(
                [{'guid': impl, 'layer': ['public']}],
                ipc.get(['implementation'], reply=['guid', 'layer'], layer='public')['result'])

        self.assertEqual({
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'license': ['GPLv3+'],
                }],
            },
            ipc.get(['context', context], cmd='feed'))
        self.assertEqual({
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed', layer='foo'))
        self.assertEqual({
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'license': ['GPLv3+'],
                }],
            },
            ipc.get(['context', context], cmd='feed', layer='public'))

        client.layers.value = ['foo', 'bar']

        self.assertEqual(
                [],
                ipc.get(['context'], reply=['guid', 'layer'])['result'])
        self.assertEqual(
                [],
                ipc.get(['context'], reply=['guid', 'layer'], layer='foo')['result'])
        self.assertEqual(
                [{'guid': context, 'layer': ['public']}],
                ipc.get(['context'], reply=['guid', 'layer'], layer='public')['result'])

        self.assertEqual(
                [],
                ipc.get(['implementation'], reply=['guid', 'layer'])['result'])
        self.assertEqual(
                [],
                ipc.get(['implementation'], reply=['guid', 'layer'], layer='foo')['result'])
        self.assertEqual(
                [{'guid': impl, 'layer': ['public']}],
                ipc.get(['implementation'], reply=['guid', 'layer'], layer='public')['result'])

        self.assertEqual({
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed'))
        self.assertEqual({
            'implementations': [],
            },
            ipc.get(['context', context], cmd='feed', layer='foo'))
        self.assertEqual({
            'implementations': [{
                'stability': 'stable',
                'guid': impl,
                'arch': '*-*',
                'version': '1',
                'license': ['GPLv3+'],
                }],
            },
            ipc.get(['context', context], cmd='feed', layer='public'))

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
                coroutine.sleep(1)
                yield 'emote"'

        node_pid = self.fork_master([User], NodeRoutes)
        ipc.get(cmd='inline')
        self.wait_for_events(ipc, event='inline', state='online').wait()

        ts = time.time()
        self.assertEqual('remote', ipc.get(cmd='sleep'))
        self.assertEqual('remote\n' * 66, ipc.get(cmd='yield_raw_and_sleep'))
        self.assertEqual('remote', ipc.get(cmd='yield_json_and_sleep'))
        assert time.time() - ts >= 2

        def kill():
            coroutine.sleep(.5)
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

    def test_SilentReconnectOnGatewayErrors(self):

        class Routes(object):

            subscribe_tries = 0

            def __init__(self, *args):
                pass

            @route('GET', cmd='info', mime_type='application/json')
            def info(self):
                return {'resources': {}}

            @route('GET', cmd='subscribe', mime_type='text/event-stream')
            def subscribe(self, request=None, response=None, ping=False, **condition):
                Routes.subscribe_tries += 1
                coroutine.sleep(.1)
                if Routes.subscribe_tries % 2:
                    raise http.BadGateway()
                else:
                    raise http.GatewayTimeout()

        node_pid = self.start_master([User], Routes)
        self.start_client([User])
        ipc = IPCConnection()
        self.wait_for_events(ipc, event='inline', state='online').wait()

        def read_events():
            for event in ipc.subscribe():
                events.append(event)
        events = []
        coroutine.spawn(read_events)

        coroutine.sleep(1)
        self.assertEqual([], events)
        assert Routes.subscribe_tries > 2

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

    def test_SubmitReport(self):
        self.home_volume = self.start_online_client([User, Report])
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

        self.assertEqual({
            'context': 'context',
            'error': 'error',
            },
            ipc.get(['report', guid], reply=['context', 'error']))
        zipfile = ZipFile('master/report/%s/%s/data.blob' % (guid[:2], guid))
        self.assertEqual('content1', zipfile.read('file1'))
        self.assertEqual('content2', zipfile.read('file2'))
        self.assertEqual('content3', zipfile.read('file3'))


if __name__ == '__main__':
    tests.main()
