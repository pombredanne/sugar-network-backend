#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
from email.utils import formatdate, parsedate
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, node, model
from sugar_network.client import Client
from sugar_network.toolkit import http, coroutine
from sugar_network.toolkit.rrd import Rrd
from sugar_network.node import stats_user, stats_node, obs
from sugar_network.node.routes import NodeRoutes
from sugar_network.node.master import MasterRoutes
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.implementation import Implementation
from sugar_network.model.review import Review
from sugar_network.model.feedback import Feedback
from sugar_network.model.artifact import Artifact
from sugar_network.model.solution import Solution
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, Blob, ACL, route


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        node.stats_root.value = 'stats'
        stats_user.stats_user_step.value = 1
        stats_user.stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']

    def test_UserStats(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume)

        call(cp, method='POST', document='user', principal=tests.UID, content={
            'name': 'user',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })

        ts = int(time.time())

        self.assertEqual({
            'enable': True,
            'status': {},
            'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
            'step': stats_user.stats_user_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

        call(cp, method='POST', cmd='stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
            'name': 'test',
            'values': [(ts + 1, {'field': '1'})],
            })

        self.assertEqual({
            'enable': True, 'status': {
                'test': ts + 2,
                },
            'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
            'step': stats_user.stats_user_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

        call(cp, method='POST', cmd='stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
            'name': 'test',
            'values': [(ts + 2, {'field': '2'})],
            })

        self.assertEqual({
            'enable': True, 'status': {
                'test': ts + 3,
                },
            'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
            'step': stats_user.stats_user_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

        call(cp, method='POST', cmd='stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
            'name': 'test2',
            'values': [(ts + 3, {'field': '3'})],
            })

        self.assertEqual({
            'enable': True, 'status': {
                'test': ts + 3,
                'test2': ts + 4,
                },
            'rras': ['RRA:AVERAGE:0.5:1:4320', 'RRA:AVERAGE:0.5:5:2016'],
            'step': stats_user.stats_user_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

    def test_NodeStats(self):
        stats_node.stats_node.value = True
        stats_node.stats_node_step.value = 1
        rrd = Rrd('stats/node', stats_node.stats_node_step.value, stats_node.stats_node_rras.value)

        ts = int(time.time()) / 3 * 3
        for i in range(100):
            rrd['user'].put({'total': i}, ts + i)

        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume)

        self.assertEqual({
            'user': [
                (ts + 0, {'total': 0.0}),
                (ts + 1, {'total': 1.0}),
                (ts + 2, {'total': 2.0}),
                (ts + 3, {'total': 3.0}),
                ],
            },
            cp.stats(ts, ts + 3, 1, ['user.total']))

        self.assertEqual({
            'user': [
                (ts + 3, {'total': 2.0}),
                (ts + 6, {'total': 5.0}),
                (ts + 9, {'total': 8.0}),
                (ts + 12, {'total': 11.0}),
                ],
            },
            cp.stats(ts, ts + 12, 3, ['user.total']))

    def test_HandleDeletes(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        guid_path = 'db/context/%s/%s' % (guid[:2], guid)

        assert exists(guid_path)
        self.assertEqual({
            'guid': guid,
            'title': 'title',
            'layer': ['public'],
            },
            call(cp, method='GET', document='context', guid=guid, reply=['guid', 'title', 'layer']))
        self.assertEqual(['public'], volume['context'].get(guid)['layer'])

        def subscribe():
            for event in cp.subscribe():
                events.append(json.loads(event[6:]))
        events = []
        coroutine.spawn(subscribe)
        coroutine.dispatch()

        call(cp, method='DELETE', document='context', guid=guid, principal=tests.UID)
        coroutine.dispatch()
        self.assertRaises(http.NotFound, call, cp, method='GET', document='context', guid=guid, reply=['guid', 'title'])
        self.assertEqual(['deleted'], volume['context'].get(guid)['layer'])
        self.assertEqual({'event': 'delete', 'resource': 'context', 'guid': guid}, events[0])

    def test_SimulateDeleteEvents(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        def subscribe():
            for event in cp.subscribe():
                events.append(json.loads(event[6:]))
        events = []
        coroutine.spawn(subscribe)
        coroutine.dispatch()

        call(cp, method='PUT', document='context', guid=guid, principal=tests.UID, content={'layer': ['deleted']})
        coroutine.dispatch()
        self.assertEqual({'event': 'delete', 'resource': 'context', 'guid': guid}, events[0])

    def test_RegisterUser(self):
        cp = NodeRoutes('guid', db.Volume('db', [User]))

        guid = call(cp, method='POST', document='user', principal='fake', content={
            'name': 'user',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })
        assert guid == tests.UID
        self.assertEqual('user', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

    def test_UnauthorizedCommands(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})

        class Routes(NodeRoutes):

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTH)
            def probe1(self, directory):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self, directory):
                pass

        class Document(db.Resource):
            pass

        cp = Routes('guid', db.Volume('db', [User, Document]))
        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})

        self.assertRaises(http.Unauthorized, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal=tests.UID)
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommands(self):

        class Routes(NodeRoutes):

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTHOR)
            def probe1(self):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self):
                pass

        class Document(db.Resource):
            pass

        volume = db.Volume('db', [User, Document])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = Routes('guid', volume)

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})

        self.assertRaises(http.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        self.assertRaises(http.Unauthorized, call, cp, method='GET', cmd='probe1', document='document', guid=guid, principal='fake')
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal=tests.UID)
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommandsForUserResource(self):
        cp = NodeRoutes('guid', db.Volume('db', [User]))

        call(cp, method='POST', document='user', principal='fake', content={
            'name': 'user1',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

        self.assertRaises(http.Unauthorized, call, cp, method='PUT', document='user', guid=tests.UID, content={'name': 'user2'})
        self.assertRaises(http.Unauthorized, call, cp, method='PUT', document='user', guid=tests.UID, principal='fake', content={'name': 'user2'})
        call(cp, method='PUT', document='user', guid=tests.UID, principal=tests.UID, content={'name': 'user2'})
        self.assertEqual('user2', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

    def test_authorize_Config(self):
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))

        class Routes(NodeRoutes):

            @route('PROBE', acl=ACL.SUPERUSER)
            def probe(self):
                return 'ok'

        volume = db.Volume('db', [User])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'test', 'color': '', 'pubkey': tests.PUBKEY2})
        cp = Routes('guid', volume)

        self.assertRaises(http.Forbidden, call, cp, method='PROBE')
        self.assertRaises(http.Forbidden, call, cp, method='PROBE', principal=tests.UID2)
        self.assertEqual('ok', call(cp, method='PROBE', principal=tests.UID))

    def test_authorize_FullWriteForRoot(self):
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID2,
            'root = True',
            ]))

        class Routes(NodeRoutes):

            @route('PROBE', [None, None], acl=ACL.AUTHOR)
            def probe(self):
                pass

        class Document(db.Resource):
            pass

        volume = db.Volume('db', [User, Document])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY2})
        cp = Routes('guid', volume)

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})

        call(cp, 'PROBE', document='document', guid=guid, principal=tests.UID)
        call(cp, 'PROBE', document='document', guid=guid, principal=tests.UID2)

    def test_authorize_LiveConfigUpdates(self):

        class Routes(NodeRoutes):

            @route('PROBE', acl=ACL.SUPERUSER)
            def probe(self):
                pass

        volume = db.Volume('db', [User])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = Routes('guid', volume)

        self.assertRaises(http.Forbidden, call, cp, 'PROBE', principal=tests.UID)
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))
        call(cp, 'PROBE', principal=tests.UID)

    def test_authorize_Anonymous(self):

        class Routes(NodeRoutes):

            @route('PROBE1', acl=ACL.AUTH)
            def probe1(self, request):
                pass

            @route('PROBE2', acl=ACL.SUPERUSER)
            def probe2(self, request):
                pass

        volume = db.Volume('db', [User])
        cp = Routes('guid', volume)

        self.assertRaises(http.Unauthorized, call, cp, 'PROBE1')
        self.assertRaises(http.Forbidden, call, cp, 'PROBE2')

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            'root = True',
            ]))
        call(cp, 'PROBE1')
        call(cp, 'PROBE2')

    def test_SetUser(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                [{'guid': tests.UID, 'name': 'user', 'role': 3}],
                call(cp, method='GET', document='context', guid=guid, prop='author'))

    def test_find_MaxLimit(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)

        call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            })

        node.find_limit.value = 3
        self.assertEqual(3, len(call(cp, method='GET', document='context', limit=1024)['result']))
        node.find_limit.value = 2
        self.assertEqual(2, len(call(cp, method='GET', document='context', limit=1024)['result']))
        node.find_limit.value = 1
        self.assertEqual(1, len(call(cp, method='GET', document='context', limit=1024)['result']))

    def test_DeletedDocuments(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })

        call(cp, method='GET', document='context', guid=guid)
        self.assertNotEqual([], call(cp, method='GET', document='context')['result'])

        volume['context'].update(guid, {'layer': ['deleted']})

        self.assertRaises(http.NotFound, call, cp, method='GET', document='context', guid=guid)
        self.assertEqual([], call(cp, method='GET', document='context')['result'])

    def test_CreateGUID(self):
        # TODO Temporal security hole, see TODO
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = NodeRoutes('guid', volume)
        call(cp, method='POST', document='context', principal=tests.UID, content={
            'guid': 'foo',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {'guid': 'foo', 'title': 'title'},
                call(cp, method='GET', document='context', guid='foo', reply=['guid', 'title']))

    def test_CreateMalformedGUID(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = MasterRoutes('guid', volume)

        self.assertRaises(RuntimeError, call, cp, method='POST', document='context', principal=tests.UID, content={
            'guid': '!?',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_FailOnExistedGUID(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'color': '', 'pubkey': tests.PUBKEY})
        cp = MasterRoutes('guid', volume)

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, call, cp, method='POST', document='context', principal=tests.UID, content={
            'guid': guid,
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_PackagesRoute(self):
        node.files_root.value = '.'
        self.touch(('packages/repo/arch/package', 'file'))
        volume = self.start_master()
        client = Client()

        self.assertEqual(['repo'], client.get(['packages']))
        self.assertEqual(['arch'], client.get(['packages', 'repo']))
        self.assertEqual(['package'], client.get(['packages', 'repo', 'arch']))
        self.assertEqual('file', client.get(['packages', 'repo', 'arch', 'package']))

    def test_PackageUpdatesRoute(self):
        node.files_root.value = '.'
        self.touch(
                ('packages/repo/1', '', 1),
                ('packages/repo/1.1', '', 1),
                ('packages/repo/2', '', 2),
                ('packages/repo/2.2', '', 2),
                )
        volume = self.start_master()
        ipc = Client()

        self.assertEqual(
                sorted(['1', '2']),
                sorted(ipc.get(['packages', 'repo', 'updates'])))

        response = ipc.request('GET', ['packages', 'repo', 'updates'], headers={'if-modified-since': formatdate(0)})
        self.assertEqual(
                sorted(['1', '2']),
                sorted(json.loads(response.content)))
        self.assertEqual(2, time.mktime(parsedate(response.headers['last-modified'])))

        response = ipc.request('GET', ['packages', 'repo', 'updates'], headers={'if-modified-since': formatdate(1)})
        self.assertEqual(
                sorted(['2']),
                sorted(json.loads(response.content)))
        self.assertEqual(2, time.mktime(parsedate(response.headers['last-modified'])))

        response = ipc.request('GET', ['packages', 'repo', 'updates'], headers={'if-modified-since': formatdate(2)})
        self.assertEqual(
                sorted([]),
                sorted(json.loads(response.content)))
        assert 'last-modified' not in response.headers

        response = ipc.request('GET', ['packages', 'repo', 'updates'], headers={'if-modified-since': formatdate(3)})
        self.assertEqual(
                sorted([]),
                sorted(json.loads(response.content)))
        assert 'last-modified' not in response.headers

    def test_Clone(self):
        volume = self.start_master()
        client = Client()

        context = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        blob1 = self.zips(('topdir/probe', 'probe1'))
        volume['implementation'].update(impl, {'data': {
            'blob': StringIO(blob1),
            'spec': {
                '*-*': {
                    'requires': {
                        'dep1': {},
                        },
                    },
                },
            }})
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        blob2 = self.zips(('topdir/probe', 'probe2'))
        volume['implementation'].update(impl, {'data': {
            'blob': StringIO(blob2),
            'spec': {
                '*-*': {
                    'requires': {
                        'dep2': {'restrictions': [[None, '2']]},
                        'dep3': {},
                        },
                    },
                },
            }})
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '3',
            'stability': 'stable',
            'notes': '',
            })
        blob3 = self.zips(('topdir/probe', 'probe3'))
        volume['implementation'].update(impl, {'data': {
            'blob': StringIO(blob3),
            'spec': {
                '*-*': {
                    'requires': {
                        'dep2': {'restrictions': [['2', None]]},
                        },
                    },
                },
            }})
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '4',
            'stability': 'developer',
            'notes': '',
            })
        blob4 = self.zips(('topdir/probe', 'probe4'))
        volume['implementation'].update(impl, {'data': {
            'blob': StringIO(blob4),
            'spec': {
                '*-*': {
                    'requires': {},
                    },
                },
            }})

        self.assertEqual(blob3, client.get(['context', context], cmd='clone'))
        self.assertEqual(blob4, client.get(['context', context], cmd='clone', stability='developer'))
        self.assertEqual(blob1, client.get(['context', context], cmd='clone', version='1'))

        self.assertEqual(blob1, client.get(['context', context], cmd='clone', requires='dep1'))
        self.assertEqual(blob3, client.get(['context', context], cmd='clone', requires='dep2'))
        self.assertEqual(blob2, client.get(['context', context], cmd='clone', requires='dep2=1'))
        self.assertEqual(blob3, client.get(['context', context], cmd='clone', requires='dep2=2'))
        self.assertEqual(blob2, client.get(['context', context], cmd='clone', requires='dep3'))

        self.assertRaises(http.NotFound, client.get, ['context', context], cmd='clone', requires='dep4')
        self.assertRaises(http.NotFound, client.get, ['context', context], cmd='clone', stability='foo')

    def test_release(self):
        volume = self.start_master()
        conn = Client()

        conn.post(['context'], {
            'guid': 'bundle_id',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            'requires = sugar>=0.88; dep'
            ])
        bundle = self.zips(('topdir/activity/activity.info', activity_info))
        guid = json.load(conn.request('POST', ['implementation'], bundle, params={'cmd': 'release'}).raw)

        impl = volume['implementation'].get(guid)
        self.assertEqual('bundle_id', impl['context'])
        self.assertEqual('1', impl['version'])
        self.assertEqual('developer', impl['stability'])
        self.assertEqual(['Public Domain'], impl['license'])
        self.assertEqual('developer', impl['stability'])

        data = impl.meta('data')
        self.assertEqual('application/vnd.olpc-sugar', data['mime_type'])
        self.assertEqual(len(bundle), data['blob_size'])
        self.assertEqual(len(activity_info), data.get('unpack_size'))


def call(routes, method, document=None, guid=None, prop=None, principal=None, cmd=None, content=None, **kwargs):
    path = []
    if document:
        path.append(document)
    if guid:
        path.append(guid)
    if prop:
        path.append(prop)
    request = Request(method=method, path=path)
    request.update(kwargs)
    request.cmd = cmd
    request.content = content
    request.environ = {'HTTP_HOST': '127.0.0.1'}
    if principal:
        request.environ['HTTP_X_SN_LOGIN'] = principal
    router = Router(routes)
    return router.call(request, Response())


if __name__ == '__main__':
    tests.main()
