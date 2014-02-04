#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import time
import json
import base64
import shutil
import hashlib
from M2Crypto import RSA
from email.utils import formatdate, parsedate
from cStringIO import StringIO
from os.path import exists, join

from __init__ import tests

from sugar_network import db, node, model, client
from sugar_network.db import files
from sugar_network.client import Connection, keyfile, api_url
from sugar_network.toolkit import http, coroutine
from sugar_network.toolkit.rrd import Rrd
from sugar_network.node import stats_user
from sugar_network.node.routes import NodeRoutes
from sugar_network.node.master import MasterRoutes
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, ACL, route
from sugar_network.toolkit import http


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        node.stats_root.value = 'stats'
        stats_user.stats_user_step.value = 1
        stats_user.stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']

    def test_UserStats(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume=volume)

        call(cp, method='POST', document='user', principal=tests.UID, content={
            'name': 'user',
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

    def test_HandleDeletes(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

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
            'layer': [],
            },
            call(cp, method='GET', document='context', guid=guid, reply=['guid', 'title', 'layer']))
        self.assertEqual([], volume['context'].get(guid)['layer'])

        def subscribe():
            for event in cp.subscribe():
                events.append(event)
        events = []
        coroutine.spawn(subscribe)
        coroutine.dispatch()

        call(cp, method='DELETE', document='context', guid=guid, principal=tests.UID)
        coroutine.dispatch()
        self.assertRaises(http.NotFound, call, cp, method='GET', document='context', guid=guid, reply=['guid', 'title'])
        self.assertEqual(['deleted'], volume['context'].get(guid)['layer'])

    def test_DeletedRestoredHandlers(self):
        trigger = []

        class TestDocument(db.Resource):

            def deleted(self):
                trigger.append(False)

            def restored(self):
                trigger.append(True)

        volume = self.start_master([TestDocument, User])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid = conn.post(['testdocument'], {})
        self.assertEqual([], trigger)

        conn.put(['testdocument', guid, 'layer'], ['deleted'])
        self.assertEqual([False], trigger)

        conn.put(['testdocument', guid, 'layer'], [])
        self.assertEqual([False, True], trigger)

        conn.put(['testdocument', guid, 'layer'], ['bar'])
        self.assertEqual([False, True], trigger)

        conn.put(['testdocument', guid, 'layer'], ['deleted'])
        self.assertEqual([False, True, False], trigger)

        conn.put(['testdocument', guid, 'layer'], ['deleted', 'foo'])
        self.assertEqual([False, True, False], trigger)

    def test_RegisterUser(self):
        cp = NodeRoutes('guid', volume=db.Volume('db', [User]))

        guid = call(cp, method='POST', document='user', principal=tests.UID2, content={
            'name': 'user',
            'pubkey': tests.PUBKEY,
            })
        assert guid is None
        self.assertEqual('user', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

    def test_UnauthorizedCommands(self):
        volume = db.Volume('db', model.RESOURCES)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        class Routes(NodeRoutes):

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTH)
            def probe1(self, directory):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self, directory):
                pass

        class Document(db.Resource):
            pass

        cp = Routes('guid', volume=db.Volume('db', [User, Document]))
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
        cp = Routes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})

        self.assertRaises(http.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        self.assertRaises(http.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid, principal=tests.UID2)
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal=tests.UID)
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommandsForUserResource(self):
        cp = NodeRoutes('guid', volume=db.Volume('db', [User]))

        call(cp, method='POST', document='user', principal=tests.UID2, content={
            'name': 'user1',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

        self.assertRaises(http.Unauthorized, call, cp, method='PUT', document='user', guid=tests.UID, content={'name': 'user2'})
        self.assertRaises(http.Forbidden, call, cp, method='PUT', document='user', guid=tests.UID, principal=tests.UID2, content={'name': 'user2'})
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
        cp = Routes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'test', 'pubkey': tests.PUBKEY2})

        self.assertRaises(http.Forbidden, call, cp, method='PROBE')
        self.assertRaises(http.Forbidden, call, cp, method='PROBE', principal=tests.UID2)
        self.assertEqual('ok', call(cp, method='PROBE', principal=tests.UID))

    def test_authorize_OnlyAuthros(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.AUTHOR)
            def prop(self, value):
                return value

        volume = db.Volume('db', [User, Document])
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={'prop': '1'})
        self.assertRaises(http.Forbidden, call, cp, 'PUT', document='document', guid=guid, content={'prop': '2'}, principal=tests.UID2)
        self.assertEqual('1', volume['document'].get(guid)['prop'])

    def test_authorize_FullWriteForRoot(self):
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID2,
            'root = True',
            ]))

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.AUTHOR)
            def prop(self, value):
                return value

        volume = db.Volume('db', [User, Document])
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={'prop': '1'})

        call(cp, 'PUT', document='document', guid=guid, content={'prop': '2'}, principal=tests.UID)
        self.assertEqual('2', volume['document'].get(guid)['prop'])

        call(cp, 'PUT', document='document', guid=guid, content={'prop': '3'}, principal=tests.UID2)
        self.assertEqual('3', volume['document'].get(guid)['prop'])

    def test_authorize_LiveConfigUpdates(self):

        class Routes(NodeRoutes):

            @route('PROBE', acl=ACL.SUPERUSER)
            def probe(self):
                pass

        volume = db.Volume('db', [User])
        cp = Routes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

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
        cp = Routes('guid', volume=volume)

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
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

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
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

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

        cp._find_limit = 3
        self.assertEqual(3, len(call(cp, method='GET', document='context', limit=1024)['result']))
        cp._find_limit = 2
        self.assertEqual(2, len(call(cp, method='GET', document='context', limit=1024)['result']))
        cp._find_limit = 1
        self.assertEqual(1, len(call(cp, method='GET', document='context', limit=1024)['result']))

    def test_DeletedDocuments(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            'artifact_icon': '',
            'icon': '',
            'logo': '',
            })

        call(cp, method='GET', document='context', guid=guid)
        self.assertNotEqual([], call(cp, method='GET', document='context')['result'])

        volume['context'].update(guid, {'layer': ['deleted']})

        self.assertRaises(http.NotFound, call, cp, method='GET', document='context', guid=guid)
        self.assertEqual([], call(cp, method='GET', document='context')['result'])

    def test_CreateGUID(self):
        # TODO Temporal security hole, see TODO
        volume = db.Volume('db', model.RESOURCES)
        cp = NodeRoutes('guid', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
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
        cp = MasterRoutes('guid', volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.BadRequest, call, cp, method='POST', document='context', principal=tests.UID, content={
            'guid': '!?',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_FailOnExistedGUID(self):
        volume = db.Volume('db', model.RESOURCES)
        cp = MasterRoutes('guid', volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(http.BadRequest, call, cp, method='POST', document='context', principal=tests.UID, content={
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
        client = Connection(auth=http.SugarAuth(keyfile.value))

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
        ipc = Connection(auth=http.SugarAuth(keyfile.value))

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
        client = http.Connection(api_url.value, http.SugarAuth(keyfile.value))

        blob1 = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'requires = dep1',
            'stability = stable',
            ])))
        release1 = json.load(client.request('POST', ['context'], blob1, params={'cmd': 'submit', 'initial': True}).raw)

        blob2 = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'requires = dep2 < 3; dep3',
            'stability = stable',
            ])))
        release2 = json.load(client.request('POST', ['context'], blob2, params={'cmd': 'submit'}).raw)

        blob3 = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 3',
            'license = Public Domain',
            'requires = dep2 >= 2',
            'stability = stable',
            ])))
        release3 = json.load(client.request('POST', ['context'], blob3, params={'cmd': 'submit'}).raw)

        blob4 = self.zips(('topdir/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 4',
            'license = Public Domain',
            'stability = developer',
            ])))
        release4 = json.load(client.request('POST', ['context'], blob4, params={'cmd': 'submit'}).raw)

        assert blob3 == client.get(['context', 'bundle_id'], cmd='clone')
        assert blob4 == client.get(['context', 'bundle_id'], cmd='clone', stability='developer')
        assert blob1 == client.get(['context', 'bundle_id'], cmd='clone', version='1')

        assert blob1 == client.get(['context', 'bundle_id'], cmd='clone', requires='dep1')
        assert blob3 == client.get(['context', 'bundle_id'], cmd='clone', requires='dep2')
        assert blob2 == client.get(['context', 'bundle_id'], cmd='clone', requires='dep2=1')
        assert blob3 == client.get(['context', 'bundle_id'], cmd='clone', requires='dep2=2')
        assert blob2 == client.get(['context', 'bundle_id'], cmd='clone', requires='dep3')

        self.assertRaises(http.NotFound, client.get, ['context', 'bundle_id'], cmd='clone', requires='dep4')
        self.assertRaises(http.NotFound, client.get, ['context', 'bundle_id'], cmd='clone', stability='foo')

        response = Response()
        client.call(Request(method='GET', path=['context', 'bundle_id'], cmd='clone'), response)
        announce = next(volume['post'].find(query='3', limit=1)[0]).guid
        self.assertEqual({
            'license': ['Public Domain'],
            'unpack_size': 162,
            'stability': 'stable',
            'version': '3',
            'release': [[3], 0],
            'announce': announce,
            'requires': ['dep2-2'],
            'spec': {
                '*-*': {
                    'commands': {'activity': {'exec': u'true'}},
                    'requires': {'dep2': {'restrictions': [['2', None]]}},
                    'bundle': str(hash(blob3)),
                    },
                },
            }, response.meta)

    def test_release(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            ])
        changelog = "LOG"
        bundle = self.zips(
                ('topdir/activity/activity.info', activity_info),
                ('topdir/CHANGELOG', changelog),
                )
        release = json.load(conn.request('POST', ['context'], bundle, params={'cmd': 'submit', 'initial': True}).raw)
        announce = next(volume['post'].find(query='1', limit=1)[0]).guid

        self.assertEqual({
            release: {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': announce,
                    'release': [[1], 0],
                    'requires': [],
                    'spec': {'*-*': {'bundle': str(hash(bundle)), 'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                    'stability': 'developer',
                    'unpack_size': len(activity_info) + len(changelog),
                    'version': '1',
                    },
                },
            }, conn.get(['context', 'bundle_id', 'releases']))

        post = volume['post'][announce]
        assert tests.UID in post['author']
        self.assertEqual('notification', post['type'])
        self.assertEqual({
            'en': 'TestActivitry 1 release',
            'es': 'TestActivitry 1 release',
            'fr': 'TestActivitry 1 release',
            }, post['title'])
        self.assertEqual({
            'en-us': 'LOG',
            }, post['message'])

    def test_AggpropInsertAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = db.Volume('db', [Document, User])
        cp = NodeRoutes('node', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})
        self.override(time, 'time', lambda: 0)

        agg1 = call(cp, method='POST', path=['document', guid, 'prop1'], principal=tests.UID)
        agg2 = call(cp, method='POST', path=['document', guid, 'prop1'], principal=tests.UID2)
        self.assertEqual({
            agg1: {'seqno': 4, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            agg2: {'seqno': 5, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}, 'value': None},
            },
            call(cp, method='GET', path=['document', guid, 'prop1']))

        agg3 = call(cp, method='POST', path=['document', guid, 'prop2'], principal=tests.UID)
        self.assertRaises(http. Forbidden, call, cp, method='POST', path=['document', guid, 'prop2'], principal=tests.UID2)
        self.assertEqual({
            agg3: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            },
            call(cp, method='GET', path=['document', guid, 'prop2']))

    def test_AggpropRemoveAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = db.Volume('db', [Document, User])
        cp = NodeRoutes('node', volume=volume)
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = call(cp, method='POST', document='document', principal=tests.UID, content={})
        self.override(time, 'time', lambda: 0)

        agg1 = call(cp, method='POST', path=['document', guid, 'prop1'], principal=tests.UID, content=True)
        agg2 = call(cp, method='POST', path=['document', guid, 'prop1'], principal=tests.UID2, content=True)
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop1']))
        self.assertRaises(http.Forbidden, call, cp, method='DELETE', path=['document', guid, 'prop1', agg1], principal=tests.UID2)
        self.assertRaises(http.Forbidden, call, cp, method='DELETE', path=['document', guid, 'prop1', agg2], principal=tests.UID)
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop1']))

        call(cp, method='DELETE', path=['document', guid, 'prop1', agg1], principal=tests.UID)
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop1']))
        call(cp, method='DELETE', path=['document', guid, 'prop1', agg2], principal=tests.UID2)
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 7, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop1']))

        agg3 = call(cp, method='POST', path=['document', guid, 'prop2'], principal=tests.UID, content=True)
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop2']))

        self.assertRaises(http.Forbidden, call, cp, method='DELETE', path=['document', guid, 'prop2', agg3], principal=tests.UID2)
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop2']))
        call(cp, method='DELETE', path=['document', guid, 'prop2', agg3], principal=tests.UID)
        self.assertEqual({
            agg3: {'seqno': 9, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            call(cp, method='GET', path=['document', guid, 'prop2']))


def call(routes, method, document=None, guid=None, prop=None, principal=None, content=None, path=None, **kwargs):
    if not path:
        path = ['']
        if document:
            path.append(document)
        if guid:
            path.append(guid)
        if prop:
            path.append(prop)
    env = {'REQUEST_METHOD': method,
           'PATH_INFO': '/'.join(path),
           'HTTP_HOST': '127.0.0.1',
           }
    if principal:
        key = RSA.load_key(join(tests.root, 'data', principal))
        nonce = int(time.time()) + 100
        data = hashlib.sha1('%s:%s' % (principal, nonce)).digest()
        signature = key.sign(data).encode('hex')
        env['HTTP_AUTHORIZATION'] = 'Sugar username="%s",nonce="%s",signature="%s"' % (principal, nonce, signature)
    request = Request(env)
    request.update(kwargs)
    request.cmd = kwargs.get('cmd')
    request.content = content
    request.principal = principal
    router = Router(routes)
    return router.call(request, Response())


if __name__ == '__main__':
    tests.main()
