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
from sugar_network.client import Connection, keyfile, api
from sugar_network.toolkit import http, coroutine
from sugar_network.node.routes import NodeRoutes
from sugar_network.node.master import MasterRoutes
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, ACL, route
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http


class NodeTest(tests.Test):

    def test_HandleDeletes(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        guid_path = 'master/db/context/%s/%s' % (guid[:2], guid)

        assert exists(guid_path)
        self.assertEqual({
            'guid': guid,
            'title': 'title',
            'layer': [],
            },
            this.call(method='GET', path=['context', guid], reply=['guid', 'title', 'layer']))
        self.assertEqual([], volume['context'].get(guid)['layer'])

        def subscribe():
            for event in conn.subscribe():
                events.append(event)
        events = []
        coroutine.spawn(subscribe)
        coroutine.dispatch()

        this.call(method='DELETE', path=['context', guid], principal=tests.UID)
        coroutine.dispatch()
        self.assertRaises(http.NotFound, this.call, method='GET', path=['context', guid], reply=['guid', 'title'])
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
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        guid = this.call(method='POST', path=['user'], principal=tests.UID2, content={
            'name': 'user',
            'pubkey': tests.PUBKEY,
            })
        assert guid is None
        self.assertEqual('user', this.call(method='GET', path=['user', tests.UID, 'name']))

    def test_UnauthorizedCommands(self):

        class Routes(NodeRoutes):

            def __init__(self, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTH)
            def probe1(self, directory):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self, directory):
                pass

        class Document(db.Resource):
            pass

        volume = self.start_master([Document, User], Routes)
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={})

        this.request = Request()
        self.assertRaises(http.Unauthorized, this.call, method='GET', cmd='probe1', path=['document', guid])
        this.request = Request()
        this.call(method='GET', cmd='probe1', path=['document', guid], principal=tests.UID)
        this.request = Request()
        this.call(method='GET', cmd='probe2', path=['document', guid])

    def test_ForbiddenCommands(self):

        class Routes(NodeRoutes):

            def __init__(self, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTHOR)
            def probe1(self):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self):
                pass

        class Document(db.Resource):
            pass

        volume = self.start_master([Document, User], Routes)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={})

        self.assertRaises(http.Forbidden, this.call, method='GET', cmd='probe1', path=['document', guid], principal=tests.UID2)
        this.call(method='GET', cmd='probe1', path=['document', guid], principal=tests.UID)

        this.call(method='GET', cmd='probe2', path=['document', guid], principal=tests.UID2)
        this.call(method='GET', cmd='probe2', path=['document', guid])

    def test_ForbiddenCommandsForUserResource(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        this.call(method='POST', path=['user'], principal=tests.UID2, content={
            'name': 'user1',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', this.call(method='GET', path=['user', tests.UID, 'name']))

        this.request = Request()
        self.assertRaises(http.Unauthorized, this.call, method='PUT', path=['user', tests.UID], content={'name': 'user2'})
        this.request = Request()
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['user', tests.UID], principal=tests.UID2, content={'name': 'user2'})
        this.request = Request()
        this.call(method='PUT', path=['user', tests.UID], principal=tests.UID, content={'name': 'user2'})
        this.request = Request()
        self.assertEqual('user2', this.call(method='GET', path=['user', tests.UID, 'name']))

    def test_authorize_Config(self):
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))

        class Routes(NodeRoutes):

            def __init__(self, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.SUPERUSER)
            def probe(self):
                return 'ok'

        volume = self.start_master([User], Routes)
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'test', 'pubkey': tests.PUBKEY2})

        self.assertRaises(http.Forbidden, this.call, method='PROBE')
        self.assertRaises(http.Forbidden, this.call, method='PROBE', principal=tests.UID2)
        self.assertEqual('ok', this.call(method='PROBE', principal=tests.UID))

    def test_authorize_OnlyAuthros(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.AUTHOR)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={'prop': '1'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['document', guid], content={'prop': '2'}, principal=tests.UID2)
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

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={'prop': '1'})

        this.call(method='PUT', path=['document', guid], content={'prop': '2'}, principal=tests.UID)
        self.assertEqual('2', volume['document'].get(guid)['prop'])

        this.call(method='PUT', path=['document', guid], content={'prop': '3'}, principal=tests.UID2)
        self.assertEqual('3', volume['document'].get(guid)['prop'])

    def test_authorize_LiveConfigUpdates(self):

        class Routes(NodeRoutes):

            def __init__(self, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.SUPERUSER)
            def probe(self):
                pass

        volume = self.start_master([User], Routes)
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.Forbidden, this.call, method='PROBE', principal=tests.UID)
        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))
        this.call(method='PROBE', principal=tests.UID)

    def test_authorize_Anonymous(self):

        class Routes(NodeRoutes):

            def __init__(self, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE1', acl=ACL.AUTH)
            def probe1(self, request):
                pass

            @route('PROBE2', acl=ACL.SUPERUSER)
            def probe2(self, request):
                pass

        volume = self.start_master([User], Routes)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        self.assertRaises(http.Unauthorized, this.call, method='PROBE1')
        self.assertRaises(http.Forbidden, this.call, method='PROBE2')

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            'root = True',
            ]))
        this.call(method='PROBE1')
        this.call(method='PROBE2')

    def test_SetUser(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                [{'guid': tests.UID, 'name': 'user', 'role': 3}],
                this.call(method='GET', path=['context', guid, 'author']))

    def test_find_MaxLimit(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            })

        self.node_routes._find_limit = 3
        self.assertEqual(3, len(this.call(method='GET', path=['context'], limit=1024)['result']))
        self.node_routes._find_limit = 2
        self.assertEqual(2, len(this.call(method='GET', path=['context'], limit=1024)['result']))
        self.node_routes._find_limit = 1
        self.assertEqual(1, len(this.call(method='GET', path=['context'], limit=1024)['result']))

    def test_DeletedDocuments(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            'artefact_icon': '',
            'icon': '',
            'logo': '',
            })

        this.call(method='GET', path=['context', guid])
        self.assertNotEqual([], this.call(method='GET', path=['context'])['result'])

        volume['context'].update(guid, {'layer': ['deleted']})

        self.assertRaises(http.NotFound, this.call, method='GET', path=['context', guid])
        self.assertEqual([], this.call(method='GET', path=['context'])['result'])

    def test_CreateGUID(self):
        # TODO Temporal security hole, see TODO
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'guid': 'foo',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {'guid': 'foo', 'title': 'title'},
                this.call(method='GET', path=['context', 'foo'], reply=['guid', 'title']))

    def test_CreateMalformedGUID(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['context'], principal=tests.UID, content={
            'guid': '!?',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_FailOnExistedGUID(self):
        volume = self.start_master()
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['context'], principal=tests.UID, content={
            'guid': guid,
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_PackagesRoute(self):
        volume = self.start_master()
        client = Connection(auth=http.SugarAuth(keyfile.value))

        self.touch(('master/files/packages/repo/arch/package', 'file'))
        volume.blobs.populate()

        self.assertEqual([], client.get(['packages']))
        self.assertEqual([], client.get(['packages', 'repo']))
        self.assertEqual(['package'], client.get(['packages', 'repo', 'arch']))
        self.assertEqual('file', client.get(['packages', 'repo', 'arch', 'package']))

    def test_PackageUpdatesRoute(self):
        volume = self.start_master()
        ipc = Connection(auth=http.SugarAuth(keyfile.value))

        self.touch('master/files/packages/repo/1', 'master/files/packages/repo/1.1')
        volume.blobs.populate()
        self.touch('master/files/packages/repo/2', 'master/files/packages/repo/2.2')
        volume.blobs.populate()

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
                'seqno': 6,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': announce,
                    'version': [[1], 0],
                    'requires': {},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle).hexdigest()), 'unpack_size': len(activity_info) + len(changelog)}},
                    'commands': {'activity': {'exec': 'true'}},
                    'stability': 'developer',
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

    def test_Solve(self):
        volume = self.start_master()
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))

        activity_unpack = '\n'.join([
            '[Activity]',
            'name = activity',
            'bundle_id = activity',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'requires = dep; package',
            ])
        activity_pack = self.zips(('topdir/activity/activity.info', activity_unpack))
        activity_blob = json.load(conn.request('POST', ['context'], activity_pack, params={'cmd': 'submit', 'initial': True}).raw)

        dep_unpack = '\n'.join([
            '[Activity]',
            'name = dep',
            'bundle_id = dep',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ])
        dep_pack = self.zips(('topdir/activity/activity.info', dep_unpack))
        dep_blob = json.load(conn.request('POST', ['context'], dep_pack, params={'cmd': 'submit', 'initial': True}).raw)

        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', 'package', 'releases', '*'], {'binary': ['package.bin']})

        self.assertEqual({
            'activity': {
                'title': 'activity',
                'blob': activity_blob,
                'command': ['activity', 'true'],
                'version': [[1], 0],
                'size': len(activity_pack),
                'unpack_size': len(activity_unpack),
                },
            'dep': {
                'title': 'dep',
                'blob': dep_blob,
                'version': [[2], 0],
                'size': len(dep_pack),
                'unpack_size': len(dep_unpack),
                },
            'package': {
                'packages': ['package.bin'],
                'version': [],
                },
            },
            conn.get(['context', 'activity'], cmd='solve'))

    def test_SolveWithArguments(self):
        volume = self.start_master()
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))

        activity_unpack = '\n'.join([
            '[Activity]',
            'name = activity',
            'bundle_id = activity',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'stability = developer',
            ])
        activity_pack = self.zips(('topdir/activity/activity.info', activity_unpack))
        activity_blob = json.load(conn.request('POST', ['context'], activity_pack, params={'cmd': 'submit', 'initial': True}).raw)

        activity_fake_blob = json.load(conn.request('POST', ['context'],
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = activity',
                'bundle_id = activity',
                'exec = true',
                'icon = icon',
                'activity_version = 2',
                'license = Public Domain',
                ]))),
            params={'cmd': 'submit'}).raw)

        dep_unpack = '\n'.join([
            '[Activity]',
            'name = dep',
            'bundle_id = dep',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            'stability = developer',
            ])
        dep_pack = self.zips(('topdir/activity/activity.info', dep_unpack))
        dep_blob = json.load(conn.request('POST', ['context'], dep_pack, params={'cmd': 'submit', 'initial': True}).raw)

        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].update('package', {'releases': {
            'resolves': {
                'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['package.bin']},
                'Ubuntu-12.04': {'version': [[2], 0], 'packages': ['package-fake.bin']},
            }}})

        self.assertEqual({
            'activity': {
                'title': 'activity',
                'blob': activity_blob,
                'command': ['activity', 'true'],
                'version': [[1], 0],
                'size': len(activity_pack),
                'unpack_size': len(activity_unpack),
                },
            'dep': {
                'title': 'dep',
                'blob': dep_blob,
                'version': [[2], 0],
                'size': len(dep_pack),
                'unpack_size': len(dep_unpack),
                },
            'package': {
                'packages': ['package.bin'],
                'version': [[1], 0],
                },
            },
            conn.get(['context', 'activity'], cmd='solve',
                stability='developer', lsb_id='Ubuntu', lsb_release='10.04', requires=['dep', 'package']))

    def test_Clone(self):
        volume = self.start_master()
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))

        activity_info = '\n'.join([
            '[Activity]',
            'name = activity',
            'bundle_id = activity',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            'requires = dep; package',
            ])
        activity_blob = self.zips(('topdir/activity/activity.info', activity_info))
        activity_file = json.load(conn.request('POST', ['context'], activity_blob, params={'cmd': 'submit', 'initial': True}).raw)
        dep_file = json.load(conn.request('POST', ['context'],
            self.zips(('topdir/activity/activity.info', '\n'.join([
                '[Activity]',
                'name = dep',
                'bundle_id = dep',
                'exec = true',
                'icon = icon',
                'activity_version = 2',
                'license = Public Domain',
                ]))),
            params={'cmd': 'submit', 'initial': True}).raw)
        this.call(method='POST', path=['context'], principal=tests.UID, content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', 'package', 'releases', '*'], {'binary': ['package.bin']})

        response = Response()
        reply = conn.call(Request(method='GET', path=['context', 'activity'], cmd='clone'), response)
        assert activity_blob == reply.read()

    def test_AggpropInsertAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = self.start_master([Document, User])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={})
        self.override(time, 'time', lambda: 0)

        agg1 = this.call(method='POST', path=['document', guid, 'prop1'], principal=tests.UID)
        agg2 = this.call(method='POST', path=['document', guid, 'prop1'], principal=tests.UID2)
        self.assertEqual({
            agg1: {'seqno': 4, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            agg2: {'seqno': 5, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}, 'value': None},
            },
            this.call(method='GET', path=['document', guid, 'prop1']))

        agg3 = this.call(method='POST', path=['document', guid, 'prop2'], principal=tests.UID)
        self.assertRaises(http. Forbidden, this.call, method='POST', path=['document', guid, 'prop2'], principal=tests.UID2)
        self.assertEqual({
            agg3: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            },
            this.call(method='GET', path=['document', guid, 'prop2']))

    def test_AggpropRemoveAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = self.start_master([Document, User])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], principal=tests.UID, content={})
        self.override(time, 'time', lambda: 0)

        agg1 = this.call(method='POST', path=['document', guid, 'prop1'], principal=tests.UID, content=True)
        agg2 = this.call(method='POST', path=['document', guid, 'prop1'], principal=tests.UID2, content=True)
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            this.call(method='GET', path=['document', guid, 'prop1']))
        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop1', agg1], principal=tests.UID2)
        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop1', agg2], principal=tests.UID)
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            this.call(method='GET', path=['document', guid, 'prop1']))

        this.call(method='DELETE', path=['document', guid, 'prop1', agg1], principal=tests.UID)
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            this.call(method='GET', path=['document', guid, 'prop1']))
        this.call(method='DELETE', path=['document', guid, 'prop1', agg2], principal=tests.UID2)
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 7, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            this.call(method='GET', path=['document', guid, 'prop1']))

        agg3 = this.call(method='POST', path=['document', guid, 'prop2'], principal=tests.UID, content=True)
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            this.call(method='GET', path=['document', guid, 'prop2']))

        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop2', agg3], principal=tests.UID2)
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            this.call(method='GET', path=['document', guid, 'prop2']))
        this.call(method='DELETE', path=['document', guid, 'prop2', agg3], principal=tests.UID)
        self.assertEqual({
            agg3: {'seqno': 9, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            this.call(method='GET', path=['document', guid, 'prop2']))


if __name__ == '__main__':
    tests.main()
