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
from sugar_network.client import Connection
from sugar_network.toolkit import http, coroutine
from sugar_network.node import routes as node_routes
from sugar_network.node.routes import NodeRoutes
from sugar_network.model.context import Context
from sugar_network.node.model import User
from sugar_network.node.auth import Principal
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, ACL, route, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, packets


class NodeRoutesTest(tests.Test):

    def test_RegisterUser(self):
        volume = self.start_master()

        guid = this.call(method='POST', path=['user'], environ=auth_env(tests.UID2), content={
            'name': 'user',
            'pubkey': tests.PUBKEY,
            })
        assert guid is None
        self.assertEqual('user', this.call(method='GET', path=['user', tests.UID, 'name']))

    def test_UnauthorizedCommands(self):

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
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
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={})

        this.request = Request()
        self.assertRaises(http.Unauthorized, this.call, method='GET', cmd='probe1', path=['document', guid])
        this.request = Request()
        this.call(method='GET', cmd='probe1', path=['document', guid], environ=auth_env(tests.UID))
        this.request = Request()
        this.call(method='GET', cmd='probe2', path=['document', guid])

    def test_ForbiddenCommands(self):

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('GET', [None, None], cmd='probe1', acl=ACL.AUTH | ACL.AUTHOR)
            def probe1(self):
                pass

            @route('GET', [None, None], cmd='probe2')
            def probe2(self):
                pass

        class Document(db.Resource):
            pass

        volume = self.start_master([Document, User], Routes)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={})

        self.assertRaises(http.Forbidden, this.call, method='GET', cmd='probe1', path=['document', guid], environ=auth_env(tests.UID2))
        this.call(method='GET', cmd='probe1', path=['document', guid], environ=auth_env(tests.UID))

        this.call(method='GET', cmd='probe2', path=['document', guid], environ=auth_env(tests.UID2))
        this.call(method='GET', cmd='probe2', path=['document', guid])

    def test_ForbiddenCommandsForUserResource(self):
        volume = self.start_master()

        this.call(method='POST', path=['user'], environ=auth_env(tests.UID2), content={
            'name': 'user1',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', this.call(method='GET', path=['user', tests.UID, 'name']))

        this.request = Request()
        self.assertRaises(http.Unauthorized, this.call, method='PUT', path=['user', tests.UID], content={'name': 'user2'})
        this.request = Request()
        self.assertRaises(http.Unauthorized, this.call, method='PUT', path=['user', tests.UID], environ=auth_env(tests.UID2), content={'name': 'user2'})
        this.request = Request()
        this.call(method='PUT', path=['user', tests.UID], environ=auth_env(tests.UID), content={'name': 'user2'})
        this.request = Request()
        self.assertEqual('user2', this.call(method='GET', path=['user', tests.UID, 'name']))

    def test_authorize_Config(self):
        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            '%s = admin' % tests.UID,
            ]))

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.AUTH)
            def probe(self):
                if not this.principal.cap_create_with_guid:
                    raise http.Forbidden()
                return 'ok'

        volume = self.start_master([User], Routes)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'test', 'pubkey': tests.PUBKEY2})

        self.assertRaises(http.Unauthorized, this.call, method='PROBE')
        self.assertRaises(http.Forbidden, this.call, method='PROBE', environ=auth_env(tests.UID2))
        self.assertEqual('ok', this.call(method='PROBE', environ=auth_env(tests.UID)))

    def test_authorize_OnlyAuthros(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.AUTHOR)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={'prop': '1'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['document', guid], content={'prop': '2'}, environ=auth_env(tests.UID2))
        self.assertEqual('1', volume['document'].get(guid)['prop'])

    def test_authorize_FullWriteForRoot(self):
        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            '%s = admin' % tests.UID2,
            ]))

        class Document(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.PUBLIC | ACL.AUTH | ACL.AUTHOR)
            def prop(self, value):
                return value

        volume = self.start_master([User, Document])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={'prop': '1'})

        this.call(method='PUT', path=['document', guid], content={'prop': '2'}, environ=auth_env(tests.UID))
        self.assertEqual('2', volume['document'].get(guid)['prop'])

        this.call(method='PUT', path=['document', guid], content={'prop': '3'}, environ=auth_env(tests.UID2))
        self.assertEqual('3', volume['document'].get(guid)['prop'])

    def test_authorize_LiveConfigUpdates(self):

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.AUTH)
            def probe(self):
                if not this.principal.cap_create_with_guid:
                    raise http.Forbidden()

        volume = self.start_master([User], Routes)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.Forbidden, this.call, method='PROBE', environ=auth_env(tests.UID))
        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            '%s = admin' % tests.UID,
            ]))
        self.node_routes._auth.reload()
        this.call(method='PROBE', environ=auth_env(tests.UID))

    def test_authorize_Anonymous(self):

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.AUTH)
            def probe(self, request):
                pass

        volume = self.start_master([User], Routes)

        self.assertRaises(http.Unauthorized, this.call, method='PROBE')

    def test_authorize_DefaultPermissions(self):

        class Routes(NodeRoutes):

            def __init__(self, master_api, **kwargs):
                NodeRoutes.__init__(self, 'node', **kwargs)

            @route('PROBE', acl=ACL.AUTH)
            def probe(self, request):
                if not this.principal.cap_create_with_guid:
                    raise http.Forbidden()

        volume = self.start_master([User], Routes)
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.Forbidden, this.call, method='PROBE', environ=auth_env(tests.UID))

        self.touch(('master/etc/authorization.conf', [
            '[permissions]',
            'default = admin',
            ]))
        self.node_routes._auth.reload()
        this.call(method='PROBE', environ=auth_env(tests.UID))

    def test_SetUser(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
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
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
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
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
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

        volume['context'].update(guid, {'state': 'deleted'})

        self.assertRaises(http.NotFound, this.call, method='GET', path=['context', guid])
        self.assertEqual([], this.call(method='GET', path=['context'])['result'])

    def test_CreateGUID(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': 'foo',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))
        self.assertEqual(
                {'guid': 'foo', 'title': 'title'},
                this.call(method='GET', path=['context', 'foo'], reply=['guid', 'title']))

    def test_CreateMalformedGUID(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': '!?',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))

    def test_FailOnExistedGUID(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        guid = this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, this.call, method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': guid,
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))

    def test_PackagesRoute(self):
        volume = self.start_master()
        client = Connection()

        self.touch(('master/files/packages/repo/arch/package', 'file'))
        for __ in volume.blobs.populate():
            pass

        self.assertEqual([], client.get(['packages']))
        self.assertEqual([], client.get(['packages', 'repo']))
        self.assertEqual(['package'], client.get(['packages', 'repo', 'arch']))
        self.assertEqual('file', client.get(['packages', 'repo', 'arch', 'package']))

    def test_PackageUpdatesRoute(self):
        volume = self.start_master()
        ipc = Connection()

        self.touch('master/files/packages/repo/1', 'master/files/packages/repo/1.1')
        for __ in volume.blobs.populate():
            pass
        self.touch('master/files/packages/repo/2', 'master/files/packages/repo/2.2')
        for __ in volume.blobs.populate():
            pass

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

    def test_SubmitReleasesViaAggpropsIface(self):
        volume = self.start_master()
        conn = Connection()

        context = conn.post(['context'], {
            'type': 'activity',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })

        activity_info1 = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license = Public Domain',
            ])
        bundle1 = self.zips(('topdir/activity/activity.info', activity_info1))
        release1 = conn.upload(['context', context, 'releases'], StringIO(bundle1))
        assert release1 == str(hashlib.sha1(bundle1).hexdigest())
        self.assertEqual({
            release1: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists

        activity_info2 = '\n'.join([
            '[Activity]',
            'name = Activity',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license = Public Domain',
            ])
        bundle2 = self.zips(('topdir/activity/activity.info', activity_info2))
        release2 = conn.upload(['context', context, 'releases'], StringIO(bundle2))
        assert release2 == str(hashlib.sha1(bundle2).hexdigest())
        self.assertEqual({
            release1: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:1')[0]).guid,
                    'version': [[1], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle1).hexdigest()), 'unpack_size': len(activity_info1)}},
                    'stability': 'stable',
                    },
                },
            release2: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists

        conn.delete(['context', context, 'releases', release1])
        self.assertEqual({
            release1: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            release2: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                'value': {
                    'license': ['Public Domain'],
                    'announce': next(volume['post'].find(query='title:2')[0]).guid,
                    'version': [[2], 0],
                    'requires': {},
                    'commands': {'activity': {'exec': 'true'}},
                    'bundles': {'*-*': {'blob': str(hashlib.sha1(bundle2).hexdigest()), 'unpack_size': len(activity_info2)}},
                    'stability': 'stable',
                    },
                },
            }, volume['context'][context]['releases'])
        assert not volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists

        conn.delete(['context', context, 'releases', release2])
        self.assertEqual({
            release1: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            release2: {
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
                },
            }, volume['context'][context]['releases'])
        assert not volume.blobs.get(str(hashlib.sha1(bundle1).hexdigest())).exists
        assert not volume.blobs.get(str(hashlib.sha1(bundle2).hexdigest())).exists

    def test_submit(self):
        volume = self.start_master()
        conn = Connection()

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
                'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
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
            }, volume['context']['bundle_id']['releases'])

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
        conn = Connection()

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

        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))
        conn.put(['context', 'package', 'releases', '*'], {'binary': ['package.bin']})

        self.assertEqual({
            'activity': {
                'title': 'activity',
                'blob': activity_blob,
                'command': ['activity', 'true'],
                'version': [[1], 0],
                'size': len(activity_pack),
                'unpack_size': len(activity_unpack),
                'content-type': 'application/vnd.olpc-sugar',
                },
            'dep': {
                'title': 'dep',
                'blob': dep_blob,
                'version': [[2], 0],
                'size': len(dep_pack),
                'unpack_size': len(dep_unpack),
                'content-type': 'application/vnd.olpc-sugar',
                },
            'package': {
                'packages': ['package.bin'],
                'version': [],
                },
            },
            conn.get(['context', 'activity'], cmd='solve'))

    def test_SolveWithArguments(self):
        volume = self.start_master()
        conn = Connection()

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

        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))
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
                'content-type': 'application/vnd.olpc-sugar',
                },
            'dep': {
                'title': 'dep',
                'blob': dep_blob,
                'version': [[2], 0],
                'size': len(dep_pack),
                'unpack_size': len(dep_unpack),
                'content-type': 'application/vnd.olpc-sugar',
                },
            'package': {
                'packages': ['package.bin'],
                'version': [[1], 0],
                },
            },
            conn.get(['context', 'activity'], cmd='solve',
                stability='developer', lsb_id='Ubuntu', lsb_release='10.04', requires=['dep', 'package']))

    def test_Resolve(self):
        volume = self.start_master()
        conn = Connection()

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
        this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'guid': 'package',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            }, principal=Principal('admin', 0xF))
        conn.put(['context', 'package', 'releases', '*'], {'binary': ['package.bin']})

        response = Response()
        reply = conn.call(Request(method='GET', path=['context', 'activity'], cmd='resolve'), response)
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
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={})
        self.override(time, 'time', lambda: 0)

        agg1 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID))
        agg2 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg1: {'seqno': 4, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            agg2: {'seqno': 5, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}, 'value': None},
            },
            volume['document'][guid]['prop1'])

        agg3 = this.call(method='POST', path=['document', guid, 'prop2'], environ=auth_env(tests.UID))
        self.assertRaises(http. Forbidden, this.call, method='POST', path=['document', guid, 'prop2'], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg3: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            },
            volume['document'][guid]['prop2'])

    def test_AggpropReplaceAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REPLACE)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REPLACE | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = self.start_master([Document, User])
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={})
        self.override(time, 'time', lambda: 0)

        agg1 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID))
        agg2 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg1: {'seqno': 4, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            agg2: {'seqno': 5, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}, 'value': None},
            },
            volume['document'][guid]['prop1'])
        self.assertRaises(http. Forbidden, this.call, method='PUT', path=['document', guid, 'prop1', agg1], environ=auth_env(tests.UID2))
        this.call(method='PUT', path=['document', guid, 'prop1', agg2], environ=auth_env(tests.UID2))

        agg3 = this.call(method='POST', path=['document', guid, 'prop2'], environ=auth_env(tests.UID))
        self.assertRaises(http. Forbidden, this.call, method='POST', path=['document', guid, 'prop2'], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg3: {'seqno': 7, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}, 'value': None},
            },
            volume['document'][guid]['prop2'])

    def test_AggpropRemoveAccess(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.AUTHOR)
            def prop2(self, value):
                return value

        volume = self.start_master([Document, User])
        volume['user'].create({'guid': tests.UID, 'name': 'user1', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        guid = this.call(method='POST', path=['document'], environ=auth_env(tests.UID), content={})
        self.override(time, 'time', lambda: 0)

        agg1 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID), content=True)
        agg2 = this.call(method='POST', path=['document', guid, 'prop1'], environ=auth_env(tests.UID2), content=True)
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            volume['document'][guid]['prop1'])
        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop1', agg1], environ=auth_env(tests.UID2))
        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop1', agg2], environ=auth_env(tests.UID))
        self.assertEqual({
            agg1: {'seqno': 4, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            volume['document'][guid]['prop1'])

        this.call(method='DELETE', path=['document', guid, 'prop1', agg1], environ=auth_env(tests.UID))
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 5, 'value': True, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            volume['document'][guid]['prop1'])
        this.call(method='DELETE', path=['document', guid, 'prop1', agg2], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg1: {'seqno': 6, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            agg2: {'seqno': 7, 'author': {tests.UID2: {'name': 'user2', 'order': 0, 'role': 1}}},
            },
            volume['document'][guid]['prop1'])

        agg3 = this.call(method='POST', path=['document', guid, 'prop2'], environ=auth_env(tests.UID), content=True)
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            volume['document'][guid]['prop2'])

        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop2', agg3], environ=auth_env(tests.UID2))
        self.assertEqual({
            agg3: {'seqno': 8, 'value': True, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            volume['document'][guid]['prop2'])
        this.call(method='DELETE', path=['document', guid, 'prop2', agg3], environ=auth_env(tests.UID))
        self.assertEqual({
            agg3: {'seqno': 9, 'author': {tests.UID: {'name': 'user1', 'order': 0, 'role': 3}}},
            },
            volume['document'][guid]['prop2'])

    def test_diff_resource(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop1(self, value):
                return value

            @db.stored_property()
            def prop2(self, value):
                return value

            @db.stored_property(db.Blob)
            def prop3(self, value):
                return value

            @db.stored_property(db.Blob)
            def prop4(self, value):
                return value

        volume = db.Volume('.', [Document])
        router = Router(NodeRoutes('node', volume=volume))

        volume['document'].create({
            'guid': 'guid',
            'prop1': '1',
            'prop2': 2,
            'prop3': volume.blobs.post('333', '3/3').digest,
            })
        volume['document'].update('guid', {'prop4': volume.blobs.post('4444', '4/4').digest})
        self.utime('db/document/gu/guid', 1)

        packet = packets.decode(StringIO(
            ''.join([i for i in this.call(method='GET', path=['document', 'guid'], cmd='diff')]),
            ))
        self.assertEqual({
            'ranges': [[1, 4]],
            'patch': {
                'guid': {'value': 'guid', 'mtime': 1},
                'prop1': {'value': '1', 'mtime': 1},
                'prop2': {'value': 2, 'mtime': 1},
                'prop3': {'value': hashlib.sha1('333').hexdigest(), 'mtime': 1},
                'prop4': {'value': hashlib.sha1('4444').hexdigest(), 'mtime': 1},
                },
            },
            packet.header)
        self.assertEqual(sorted([
            {'content-type': '4/4', 'content-length': '4', 'x-seqno': '3'},
            {'content-type': '3/3', 'content-length': '3', 'x-seqno': '1'},
            ]),
            sorted([i.meta for i in packet]))

        packet = packets.decode(StringIO(
            ''.join([i for i in this.call(method='GET', path=['document', 'guid'], cmd='diff', environ={
                'HTTP_X_RANGES': json.dumps([[1, 1]]),
            })])))
        self.assertEqual({
            },
            packet.header)
        self.assertEqual(sorted([
            ]),
            sorted([i.meta for i in packet]))

        packet = packets.decode(StringIO(
            ''.join([i for i in this.call(method='GET', path=['document', 'guid'], cmd='diff', environ={
                'HTTP_X_RANGES': json.dumps([[2, 2]]),
            })])))
        self.assertEqual({
            'ranges': [[1, 2]],
            'patch': {
                'guid': {'value': 'guid', 'mtime': 1},
                'prop1': {'value': '1', 'mtime': 1},
                'prop2': {'value': 2, 'mtime': 1},
                'prop3': {'value': hashlib.sha1('333').hexdigest(), 'mtime': 1},
                },
            },
            packet.header)
        self.assertEqual(sorted([
            {'content-type': '3/3', 'content-length': '3', 'x-seqno': '1'},
            ]),
            sorted([i.meta for i in packet]))

        packet = packets.decode(StringIO(
            ''.join([i for i in this.call(method='GET', path=['document', 'guid'], cmd='diff', environ={
                'HTTP_X_RANGES': json.dumps([[3, 3]]),
            })])))
        self.assertEqual({
            },
            packet.header)
        self.assertEqual(sorted([
            ]),
            sorted([i.meta for i in packet]))

        packet = packets.decode(StringIO(
            ''.join([i for i in this.call(method='GET', path=['document', 'guid'], cmd='diff', environ={
                'HTTP_X_RANGES': json.dumps([[4, 4]]),
            })])))
        self.assertEqual({
            'ranges': [[3, 4]],
            'patch': {
                'prop4': {'value': hashlib.sha1('4444').hexdigest(), 'mtime': 1},
                },
            },
            packet.header)
        self.assertEqual(sorted([
            {'content-type': '4/4', 'content-length': '4', 'x-seqno': '3'},
            ]),
            sorted([i.meta for i in packet]))

    def test_diff_resource_NotForUsers(self):

        class User(db.Resource):
            pass

        volume = db.Volume('.', [User])
        router = Router(NodeRoutes('node', volume=volume))
        volume['user'].create({'guid': 'guid'})

        self.assertRaises(http.BadRequest, this.call, method='GET', path=['user', 'guid'], cmd='diff')

    def test_grouped_diff(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = db.Volume('.', [Document])
        router = Router(NodeRoutes('node', volume=volume))

        volume['document'].create({'guid': '1', 'prop': 'q'})
        volume['document'].create({'guid': '2', 'prop': 'w'})
        volume['document'].create({'guid': '3', 'prop': 'w'})
        volume['document'].create({'guid': '4', 'prop': 'e'})
        volume['document'].create({'guid': '5', 'prop': 'e'})
        volume['document'].create({'guid': '6', 'prop': 'e'})
        self.utime('db/document', 0)

        self.assertEqual({
            '1': [[1, 1]],
            '2': [[2, 2]],
            '3': [[3, 3]],
            '4': [[4, 4]],
            '5': [[5, 5]],
            '6': [[6, 6]],
            },
            this.call(method='GET', path=['document'], cmd='diff'))

        self.assertEqual({
            'q': [[1, 1]],
            'w': [[2, 3]],
            'e': [[4, 6]],
            },
            this.call(method='GET', path=['document'], cmd='diff', key='prop'))

    def test_grouped_diff_Limits(self):
        node_routes._GROUPED_DIFF_LIMIT = 2

        class Document(db.Resource):
            pass

        volume = db.Volume('.', [Document])
        router = Router(NodeRoutes('node', volume=volume))

        volume['document'].create({'guid': '1'})
        volume['document'].create({'guid': '2'})
        volume['document'].create({'guid': '3'})
        volume['document'].create({'guid': '4'})
        volume['document'].create({'guid': '5'})
        self.utime('db/document', 0)

        self.assertEqual({
            '1': [[1, 1]],
            '2': [[2, 2]],
            },
            this.call(method='GET', path=['document'], cmd='diff'))

        self.assertEqual({
            '3': [[3, 3]],
            '4': [[4, 4]],
            },
            this.call(method='GET', path=['document'], cmd='diff', environ={'HTTP_X_RANGES': json.dumps([[3, None]])}))

        self.assertEqual({
            '5': [[5, 5]],
            },
            this.call(method='GET', path=['document'], cmd='diff', environ={'HTTP_X_RANGES': json.dumps([[5, None]])}))

        self.assertEqual({
            },
            this.call(method='GET', path=['document'], cmd='diff', environ={'HTTP_X_RANGES': json.dumps([[6, None]])}))

    def test_grouped_diff_NotForUsers(self):

        class User(db.Resource):
            pass

        volume = db.Volume('.', [User])
        router = Router(NodeRoutes('node', volume=volume))
        volume['user'].create({'guid': '1'})

        self.assertRaises(http.BadRequest, this.call, method='GET', path=['user'], cmd='diff')

    def test_apply(self):

        class Document(db.Resource):

            @db.stored_property()
            def prop(self, value):
                return value

        volume = self.start_master([Document, User])
        conn = Connection()

        conn.upload(cmd='apply', data=
            json.dumps({
                }) + '\n' +
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': '1'},
                }) + '\n' +
            json.dumps({
                'op': {'method': 'POST', 'path': ['document']},
                'content': {'prop': '2'},
                }) + '\n'
            )

        self.assertEqual(sorted([
            {'prop': '1', 'author': [{'guid': tests.UID, 'name': 'test', 'role': ACL.ORIGINAL | ACL.INSYSTEM}]},
            {'prop': '2', 'author': [{'guid': tests.UID, 'name': 'test', 'role': ACL.ORIGINAL | ACL.INSYSTEM}]},
            ]),
            sorted(this.call(method='GET', path=['document'], reply=['prop', 'author'])['result']))

    def test_DoNotPassGuidsForCreate(self):

        class TestDocument(db.Resource):
            pass

        volume = self.start_master([TestDocument, User])
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['testdocument'], content={'guid': 'foo'}, environ=auth_env(tests.UID))
        guid = this.call(method='POST', path=['testdocument'], content={}, environ=auth_env(tests.UID))
        assert guid


def auth_env(uid):
    key = RSA.load_key(join(tests.root, 'data', uid))
    nonce = int(time.time() + 2)
    data = hashlib.sha1('%s:%s' % (uid, nonce)).digest()
    signature = key.sign(data).encode('hex')
    authorization = 'Sugar username="%s",nonce="%s",signature="%s"' % \
            (uid, nonce, signature)
    return {'HTTP_AUTHORIZATION': authorization}


if __name__ == '__main__':
    tests.main()