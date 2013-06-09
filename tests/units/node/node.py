#!/usr/bin/env python
# sugar-lint: disable

import os
import time
import json
import zipfile
from email.utils import formatdate, parsedate
from os.path import exists

from __init__ import tests

from sugar_network import db, node
from sugar_network.client import Client
from sugar_network.toolkit import http, coroutine
from sugar_network.toolkit.rrd import Rrd
from sugar_network.node import stats_user, stats_node, obs
from sugar_network.node.commands import NodeCommands
from sugar_network.node.master import MasterCommands
from sugar_network.resources.volume import Volume, Resource
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.review import Review
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.solution import Solution
from sugar_network.resources.user import User


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        node.stats_root.value = 'stats'
        stats_user.stats_user_step.value = 1
        stats_user.stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']

    def test_UserStats(self):
        volume = Volume('db')
        cp = NodeCommands('guid', volume)

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

        volume = Volume('db', [User, Context, Review, Feedback, Solution, Artifact])
        cp = NodeCommands('guid', volume)

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
        volume = Volume('db')
        cp = NodeCommands('guid', volume)

        guid = call(cp, method='POST', document='context', principal='principal', content={
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

        events = []
        volume.connect(lambda event: events.append(event))
        call(cp, method='DELETE', document='context', guid=guid, principal='principal')
        coroutine.dispatch()

        self.assertRaises(http.NotFound, call, cp, method='GET', document='context', guid=guid, reply=['guid', 'title'])
        self.assertEqual(['deleted'], volume['context'].get(guid)['layer'])
        self.assertEqual([
            {'event': 'delete', 'document': 'context', 'guid': guid},
            {'event': 'commit', 'document': 'context', 'mtime': int(os.stat('db/context/index/mtime').st_mtime)},
            ],
            events)

    def test_SimulateDeleteEvents(self):
        volume = Volume('db')
        cp = NodeCommands('guid', volume)

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        events = []
        volume.connect(lambda event: events.append(event))
        call(cp, method='PUT', document='context', guid=guid, principal='principal', content={'layer': ['deleted']})
        coroutine.dispatch()

        self.assertEqual([
            {'event': 'delete', 'document': 'context', 'guid': guid},
            {'event': 'commit', 'document': 'context', 'mtime': int(os.stat('db/context/index/mtime').st_mtime)},
            ],
            events)

    def test_RegisterUser(self):
        cp = NodeCommands('guid', Volume('db', [User]))

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

        class Document(Resource):

            @db.document_command(method='GET', cmd='probe1',
                    permissions=db.ACCESS_AUTH)
            def probe1(self, directory):
                pass

            @db.document_command(method='GET', cmd='probe2')
            def probe2(self, directory):
                pass

        cp = NodeCommands('guid', Volume('db', [User, Document]))
        guid = call(cp, method='POST', document='document', principal='user', content={})
        self.assertRaises(http.Unauthorized, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal='user')
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommands(self):

        class Document(Resource):

            @db.document_command(method='GET', cmd='probe1',
                    permissions=db.ACCESS_AUTHOR)
            def probe1(self):
                pass

            @db.document_command(method='GET', cmd='probe2')
            def probe2(self):
                pass

        class User(db.Document):
            pass

        cp = NodeCommands('guid', Volume('db', [User, Document]))
        guid = call(cp, method='POST', document='document', principal='principal', content={})

        self.assertRaises(http.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        self.assertRaises(http.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid, principal='fake')
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal='principal')
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommandsForUserResource(self):
        cp = NodeCommands('guid', Volume('db', [User]))

        call(cp, method='POST', document='user', principal='fake', content={
            'name': 'user1',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

        self.assertRaises(http.Unauthorized, call, cp, method='PUT', document='user', guid=tests.UID, content={'name': 'user2'})
        self.assertRaises(http.Forbidden, call, cp, method='PUT', document='user', guid=tests.UID, principal='fake', content={'name': 'user2'})
        call(cp, method='PUT', document='user', guid=tests.UID, principal=tests.UID, content={'name': 'user2'})
        self.assertEqual('user2', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

    def test_SetUser(self):
        cp = NodeCommands('guid', Volume('db'))

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                [{'name': 'principal', 'role': 2}],
                call(cp, method='GET', document='context', guid=guid, prop='author'))

    def test_find_MaxLimit(self):
        cp = NodeCommands('guid', Volume('db'))

        call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        call(cp, method='POST', document='context', principal='principal', content={
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
        volume = Volume('db')
        cp = NodeCommands('guid', volume)

        guid = call(cp, method='POST', document='context', principal='principal', content={
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
        volume2 = Volume('db2')
        cp2 = MasterCommands('guid', volume2)
        call(cp2, method='POST', document='context', principal='principal', content={
            'guid': 'foo',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {'guid': 'foo', 'title': 'title'},
                call(cp2, method='GET', document='context', guid='foo', reply=['guid', 'title']))

    def test_CreateMalformedGUID(self):
        cp = MasterCommands('guid', Volume('db2'))

        self.assertRaises(RuntimeError, call, cp, method='POST', document='context', principal='principal', content={
            'guid': '!?',
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

    def test_FailOnExistedGUID(self):
        cp = MasterCommands('guid', Volume('db2'))

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertRaises(RuntimeError, call, cp, method='POST', document='context', principal='principal', content={
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
            'requires': ['foo', 'bar'],
            })
        bundle = zipfile.ZipFile('blob', 'w')
        bundle.writestr('topdir/probe', 'probe')
        bundle.close()
        blob = file('blob', 'rb').read()
        client.request('PUT', ['implementation', impl, 'data'], blob)

        self.assertEqual(blob, client.get(['context', context], cmd='clone', version='1', stability='stable', requires=['foo', 'bar']))


def call(cp, principal=None, content=None, **kwargs):
    request = db.Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': '127.0.0.1'}
    return cp.call(request, db.Response())


if __name__ == '__main__':
    tests.main()
