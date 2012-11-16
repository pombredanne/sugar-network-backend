#!/usr/bin/env python
# sugar-lint: disable

import time
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network import node, Client
from sugar_network.toolkit.rrd import Rrd
from sugar_network.toolkit.router import Unauthorized
from sugar_network.node import stats, obs
from sugar_network.node.commands import NodeCommands
from sugar_network.node.stats import stats_node_step, stats_node_rras, NodeStats
from sugar_network.resources.volume import Volume, Resource
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.resources.review import Review
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.artifact import Artifact
from sugar_network.resources.solution import Solution
from sugar_network.resources.user import User
from sugar_network.toolkit.router import Request


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        stats.stats_root.value = 'stats'
        stats.stats_user_step.value = 1
        stats.stats_user_rras.value = ['RRA:AVERAGE:0.5:1:100']

    def test_UserStats(self):
        volume = Volume('db')
        cp = NodeCommands(volume)

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
            'step': stats.stats_user_step.value,
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
            'step': stats.stats_user_step.value,
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
            'step': stats.stats_user_step.value,
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
            'step': stats.stats_user_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

    def test_NodeStats(self):
        stats_node_step.value = 1
        rrd = Rrd('stats/node', stats_node_step.value, stats_node_rras.value)

        ts = int(time.time()) / 3 * 3
        for i in range(100):
            rrd['user'].put({'total': i}, ts + i)

        volume = Volume('db', [User, Context, Review, Feedback, Solution, Artifact])
        stats = NodeStats(volume)
        cp = NodeCommands(volume, stats)

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
        cp = NodeCommands(volume)

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

        call(cp, method='DELETE', document='context', guid=guid, principal='principal')

        assert exists(guid_path)
        self.assertRaises(ad.NotFound, call, cp, method='GET', document='context', guid=guid, reply=['guid', 'title'])
        self.assertEqual(['deleted'], volume['context'].get(guid)['layer'])

    def test_RegisterUser(self):
        cp = NodeCommands(Volume('db', [User]))

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

            @ad.document_command(method='GET', cmd='probe1',
                    permissions=ad.ACCESS_AUTH)
            def probe1(self, directory):
                pass

            @ad.document_command(method='GET', cmd='probe2')
            def probe2(self, directory):
                pass

        cp = NodeCommands(Volume('db', [User, Document]))
        guid = call(cp, method='POST', document='document', principal='user', content={})
        self.assertRaises(Unauthorized, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal='user')
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommands(self):

        class Document(Resource):

            @ad.document_command(method='GET', cmd='probe1',
                    permissions=ad.ACCESS_AUTHOR)
            def probe1(self):
                pass

            @ad.document_command(method='GET', cmd='probe2')
            def probe2(self):
                pass

        class User(ad.Document):
            pass

        cp = NodeCommands(Volume('db', [User, Document]))
        guid = call(cp, method='POST', document='document', principal='principal', content={})

        self.assertRaises(ad.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        self.assertRaises(ad.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid, principal='fake')
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal='principal')
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_ForbiddenCommandsForUserResource(self):
        cp = NodeCommands(Volume('db', [User]))

        call(cp, method='POST', document='user', principal='fake', content={
            'name': 'user1',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })
        self.assertEqual('user1', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

        self.assertRaises(Unauthorized, call, cp, method='PUT', document='user', guid=tests.UID, content={'name': 'user2'})
        self.assertRaises(ad.Forbidden, call, cp, method='PUT', document='user', guid=tests.UID, principal='fake', content={'name': 'user2'})
        call(cp, method='PUT', document='user', guid=tests.UID, principal=tests.UID, content={'name': 'user2'})
        self.assertEqual('user2', call(cp, method='GET', document='user', guid=tests.UID, prop='name'))

    def test_SetUser(self):
        cp = NodeCommands(Volume('db'))

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {'principal': 1},
                call(cp, method='GET', document='context', guid=guid, prop='authority'))

    def test_find_MaxLimit(self):
        cp = NodeCommands(Volume('db'))

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
        cp = NodeCommands(volume)

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })

        call(cp, method='GET', document='context', guid=guid)
        self.assertNotEqual([], call(cp, method='GET', document='context')['result'])

        volume['context'].update(guid, layer=['deleted'])

        self.assertRaises(ad.NotFound, call, cp, method='GET', document='context', guid=guid)
        self.assertEqual([], call(cp, method='GET', document='context')['result'])

    def test_SetGuidOnMaster(self):
        volume1 = Volume('db1')
        cp1 = NodeCommands(volume1)
        call(cp1, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'foo',
            })
        self.assertRaises(ad.NotFound, call, cp1, method='GET', document='context', guid='foo')

        volume2 = Volume('db2')
        self.touch('db2/master')
        cp2 = NodeCommands(volume2)
        call(cp2, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'foo',
            })
        self.assertEqual(
                {'guid': 'foo', 'implement': ('foo',), 'title': 'title'},
                call(cp2, method='GET', document='context', guid='foo', reply=['guid', 'implement', 'title']))

    def test_PackagesRoute(self):
        self.override(obs, 'get_presolve_repos', lambda: [
            {'name': 'Gentoo-2.1', 'arch': 'x86'},
            {'name': 'Debian-6.0', 'arch': 'x86_64'},
            ])

        volume = self.start_master()
        client = Client()

        client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package1',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'binary': ['package1-1', 'package1-2']},
                'Debian-6.0': {'status': 'success', 'binary': ['package1-3']},
                },
            })
        client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package2',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'devel': ['package2-1', 'package2-2']},
                'Debian-6.0': {'status': 'success', 'devel': ['package2-3']},
                },
            })
        client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package3',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'binary': ['package3-1', 'package3-2']},
                'Debian-6.0': {'status': 'success', 'binary': ['package3-3']},
                },
            })

        self.assertEqual(
                {'total': 2, 'result': [{'arch': 'x86', 'name': 'Gentoo-2.1'}, {'arch': 'x86_64', 'name': 'Debian-6.0'}]},
                client.get(['packages']))
        self.assertEqual(
                {'total': 2, 'result': ['package1', 'package2']},
                client.get(['packages', 'Gentoo-2.1']))
        self.assertEqual(
                ['package1-1', 'package1-2'],
                client.get(['packages', 'Gentoo-2.1', 'package1']))
        self.assertEqual(
                ['package1-3'],
                client.get(['packages', 'Debian-6.0', 'package1']))
        self.assertRaises(RuntimeError, client.request, 'GET', ['packages', 'Debian-6.0', 'package2'])
        self.assertRaises(RuntimeError, client.request, 'GET', ['packages', 'Gentoo-2.1', 'package3'])

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
        client.request('PUT', ['implementation', impl, 'data'], 'bundle')

        self.assertEqual('bundle', client.get(['context', context], cmd='clone', version='1', stability='stable', requires=['foo', 'bar']))


def call(cp, principal=None, content=None, **kwargs):
    request = Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': 'localhost'}
    return cp.call(request, ad.Response())


if __name__ == '__main__':
    tests.main()
