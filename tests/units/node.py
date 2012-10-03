#!/usr/bin/env python
# sugar-lint: disable

import time
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network import node
from sugar_network.toolkit.router import Unauthorized
from sugar_network.node import stats
from sugar_network.node.commands import NodeCommands
from sugar_network.resources.volume import Volume, Request
from sugar_network.resources.user import User


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        stats.stats.value = True
        stats.stats_root.value = 'stats'
        stats.stats_step.value = 1

    def test_stats(self):
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
            'step': stats.stats_step.value,
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
            'step': stats.stats_step.value,
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
            'step': stats.stats_step.value,
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
            'step': stats.stats_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

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

        class Document(ad.Document):

            @classmethod
            @ad.directory_command(method='GET', cmd='probe1',
                    permissions=ad.ACCESS_AUTH)
            def probe1(self, directory):
                pass

            @classmethod
            @ad.directory_command(method='GET', cmd='probe2')
            def probe2(self, directory):
                pass

        cp = NodeCommands(Volume('db', [Document]))
        self.assertRaises(Unauthorized, call, cp, method='GET', cmd='probe1', document='document')
        call(cp, method='GET', cmd='probe1', document='document', principal='user')
        call(cp, method='GET', cmd='probe2', document='document')

    def test_ForbiddenCommands(self):

        class Document(ad.Document):

            @ad.active_property(prefix='U', typecast=[], default=[])
            def user(self, value):
                return value

            @ad.active_property(prefix='A', typecast=[], default=[])
            def author(self, value):
                return value

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
        guid = call(cp, method='POST', document='document', principal='principal', content={'user': ['principal']})

        self.assertRaises(ad.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid)
        self.assertRaises(ad.Forbidden, call, cp, method='GET', cmd='probe1', document='document', guid=guid, principal='fake')
        call(cp, method='GET', cmd='probe1', document='document', guid=guid, principal='principal')
        call(cp, method='GET', cmd='probe2', document='document', guid=guid)

    def test_SetUser(self):
        cp = NodeCommands(Volume('db'))

        guid = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(['principal'], call(cp, method='GET', document='context', guid=guid, prop='user'))

    def test_SetAuthor(self):
        cp = NodeCommands(Volume('db'))

        call(cp, method='POST', document='user', principal=tests.UID, content={
            'name': 'user1',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY,
            })

        call(cp, method='POST', document='user', principal=tests.UID2, content={
            'name': 'user1',
            'color': '',
            'machine_sn': '',
            'machine_uuid': '',
            'pubkey': tests.PUBKEY2,
            })

        context1 = call(cp, method='POST', document='context', principal=tests.UID, content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                ['user1'],
                call(cp, method='GET', document='context', guid=context1, prop='author'))

        context2 = call(cp, method='POST', document='context', principal='fake', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                [],
                call(cp, method='GET', document='context', guid=context2, prop='author'))

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

    def test_GetBlobsByUrls(self):
        volume = Volume('db')
        cp = NodeCommands(volume)

        guid1 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        guid2 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid2, 'icon', url='http://foo/bar')
        guid3 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title3',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid3, 'icon', url='/foo/bar')
        guid4 = call(cp, method='POST', document='artifact', principal='principal', content={
            })
        guid5 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title5',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid5, 'icon', url={'file1': {'order': 1, 'url': '/1'}, 'file2': {'order': 2, 'url': 'http://2'}})

        self.assertEqual(
                {'guid': guid1, 'icon': 'http://localhost/static/images/missing.png', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid1, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid2, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid3, 'icon': 'http://localhost/foo/bar', 'layer': ['public']},
                call(cp, method='GET', document='context', guid=guid3, reply=['guid', 'icon', 'layer']))
        self.assertEqual(
                {'guid': guid4, 'data': 'http://localhost/artifact/%s/data' % guid4, 'layer': ['public']},
                call(cp, method='GET', document='artifact', guid=guid4, reply=['guid', 'data', 'layer']))

        self.assertEqual(
                sorted([
                    {'guid': guid1, 'icon': 'http://localhost/static/images/missing.png', 'layer': ['public']},
                    {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                    {'guid': guid3, 'icon': 'http://localhost/foo/bar', 'layer': ['public']},
                    {'guid': guid5, 'icon': ['http://localhost/1', 'http://2'], 'layer': ['public']},
                    ]),
                sorted(call(cp, method='GET', document='context', reply=['guid', 'icon', 'layer'])['result']))

        self.assertEqual([
            {'guid': guid4, 'data': 'http://localhost/artifact/%s/data' % guid4, 'layer': ['public']},
            ],
            call(cp, method='GET', document='artifact', reply=['guid', 'data', 'layer'])['result'])

        node.static_url.value = 'static_url'
        self.assertEqual(
                sorted([
                    {'guid': guid1, 'icon': 'static_url/static/images/missing.png', 'layer': ['public']},
                    {'guid': guid2, 'icon': 'http://foo/bar', 'layer': ['public']},
                    {'guid': guid3, 'icon': 'static_url/foo/bar', 'layer': ['public']},
                    {'guid': guid5, 'icon': ['static_url/1', 'http://2'], 'layer': ['public']},
                    ]),
                sorted(call(cp, method='GET', document='context', reply=['guid', 'icon', 'layer'])['result']))

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
                {'guid': 'foo', 'implement': ['foo'], 'title': 'title'},
                call(cp2, method='GET', document='context', guid='foo', reply=['guid', 'implement', 'title']))


def call(cp, principal=None, content=None, **kwargs):
    request = Request(**kwargs)
    request.principal = principal
    request.content = content
    request.environ = {'HTTP_HOST': 'localhost'}
    return cp.call(request, ad.Response())


if __name__ == '__main__':
    tests.main()
