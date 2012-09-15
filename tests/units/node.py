#!/usr/bin/env python
# sugar-lint: disable

import time
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network import node
from sugar_network.node import stats, Unauthorized
from sugar_network.node.commands import NodeCommands
from sugar_network.resources.volume import Volume
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
            'title': {'en': 'title'},
            },
            call(cp, method='GET', document='context', guid=guid, reply=['guid', 'title']))
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
        guid4 = call(cp, method='POST', document='report', principal='principal', content={
            'context': 'context',
            'implementation': 'implementation',
            'description': 'description',
            })
        guid5 = call(cp, method='POST', document='context', principal='principal', content={
            'type': 'activity',
            'title': 'title5',
            'summary': 'summary',
            'description': 'description',
            })
        volume['context'].set_blob(guid5, 'icon', url={'file1': {'order': 1, 'url': '/1'}, 'file2': {'order': 2, 'url': 'http://2'}})

        self.assertEqual(
                {'guid': guid1, 'icon': 'http://localhost:8000/static/images/missing.png'},
                call(cp, method='GET', document='context', guid=guid1, reply=['guid', 'icon']))
        self.assertEqual(
                {'guid': guid2, 'icon': 'http://foo/bar'},
                call(cp, method='GET', document='context', guid=guid2, reply=['guid', 'icon']))
        self.assertEqual(
                {'guid': guid3, 'icon': 'http://localhost:8000/foo/bar'},
                call(cp, method='GET', document='context', guid=guid3, reply=['guid', 'icon']))
        self.assertEqual(
                {'guid': guid4, 'data': 'http://localhost:8000/report/%s/data' % guid4},
                call(cp, method='GET', document='report', guid=guid4, reply=['guid', 'data']))

        self.assertEqual([
            {'guid': guid1, 'icon': 'http://localhost:8000/static/images/missing.png'},
            {'guid': guid2, 'icon': 'http://foo/bar'},
            {'guid': guid3, 'icon': 'http://localhost:8000/foo/bar'},
            {'guid': guid5, 'icon': ['http://localhost:8000/1', 'http://2']},
            ],
            call(cp, method='GET', document='context', reply=['guid', 'icon'])['result'])

        self.assertEqual([
            {'guid': guid4, 'data': 'http://localhost:8000/report/%s/data' % guid4},
            ],
            call(cp, method='GET', document='report', reply=['guid', 'data'])['result'])

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


def call(cp, principal=None, content=None, **kwargs):
    request = ad.Request(**kwargs)
    request.principal = principal
    request.content = content
    return cp.call(request, ad.Response())


if __name__ == '__main__':
    tests.main()
