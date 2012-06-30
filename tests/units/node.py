#!/usr/bin/env python
# sugar-lint: disable

import time
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network.node import stats
from sugar_network.node.commands import NodeCommands
from sugar_network.resources.user import User
from sugar_network.resources.context import Context


class NodeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        stats.stats.value = True
        stats.stats_root.value = 'stats'
        stats.stats_step.value = 1

    def test_stats(self):
        volume = ad.SingleVolume('db', [User])
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
            'rras': stats.stats_client_rras.value,
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
            'rras': stats.stats_client_rras.value,
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
            'rras': stats.stats_client_rras.value,
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
            'rras': stats.stats_client_rras.value,
            'step': stats.stats_step.value,
            },
            call(cp, method='GET', cmd='stats-info', document='user', guid=tests.UID, principal=tests.UID))

    def test_HandleDeletes(self):
        volume = ad.SingleVolume('db', [User, Context])
        cp = NodeCommands(volume)

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
            'title': {'en': 'title'},
            },
            call(cp, method='GET', document='context', guid=guid, reply=['guid', 'title']))
        self.assertEqual(['public'], volume['context'].get(guid)['layer'])

        call(cp, method='DELETE', document='context', guid=guid, principal=tests.UID)

        assert exists(guid_path)
        self.assertRaises(ad.NotFound, call, cp, method='GET', document='context', guid=guid, reply=['guid', 'title'])
        self.assertEqual(['deleted'], volume['context'].get(guid)['layer'])

    def test_SetAuthor(self):
        volume = ad.SingleVolume('db', [User, Context])
        cp = NodeCommands(volume)

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


def call(cp, principal=None, content=None, **kwargs):
    request = ad.Request(**kwargs)
    request.principal = principal
    request.content = content
    return cp.call(request, ad.Response())


if __name__ == '__main__':
    tests.main()
