#!/usr/bin/env python
# sugar-lint: disable

import time

import gobject

from __init__ import tests

import active_document as ad
import restful_document as rd

from sugar_network.node import stats
from sugar_network.resources.user import User


class PublicTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        stats.stats.value = True
        stats.stats_root.value = 'stats'
        stats.stats_step.value = 1

    def test_stats(self):
        volume = ad.SingleVolume('db', [User])
        cp = ad.ProxyCommands(ad.VolumeCommands(volume))

        cp.super_call('POST', document='user', principal=tests.UID, content={
            'nickname': 'me',
            'fullname': 'M. E.',
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
            cp.super_call('GET', 'stats-info', document='user', guid=tests.UID, principal=tests.UID))

        cp.super_call('POST', 'stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
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
            cp.super_call('GET', 'stats-info', document='user', guid=tests.UID, principal=tests.UID))

        cp.super_call('POST', 'stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
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
            cp.super_call('GET', 'stats-info', document='user', guid=tests.UID, principal=tests.UID))

        cp.super_call('POST', 'stats-upload', document='user', guid=tests.UID, principal=tests.UID, content={
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
            cp.super_call('GET', 'stats-info', document='user', guid=tests.UID, principal=tests.UID))


if __name__ == '__main__':
    tests.main()
