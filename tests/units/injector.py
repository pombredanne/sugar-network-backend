#!/usr/bin/env python
# sugar-lint: disable

import json
from os.path import exists

from __init__ import tests

import zerosugar
from sugar_network.client import Client
from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context
from sugar_network_server.resources.implementation import Implementation


class InjectorTest(tests.Test):

    def test_checkin(self):
        self.start_server([User, Context, Implementation])
        client = Client('~')

        context = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        impl = client.Implementation(
                context=context,
                license=['GPLv3+'],
                version='1',
                date=0,
                stability='stable',
                notes='').post()

        blob_path = 'local/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, json.dumps({
                    '1': {
                        '*-*': {
                            'commands': {
                                'activity': {
                                    'exec': 'echo',
                                    },
                                },
                            'stability': 'stable',
                            'guid': impl,
                            'size': 0,
                            },
                        },
                    })),
                (blob_path + '.sha1', ''),
                )

        blob_path = 'local/implementation/%s/%s/bundle' % (impl[:2], impl)
        self.touch(
                (blob_path + '/file', 'probe'),
                (blob_path + '.sha1', ''),
                )

        pipe = zerosugar.checkin('~', context)

        messages = []
        for i in pipe:
            messages.append(i)

        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ],
            messages)

        assert exists('Activities/bundle/file')
        self.assertEqual('probe', file('Activities/bundle/file').read())


if __name__ == '__main__':
    tests.main()
