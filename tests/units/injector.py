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

        pipe = zerosugar.checkin('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s.log' % context,
                'error': "Interface '%s' has no usable implementations" % context,
                'mountpoint': '~',
                'context': context,
                }),
            ],
            [i for i in pipe])

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

        pipe = zerosugar.checkin('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context,
                'error': 'Cannot download bundle',
                'mountpoint': '~',
                'context': context,
                }),
            ],
            [i for i in pipe])

        blob_path = 'local/implementation/%s/%s/bundle' % (impl[:2], impl)
        self.touch(
                (blob_path + '/file', 'probe'),
                (blob_path + '.sha1', ''),
                )

        pipe = zerosugar.checkin('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ],
            [i for i in pipe])

        assert exists('Activities/bundle/file')
        self.assertEqual('probe', file('Activities/bundle/file').read())

    def test_launch(self):
        self.start_server([User, Context, Implementation])
        client = Client('~')

        context = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        pipe = zerosugar.launch('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s.log' % context,
                'error': "Interface '%s' has no usable implementations" % context,
                'mountpoint': '~',
                'context': context,
                }),
            ],
            [i for i in pipe])

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
                                    'exec': 'false',
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

        pipe = zerosugar.launch('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context,
                'error': 'Cannot download bundle',
                'mountpoint': '~',
                'context': context,
                }),
            ],
            [i for i in pipe])

        blob_path = 'local/implementation/%s/%s/bundle' % (impl[:2], impl)
        self.touch(
                (blob_path + '/file', 'probe'),
                (blob_path + '.sha1', ''),
                )

        pipe = zerosugar.launch('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('exec', {}),
            ('failure', {
                'implementation': impl,
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context,
                'error': 'Exited with status 1',
                'mountpoint': '~',
                'context': context,
                }),
            ],
            [i for i in pipe])

        impl_2 = client.Implementation(
                context=context,
                license=['GPLv3+'],
                version='2',
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
                                    'exec': 'false',
                                    },
                                },
                            'stability': 'stable',
                            'guid': impl,
                            'size': 0,
                            },
                        },
                    '2': {
                        '*-*': {
                            'commands': {
                                'activity': {
                                    'exec': 'true',
                                    },
                                },
                            'stability': 'stable',
                            'guid': impl_2,
                            'size': 0,
                            },
                        },
                    })),
                (blob_path + '.sha1', ''),
                )

        blob_path = 'local/implementation/%s/%s/bundle' % (impl_2[:2], impl_2)
        self.touch(
                (blob_path + '/file', 'probe'),
                (blob_path + '.sha1', ''),
                )

        pipe = zerosugar.launch('~', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('exec', {}),
            ],
            [i for i in pipe])


if __name__ == '__main__':
    tests.main()
