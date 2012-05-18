#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import shutil
import zipfile
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

import zerosugar
from active_document import coroutine
from sugar_network.client import Client
from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context
from sugar_network_server.resources.implementation import Implementation


class InjectorTest(tests.Test):

    def test_checkin_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        client = Client('/')

        context = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, json.dumps({})),
                (blob_path + '.sha1', ''),
                )

        pipe = zerosugar.checkin('/', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s.log' % context,
                'error': "Interface '%s' has no usable implementations" % context,
                'mountpoint': '/',
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

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
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
        os.unlink('cache/context/%s/%s/feed' % (context[:2], context))

        pipe = zerosugar.checkin('/', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('failure', {
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context,
                'error': 'Cannot download bundle',
                'mountpoint': '/',
                'context': context,
                }),
            ],
            [i for i in pipe])

        blob_path = 'remote/implementation/%s/%s/bundle' % (impl[:2], impl)
        self.touch((blob_path + '.sha1', ''))
        bundle = zipfile.ZipFile(blob_path, 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()

        pipe = zerosugar.checkin('/', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ],
            [i for i in pipe])

        assert exists('Activities/bundle/probe')
        self.assertEqual('probe', file('Activities/bundle/probe').read())

    def test_launch_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        client = Client('/')

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

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
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
                            'extract': 'TestActivitry',
                            },
                        },
                    })),
                (blob_path + '.sha1', ''),
                )

        blob_path = 'remote/implementation/%s/%s/bundle' % (impl[:2], impl)
        self.touch((blob_path + '.sha1', ''))
        bundle = zipfile.ZipFile(blob_path, 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        bundle.close()

        pipe = zerosugar.launch('/', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('exec', {}),
            ('failure', {
                'implementation': impl,
                'log_path': tests.tmpdir +  '/.sugar/default/logs/%s.log' % context,
                'error': 'Exited with status 1',
                'mountpoint': '/',
                'context': context,
                }),
            ],
            [i for i in pipe])

        impl_2 = client.Implementation(
                context=context,
                license=['GPLv3+'],
                version='1',
                date=0,
                stability='stable',
                notes='').post()

        os.unlink('cache/context/%s/%s/feed' % (context[:2], context))
        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
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
                            'extract': 'TestActivitry',
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
                            'extract': 'TestActivitry',
                            },
                        },
                    })),
                (blob_path + '.sha1', ''),
                )

        blob_path = 'remote/implementation/%s/%s/bundle' % (impl_2[:2], impl_2)
        self.touch((blob_path + '.sha1', ''))
        bundle = zipfile.ZipFile(blob_path, 'w')
        bundle.writestr('TestActivitry/activity/activity.info', '\n'.join([
            '[Activity]',
            'name = TestActivitry',
            'bundle_id = %s' % context,
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license=Public Domain',
            ]))
        bundle.close()

        pipe = zerosugar.launch('/', context)
        self.assertEqual([
            ('analyze', {'progress': -1}),
            ('download', {'progress': -1}),
            ('exec', {}),
            ],
            [i for i in pipe])

    def test_OfflineFeed(self):
        self.touch(('Activities/activity-1/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = false',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ]))
        self.touch(('Activities/activity-2/activity/activity.info', [
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 2',
            'license=Public Domain',
            ]))

        self.start_server()
        client = Client('~')

        self.assertEqual(
                json.dumps({
                    '1': {
                        '*-*': {
                            'commands': {
                                'activity': {
                                    'exec': 'false',
                                    },
                                },
                            'stability': 'stable',
                            'guid': tests.tmpdir + '/Activities/activity-1',
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
                            'guid': tests.tmpdir + '/Activities/activity-2',
                            },
                        },
                    }),
                client.Context('bundle_id').get_blob('feed').read())


if __name__ == '__main__':
    tests.main()
