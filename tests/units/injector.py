#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import shutil
import zipfile
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from active_toolkit import coroutine
from sugar_network import checkin, launch
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.implementation import Implementation
from sugar_network.local import activities
from sugar_network import IPCClient


class InjectorTest(tests.Test):

    def test_checkin_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, '{}'),
                (blob_path + '.blob', json.dumps({})),
                )

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'error': "Interface '%s' has no usable implementations" % context, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, '{}'),
                (blob_path + '.blob', json.dumps({
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
                )
        os.unlink('cache/context/%s/%s/feed.meta' % (context[:2], context))

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'error': 'Cannot download implementation', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])
        os.unlink('cache/implementation/%s/%s/data.meta' % (impl[:2], impl))

        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
        bundle.writestr('probe', 'probe')
        bundle.close()

        pipe = checkin('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_2.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        assert exists('Activities/data/probe')
        self.assertEqual('probe', file('Activities/data/probe').read())

    def test_launch_Online(self):
        self.start_ipc_and_restful_server([User, Context, Implementation])
        remote = IPCClient(mountpoint='/')

        context = remote.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })

        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, '{}'),
                (blob_path + '.blob', json.dumps({
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
                )

        blob_path = 'remote/implementation/%s/%s/data' % (impl[:2], impl)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
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

        pipe = launch('/', context)

        log_path = tests.tmpdir +  '/.sugar/default/logs/%s.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'exec', 'implementation': impl, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'failure', 'implementation': impl, 'error': 'Exited with status 1', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            ],
            [i for i in pipe])

        impl_2 = remote.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'date': 0,
            'stability': 'stable',
            'notes': '',
            })

        os.unlink('cache/context/%s/%s/feed.meta' % (context[:2], context))
        blob_path = 'remote/context/%s/%s/feed' % (context[:2], context)
        self.touch(
                (blob_path, '{}'),
                (blob_path + '.blob', json.dumps({
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
                )

        blob_path = 'remote/implementation/%s/%s/data' % (impl_2[:2], impl_2)
        self.touch((blob_path, '{}'))
        bundle = zipfile.ZipFile(blob_path + '.blob', 'w')
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

        pipe = launch('/', context)
        log_path = tests.tmpdir +  '/.sugar/default/logs/%s_1.log' % context
        self.assertEqual([
            {'state': 'boot', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'analyze', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'download', 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'ready', 'implementation': impl_2, 'mountpoint': '/', 'context': context, 'log_path': log_path},
            {'state': 'exec', 'implementation': impl_2, 'mountpoint': '/', 'context': context, 'log_path': log_path},
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
        client = IPCClient(mountpoint='~')

        monitor = coroutine.spawn(activities.monitor,
                self.mounts.volume['context'], ['Activities'])
        coroutine.sleep()

        blob = client.get(['context', 'bundle_id', 'feed'], cmd='get_blob')
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
                blob)


if __name__ == '__main__':
    tests.main()
