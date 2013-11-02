#!/usr/bin/env python
# sugar-lint: disable

import json
from cStringIO import StringIO
from zipfile import ZipFile
from os.path import exists

from __init__ import tests, src_root

from sugar_network import client, model
from sugar_network.client import IPCConnection, implementations, packagekit
from sugar_network.client.routes import ClientRoutes
from sugar_network.model.user import User
from sugar_network.model.report import Report
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import coroutine, http, lsb_release


class OfflineRoutes(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        self.override(implementations, '_activity_id_new', lambda: 'activity_id')

    def test_whoami(self):
        ipc = self.start_offline_client()

        self.assertEqual(
                {'guid': tests.UID, 'roles': [], 'route': 'offline'},
                ipc.get(cmd='whoami'))

    def test_Events(self):
        ipc = self.start_offline_client()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        job = coroutine.spawn(read_events)
        coroutine.dispatch()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.put(['context', guid], {
            'title': 'title_2',
            })
        ipc.delete(['context', guid])
        coroutine.sleep(.1)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'resource': 'context', 'event': 'create'},
            {'guid': guid, 'resource': 'context', 'event': 'update'},
            {'guid': guid, 'event': 'delete', 'resource': 'context'},
            ],
            events)

    def test_Feeds(self):
        ipc = self.start_offline_client()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl1 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        self.home_volume['implementation'].update(impl1, {'data': {
            'spec': {'*-*': {}},
            }})
        impl2 = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '2',
            'stability': 'stable',
            'notes': '',
            })
        self.home_volume['implementation'].update(impl2, {'data': {
            'spec': {'*-*': {
                'requires': {
                    'dep1': {},
                    'dep2': {'restrictions': [['1', '2']]},
                    'dep3': {'restrictions': [[None, '2']]},
                    'dep4': {'restrictions': [['3', None]]},
                    },
                }},
            }})

        self.assertEqual({
            'implementations': [
                {
                    'version': '1',
                    'stability': 'stable',
                    'guid': impl1,
                    'license': ['GPLv3+'],
                    'layer': ['local'],
                    'author': {},
                    'ctime': self.home_volume['implementation'].get(impl1).ctime,
                    'notes': {'en-us': ''},
                    'tags': [],
                    'data': {'spec': {'*-*': {}}},
                    },
                {
                    'version': '2',
                    'stability': 'stable',
                    'guid': impl2,
                    'license': ['GPLv3+'],
                    'layer': ['local'],
                    'author': {},
                    'ctime': self.home_volume['implementation'].get(impl2).ctime,
                    'notes': {'en-us': ''},
                    'tags': [],
                    'data': {
                        'spec': {'*-*': {
                            'requires': {
                                'dep1': {},
                                'dep2': {'restrictions': [['1', '2']]},
                                'dep3': {'restrictions': [[None, '2']]},
                                'dep4': {'restrictions': [['3', None]]},
                                },
                            }},
                        },
                    },
                ],
            },
            ipc.get(['context', context], cmd='feed'))

    def test_BLOBs(self):
        ipc = self.start_offline_client()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.request('PUT', ['context', guid, 'preview'], 'image')

        self.assertEqual(
                'image',
                ipc.request('GET', ['context', guid, 'preview']).content)
        self.assertEqual(
                {'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid},
                ipc.get(['context', guid], reply=['preview']))
        self.assertEqual(
                [{'preview': 'http://127.0.0.1:5555/context/%s/preview' % guid}],
                ipc.get(['context'], reply=['preview'])['result'])

        self.assertEqual(
                file(src_root + '/sugar_network/static/httpdocs/images/missing.png').read(),
                ipc.request('GET', ['context', guid, 'icon']).content)
        self.assertEqual(
                {'icon': 'http://127.0.0.1:5555/static/images/missing.png'},
                ipc.get(['context', guid], reply=['icon']))
        self.assertEqual(
                [{'icon': 'http://127.0.0.1:5555/static/images/missing.png'}],
                ipc.get(['context'], reply=['icon'])['result'])

    def test_favorite(self):
        ipc = self.start_offline_client()
        events = []

        def read_events():
            for event in ipc.subscribe(event='!commit'):
                events.append(event)
        coroutine.spawn(read_events)
        coroutine.dispatch()

        context1 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title1',
            'summary': 'summary',
            'description': 'description',
            })
        context2 = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title2',
            'summary': 'summary',
            'description': 'description',
            })

        self.assertEqual(
                sorted([]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='local')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['local']}, {'guid': context2, 'layer': ['local']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['local']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['local'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['local']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['local'], ipc.get(['context', context2, 'layer']))

        del events[:]
        ipc.put(['context', context1], True, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual(
            {'guid': context1, 'resource': 'context', 'event': 'update'},
            events[-1])
        self.assertEqual(
                sorted([{'guid': context1}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='local')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['favorite', 'local']}, {'guid': context2, 'layer': ['local']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['favorite', 'local']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['favorite', 'local'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['local']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['local'], ipc.get(['context', context2, 'layer']))

        del events[:]
        ipc.put(['context', context2], True, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual(
            {'guid': context2, 'resource': 'context', 'event': 'update'},
            events[-1])
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='local')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['favorite', 'local']}, {'guid': context2, 'layer': ['favorite', 'local']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['favorite', 'local']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['favorite', 'local'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['favorite', 'local']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['favorite', 'local'], ipc.get(['context', context2, 'layer']))

        del events[:]
        ipc.put(['context', context1], False, cmd='favorite')
        coroutine.sleep(.1)

        self.assertEqual(
            {'guid': context1, 'resource': 'context', 'event': 'update'},
            events[-1])
        self.assertEqual(
                sorted([{'guid': context2}]),
                sorted(ipc.get(['context'], layer='favorite')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'], layer='local')['result']))
        self.assertEqual(
                sorted([{'guid': context1}, {'guid': context2}]),
                sorted(ipc.get(['context'])['result']))
        self.assertEqual(
                sorted([{'guid': context1, 'layer': ['local']}, {'guid': context2, 'layer': ['favorite', 'local']}]),
                sorted(ipc.get(['context'], reply='layer')['result']))
        self.assertEqual({'layer': ['local']}, ipc.get(['context', context1], reply='layer'))
        self.assertEqual(['local'], ipc.get(['context', context1, 'layer']))
        self.assertEqual({'layer': ['favorite', 'local']}, ipc.get(['context', context2], reply='layer'))
        self.assertEqual(['favorite', 'local'], ipc.get(['context', context2, 'layer']))

    def test_launch_Activity(self):
        local = self.start_online_client()
        ipc = IPCConnection()

        activity_info = '\n'.join([
            '[Activity]',
            'name = TestActivity',
            'bundle_id = bundle_id',
            'exec = true',
            'icon = icon',
            'activity_version = 1',
            'license=Public Domain',
            ])
        blob = self.zips(['TestActivity/activity/activity.info', activity_info])
        impl = ipc.upload(['implementation'], StringIO(blob), cmd='submit', initial=True)

        ipc.put(['context', 'bundle_id'], True, cmd='clone')
        solution = [{
            'guid': impl,
            'context': 'bundle_id',
            'license': ['Public Domain'],
            'stability': 'stable',
            'version': '1',
            'path': tests.tmpdir + '/client/implementation/%s/%s/data.blob' % (impl[:2], impl),
            'layer': ['origin'],
            'author': {tests.UID: {'name': 'test', 'order': 0, 'role': 3}},
            'ctime': self.node_volume['implementation'].get(impl).ctime,
            'notes': {'en-us': ''},
            'tags': [],
            'data': {
                'unpack_size': len(activity_info),
                'blob_size': len(blob),
                'mime_type': 'application/vnd.olpc-sugar',
                'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {}}},
                },
            }]
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

        self.node.stop()
        coroutine.sleep(.1)

        log_path = tests.tmpdir + '/.sugar/default/logs/bundle_id.log'
        self.assertEqual([
            {'event': 'launch', 'foo': 'bar', 'activity_id': 'activity_id'},
            {'event': 'exec', 'activity_id': 'activity_id'},
            {'event': 'exit', 'activity_id': 'activity_id'},
            ],
            [i for i in ipc.get(['context', 'bundle_id'], cmd='launch', foo='bar')])
        assert local['implementation'].exists(impl)
        self.assertEqual(
                [client.api_url.value, ['stable'], solution],
                json.load(file('solutions/bu/bundle_id')))

    def test_ServiceUnavailableWhileSolving(self):
        ipc = self.start_offline_client()

        self.assertEqual([
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'ServiceUnavailable', 'error': "Resource 'foo' does not exist in 'context'"},
            ],
            [i for i in ipc.get(['context', 'foo'], cmd='launch')])

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            {'event': 'launch', 'activity_id': 'activity_id'},
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'ServiceUnavailable',
                'stability': ['stable'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                'error': """\
Can't find all required implementations:
- %s -> (problem)
    No known implementations at all""" % context},
            ],
            [i for i in ipc.get(['context', context], cmd='launch')])

        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'layer': ['origin'],
            })
        self.home_volume['implementation'].update(impl, {'data': {
            'spec': {
                '*-*': {
                    'commands': {'activity': {'exec': 'true'}},
                    'requires': {'dep': {}},
                    },
                },
            }})
        self.assertEqual([
            {'event': 'launch', 'activity_id': 'activity_id'},
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'ServiceUnavailable',
                'stability': ['stable'],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                'error': """\
Can't find all required implementations:
- %s -> 1 (%s)
- dep -> (problem)
    No known implementations at all""" % (context, impl)},
            ],
            [i for i in ipc.get(['context', context], cmd='launch')])
        assert not exists('solutions/%s/%s' % (context[:2], context))

    def test_ServiceUnavailableWhileInstalling(self):
        ipc = self.start_offline_client()

        context = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = ipc.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'layer': ['origin'],
            })
        self.home_volume['implementation'].update(impl, {'data': {
            'spec': {
                '*-*': {
                    'commands': {'activity': {'exec': 'true'}},
                    'requires': {'dep': {}},
                    },
                },
            }})
        ipc.post(['context'], {
            'guid': 'dep',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'aliases': {
                lsb_release.distributor_id(): {
                    'status': 'success',
                    'binary': [['dep.bin']],
                    },
                },
            })

        def resolve(names):
            return dict([(i, {'name': i, 'pk_id': i, 'version': '0', 'arch': '*', 'installed': False}) for i in names])
        self.override(packagekit, 'resolve', resolve)

        self.assertEqual([
            {'event': 'launch', 'activity_id': 'activity_id'},
            {'event': 'failure', 'activity_id': 'activity_id', 'exception': 'ServiceUnavailable', 'error': 'Installation is not available in offline',
                'stability': ['stable'],
                'solution': [
                    {   'guid': impl,
                        'context': context,
                        'license': ['GPLv3+'],
                        'stability': 'stable',
                        'version': '1',
                        'layer': ['origin', 'local'],
                        'author': {},
                        'ctime': self.home_volume['implementation'].get(impl).ctime,
                        'notes': {'en-us': ''},
                        'tags': [],
                        'data': {
                            'spec': {'*-*': {'commands': {'activity': {'exec': 'true'}}, 'requires': {'dep': {}}}},
                            },
                        },
                    {   'guid': 'dep',
                        'context': 'dep',
                        'install': [{'arch': '*', 'installed': False, 'name': 'dep.bin', 'pk_id': 'dep.bin', 'version': '0'}],
                        'license': None,
                        'stability': 'packaged',
                        'version': '0',
                        },
                    ],
                'logs': [
                    tests.tmpdir + '/.sugar/default/logs/shell.log',
                    tests.tmpdir + '/.sugar/default/logs/sugar-network-client.log',
                    ],
                },
            ],
            [i for i in ipc.get(['context', context], cmd='launch')])

    def test_NoAuthors(self):
        ipc = self.start_offline_client()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                {},
                self.home_volume['context'].get(guid)['author'])
        self.assertEqual(
                [],
                ipc.get(['context', guid, 'author']))

    def test_HandleDeletes(self):
        ipc = self.start_offline_client()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        guid_path = 'db/context/%s/%s' % (guid[:2], guid)
        assert exists(guid_path)

        ipc.delete(['context', guid])
        self.assertRaises(http.NotFound, ipc.get, ['context', guid])
        assert not exists(guid_path)

    def test_SubmitReport(self):
        ipc = self.home_volume = self.start_offline_client()

        self.touch(
                ['file1', 'content1'],
                ['file2', 'content2'],
                ['file3', 'content3'],
                )
        events = [i for i in ipc.post(['report'], {'context': 'context', 'error': 'error', 'logs': [
            tests.tmpdir + '/file1',
            tests.tmpdir + '/file2',
            tests.tmpdir + '/file3',
            ]}, cmd='submit')]
        self.assertEqual('done', events[-1]['event'])
        guid = events[-1]['guid']

        self.assertEqual({
            'context': 'context',
            'error': 'error',
            },
            ipc.get(['report', guid], reply=['context', 'error']))
        zipfile = ZipFile('db/report/%s/%s/data.blob' % (guid[:2], guid))
        self.assertEqual('content1', zipfile.read('file1'))
        self.assertEqual('content2', zipfile.read('file2'))
        self.assertEqual('content3', zipfile.read('file3'))


if __name__ == '__main__':
    tests.main()
