#!/usr/bin/env python
# sugar-lint: disable

import os
import socket
from os.path import exists, abspath

from __init__ import tests

from active_toolkit import sockets, coroutine
from sugar_network.resources.report import Report
from sugar_network import IPCClient


class HomeMountTest(tests.Test):

    def test_create(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertNotEqual(None, guid)

        res = local.get(['context', guid], reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(guid, res['guid'])
        self.assertEqual('title', res['title'])
        self.assertEqual(False, res['keep'])
        self.assertEqual(0, res['keep_impl'])
        self.assertEqual([-1, -1], res['position'])

    def test_update(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        local.put(['context', guid], {
            'title': 'title_2',
            'keep': True,
            'position': (2, 3),
            })

        context = local.get(['context', guid], reply=['title', 'keep', 'position'])
        self.assertEqual('title_2', context['title'])
        self.assertEqual(True, context['keep'])
        self.assertEqual([2, 3], context['position'])

    def test_find(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid_1 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_1',
            'summary': 'summary',
            'description': 'description',
            })
        guid_2 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_2',
            'summary': 'summary',
            'description': 'description',
            })
        guid_3 = local.post(['context'], {
            'type': 'activity',
            'title': 'title_3',
            'summary': 'summary',
            'description': 'description',
            })

        cursor = local.get(['context'], reply=['guid', 'title', 'keep', 'keep_impl', 'position'])
        self.assertEqual(3, cursor['total'])
        self.assertEqual(
                sorted([
                    (guid_1, 'title_1', False, 0, [-1, -1]),
                    (guid_2, 'title_2', False, 0, [-1, -1]),
                    (guid_3, 'title_3', False, 0, [-1, -1]),
                    ]),
                sorted([(i['guid'], i['title'], i['keep'], i['keep_impl'], i['position']) for i in cursor['result']]))

    def test_upload_blob(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.touch(('file', 'blob'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file'))
        blob = local.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob', file(blob['path']).read())

        self.touch(('file2', 'blob2'))
        local.put(['context', guid, 'preview'], cmd='upload_blob', path=abspath('file2'), pass_ownership=True)
        blob = local.get(['context', guid, 'preview'], cmd='get_blob')
        self.assertEqual('blob2', file(blob['path']).read())
        assert not exists('file2')

    def test_GetAbsetnBLOB(self):
        self.start_server([Report])
        local = IPCClient(mountpoint='~')

        guid = local.post(['report'], {
            'context': 'context',
            'implementation': 'implementation',
            'description': 'description',
            })

        self.assertEqual(None, local.get(['report', guid, 'data'], cmd='get_blob'))

    def test_GetDefaultBLOB(self):
        self.start_server()
        local = IPCClient(mountpoint='~')

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        blob = local.get(['context', guid, 'icon'], cmd='get_blob')
        assert blob['path'].endswith('missing.png')
        assert exists(blob['path'])

    def test_Subscription(self):
        self.start_server()
        local = IPCClient(mountpoint='~')
        events = []

        def read_events():
            for event in local.subscribe():
                if 'props' in event:
                    event.pop('props')
                events.append(event)
        job = coroutine.spawn(read_events)

        guid = local.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        coroutine.dispatch()
        local.put(['context', guid], {
            'title': 'title_2',
            })
        coroutine.dispatch()
        local.delete(['context', guid])
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': guid, 'seqno': 1, 'document': 'context', 'event': 'create'},
            {'guid': guid, 'seqno': 2, 'document': 'context', 'event': 'update', 'mountpoint': '~'},
            {'guid': guid, 'event': 'delete', 'document': 'context', 'mountpoint': '~'},
            ],
            events)


if __name__ == '__main__':
    tests.main()
