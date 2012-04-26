#!/usr/bin/env python
# sugar-lint: disable

import time
import json
from os.path import join

import gevent
from gevent.wsgi import WSGIServer

from __init__ import tests

import active_document as ad
import restful_document as rd
from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

from local_document import env
from local_document.commands import OfflineCommands, OnlineCommands
from local_document.ipc_server import Server


class OnlineCommandsTest(tests.Test):

    def restful_server(self):
        ad.data_root.value = tests.tmpdir + '/remote'
        ad.index_flush_timeout.value = 0
        ad.index_flush_threshold.value = 1
        ad.find_limit.value = 1024
        ad.index_write_queue.value = 10

        folder = ad.SingleFolder([User, Context])
        httpd = WSGIServer(('localhost', 8000), rd.Router(folder))
        httpd.serve_forever()

    def test_GetKeeps(self):
        self.fork(self.restful_server)

        ad.data_root.value = tests.tmpdir + '/local'
        env.api_url.value = 'http://localhost:8000'
        folder = ad.SingleFolder([User, Context])
        online = OnlineCommands(folder)
        offline = OfflineCommands(folder)

        props = {'type': 'activity',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }

        guid = online.create(None, 'context', props)['guid']

        self.assertEqual(False, online.get(None, 'context', guid)['keep'])
        self.assertEqual(
                [(guid, False)],
                [(i['guid'], i['keep']) for i in online.find(None, 'context')['result']])

        context = Context(**props)
        context.set('guid', guid, raw=True)
        context.post()

        self.assertEqual(True, online.get(None, 'context', guid)['keep'])
        self.assertEqual(
                [(guid, True)],
                [(i['guid'], i['keep']) for i in online.find(None, 'context')['result']])

    def test_SetKeeps(self):
        self.fork(self.restful_server)

        ad.data_root.value = tests.tmpdir + '/local'
        env.api_url.value = 'http://localhost:8000'
        folder = ad.SingleFolder([User, Context])
        online = OnlineCommands(folder)
        offline = OfflineCommands(folder)

        props = {'type': ['activity'],
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }

        guid = online.create(None, 'context', props)['guid']
        self.assertEqual(False, online.get(None, 'context', guid)['keep'])
        assert not Context(guid).exists

        online.update(None, 'context', guid, {'keep': False})
        self.assertEqual(False, online.get(None, 'context', guid)['keep'])
        assert not Context(guid).exists

        online.update(None, 'context', guid, {'keep': True})
        self.assertEqual(True, online.get(None, 'context', guid)['keep'])
        assert Context(guid).exists
        self.assertEqual(props, Context(guid).properties(['type', 'title', 'summary', 'description']))

        online.update(None, 'context', guid, {'keep': False})
        self.assertEqual(False, online.get(None, 'context', guid)['keep'])
        assert not Context(guid).exists

        props['keep'] = True
        guid_2 = online.create(None, 'context', props)['guid']
        assert guid_2 != guid
        self.assertEqual(True, online.get(None, 'context', guid_2)['keep'])
        assert Context(guid_2).exists
        self.assertEqual(props, Context(guid_2).properties(['type', 'title', 'summary', 'description']))

        online.delete(None, 'context', guid_2)
        self.assertRaises(RuntimeError, online.get, None, 'context', guid_2)
        assert not Context(guid_2).exists


if __name__ == '__main__':
    tests.main()
