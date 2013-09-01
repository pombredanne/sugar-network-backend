#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
from email.utils import formatdate
from os.path import exists

from __init__ import tests, src_root

from sugar_network import db, model
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router
from sugar_network.toolkit import coroutine


class RoutesTest(tests.Test):

    def test_StaticFiles(self):
        router = Router(model.FrontRoutes())
        local_path = src_root + '/sugar_network/static/httpdocs/images/missing.png'

        response = []
        reply = router({
            'PATH_INFO': '/static/images/missing.png',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = file(local_path).read()
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {
                'last-modified': formatdate(os.stat(local_path).st_mtime, localtime=False, usegmt=True),
                'content-length': str(len(result)),
                'content-type': 'image/png',
                'content-disposition': 'attachment; filename="missing.png"',
                }
            ],
            response)

    def test_Subscribe(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        routes = model.FrontRoutes()
        volume = db.Volume('db', [Document], routes.broadcast)
        events = []

        def read_events():
            for event in routes.subscribe(event='!commit'):
                events.append(event)

        job = coroutine.spawn(read_events)
        coroutine.dispatch()
        volume['document'].create({'guid': 'guid', 'prop': 'value1'})
        coroutine.dispatch()
        volume['document'].update('guid', {'prop': 'value2'})
        coroutine.dispatch()
        volume['document'].delete('guid')
        coroutine.dispatch()
        volume['document'].commit()
        coroutine.sleep(.5)
        job.kill()

        self.assertEqual([
            {'guid': 'guid', 'resource': 'document', 'event': 'create'},
            {'guid': 'guid', 'resource': 'document', 'event': 'update'},
            {'guid': 'guid', 'event': 'delete', 'resource': u'document'},
            ],
            events)

    def test_SubscribeWithPong(self):
        routes = model.FrontRoutes()
        for event in routes.subscribe(ping=True):
            break
        self.assertEqual({'event': 'pong'}, event)


if __name__ == '__main__':
    tests.main()
