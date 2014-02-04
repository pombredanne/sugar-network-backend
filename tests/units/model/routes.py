#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
from email.utils import formatdate
from os.path import exists

from __init__ import tests, src_root

from sugar_network import db, model
from sugar_network.db import files
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, Request
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import coroutine


class RoutesTest(tests.Test):

    def test_Subscribe(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        routes = model.FrontRoutes()
        volume = db.Volume('db', [Document])
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
            {'event': 'pong'},
            {'guid': 'guid', 'resource': 'document', 'event': 'create'},
            {'guid': 'guid', 'resource': 'document', 'event': 'update'},
            {'guid': 'guid', 'event': 'delete', 'resource': u'document'},
            ],
            events)

    def test_SubscribeWithPong(self):
        routes = model.FrontRoutes()
        for event in routes.subscribe():
            break
        self.assertEqual({'event': 'pong'}, event)


if __name__ == '__main__':
    tests.main()
