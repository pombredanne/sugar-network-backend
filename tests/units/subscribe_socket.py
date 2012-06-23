#!/usr/bin/env python
# sugar-lint: disable

import gevent
from gevent import socket

from __init__ import tests
from tests import Resource

import active_document as ad
import restful_document as rd
from active_toolkit import sockets, coroutine


class SubscribeSocketTest(tests.Test):

    def subscribe(self, host, port, ticket):
        conn = coroutine.socket()
        conn.connect((host, port))
        result = sockets.SocketFile(conn)
        result.write_message({'ticket': ticket})
        gevent.sleep(1)
        return result

    def test_Subscribe(self):
        rd.only_sync_notification.value = False

        self.httpd(8100, [tests.User, Document])
        rest = Resource('http://localhost:8100')

        with self.subscribe(**rest.post('/', None, cmd='subscribe')) as subscription:
            guid = rest.post('/document', {'prop': 'value'})
            self.assertEqual(
                    {'layer': ['public']},
                    rest.get('/document/' + guid, reply=['layer']))

            event = subscription.read_message()
            event.pop('props')
            self.assertEqual({
                'guid': guid,
                'document': 'document',
                'event': 'create',
                },
                event)

            rest.put('/document/' + guid, {'prop': 'value2'})
            self.assertEqual(
                    {'prop': 'value2'},
                    rest.get('/document/' + guid, reply=['prop']))

            event = subscription.read_message()
            event.pop('props')
            self.assertEqual({
                'event': 'update',
                'document': 'document',
                'guid': guid,
                },
                event)

            rest.delete('/document/' + guid)
            self.assertRaises(RuntimeError, rest.get, '/document/' + guid)

            self.assertEqual({
                'event': 'delete',
                'document': 'document',
                'guid': guid,
                },
                subscription.read_message())

            self.assertRaises(socket.timeout, socket.wait_read, subscription.fileno(), 1)

    def test_OnlySyncEvents(self):
        rd.only_sync_notification.value = True

        self.httpd(8100, [tests.User, Document])
        rest = Resource('http://localhost:8100')

        with self.subscribe(**rest.post('/', None, cmd='subscribe')) as subscription:
            guid = rest.post('/document', {'prop': 'value'})

            self.assertEqual({
                'document': 'document',
                'event': 'sync',
                'seqno': 1,
                },
                subscription.read_message())

            rest.put('/document/' + guid, {'prop': 'value2'})

            self.assertEqual({
                'document': 'document',
                'event': 'sync',
                'seqno': 2,
                },
                subscription.read_message())

            rest.delete('/document/' + guid)

            self.assertEqual({
                'document': 'document',
                'event': 'sync',
                'seqno': 3,
                },
                subscription.read_message())

            self.assertRaises(socket.timeout, socket.wait_read, subscription.fileno(), 1)


class Document(ad.Document):

    @ad.active_property(slot=1, prefix='A', default='')
    def prop(self, value):
        return value


if __name__ == '__main__':
    tests.main()
