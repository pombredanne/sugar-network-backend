#!/usr/bin/env python
# sugar-lint: disable

import json
import hashlib
import tempfile

from gevent import socket

from __init__ import tests

import active_document as ad
from sugar_network import node
from active_toolkit import sockets, coroutine, util


class SubscribeSocketTest(tests.Test):

    def subscribe(self, host, port, ticket):
        conn = coroutine.socket()
        conn.connect((host, port))
        result = sockets.SocketFile(conn)
        result.write_message({'ticket': ticket})
        coroutine.sleep(1)
        return result

    def test_Subscribe(self):
        node.only_sync_notification.value = False

        self.fork(self.restful_server, [User, Document])
        rest = tests.Request('http://localhost:8800')

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
        node.only_sync_notification.value = True

        self.fork(self.restful_server, [User, Document])
        rest = tests.Request('http://localhost:8800')

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

    @ad.active_property(ad.StoredProperty, default='')
    def author(self, value):
        return value


class User(ad.Document):

    @ad.active_property(ad.StoredProperty)
    def pubkey(self, value):
        return value

    @ad.active_property(ad.StoredProperty, default='')
    def name(self, value):
        return value

    @classmethod
    def before_create(cls, props):
        ssh_pubkey = props['pubkey'].split()[1]
        props['guid'] = str(hashlib.sha1(ssh_pubkey).hexdigest())

        with tempfile.NamedTemporaryFile() as tmp_pubkey:
            tmp_pubkey.file.write(props['pubkey'])
            tmp_pubkey.file.flush()

            pubkey_pkcs8 = util.assert_call(
                    ['ssh-keygen', '-f', tmp_pubkey.name, '-e', '-m', 'PKCS8'])
            props['pubkey'] = pubkey_pkcs8

        super(User, cls).before_create(props)


if __name__ == '__main__':
    tests.main()
