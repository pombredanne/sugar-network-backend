#!/usr/bin/env python
# sugar-lint: disable

import os
import gzip
import time
import json
import base64
import hashlib
from glob import glob
from os.path import join, exists, basename
from StringIO import StringIO
from base64 import b64decode, b64encode

import rrdtool

from __init__ import tests

from sugar_network.client import Connection, keyfile, api
from sugar_network.db.directory import Directory
from sugar_network import db, node, toolkit
from sugar_network.node.master import MasterRoutes
from sugar_network.db.volume import Volume
from sugar_network.model.user import User
from sugar_network.toolkit.router import Response, File
from sugar_network.toolkit import coroutine, parcel, http


class MasterTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        def next_uuid():
            self.uuid += 1
            return self.uuid

        self.uuid = 0
        self.override(toolkit, 'uuid', next_uuid)

    def test_push(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        self.touch(('blob1', '1'))
        self.touch(('blob2', '2'))

        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    }},
                File('./blob1', meta={'content-length': '1'}),
                File('./blob2', meta={'content-length': '1', 'path': 'foo/bar'}),
                {'commit': [[1, 3]]},
                ]),
            ], header={'to': self.node_routes.guid, 'from': 'slave'}))
        response = conn.request('POST', [], patch, params={'cmd': 'push'})
        reply = parcel.decode(response.raw)

        assert volume['document'].exists('1')
        blob = volume.blobs.get(hashlib.sha1('1').hexdigest())
        self.assertEqual('1', ''.join(blob.iter_content()))
        blob = volume.blobs.get('foo/bar')
        self.assertEqual('2', ''.join(blob.iter_content()))

        self.assertEqual({
            'packet': 'ack',
            'from': self.node_routes.guid,
            'to': 'slave',
            'ack': [[1, 1]],
            'ranges': [[1, 3]],
            },
            next(reply).header)
        self.assertRaises(StopIteration, next, reply)

        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'ack': {
                        'slave': [[[[1, 3]], [[1, 1]]]],
                        },
                    })),
                response.headers['set-cookie'])

    def test_push_MisaddressedPackets(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ], header={'from': 'slave'}))
        self.assertRaises(http.BadRequest, conn.request, 'POST', [], patch, params={'cmd': 'push'})

        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ], header={'to': 'fake', 'from': 'slave'}))
        self.assertRaises(http.BadRequest, conn.request, 'POST', [], patch, params={'cmd': 'push'})

        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ], header={'to': '127.0.0.1:7777', 'from': 'slave'}))
        conn.request('POST', [], patch, params={'cmd': 'push'})

    def test_push_WithCookies(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        self.touch(('blob', 'blob'))

        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 1},
                    'ctime': {'value': 1, 'mtime': 1},
                    'mtime': {'value': 1, 'mtime': 1},
                    }},
                File('./blob', meta={'content-length': str(len('blob'))}),
                {'commit': [[1, 2]]},
                ]),
            ], header={'to': self.node_routes.guid, 'from': 'slave'}))
        response = conn.request('POST', [], patch, params={'cmd': 'push'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'ack': {
                    'slave': [[[[100, 100]], [[200, 200]]]],
                    },
                })),
            })
        reply = parcel.decode(response.raw)

        assert volume['document'].exists('1')
        blob_digest = hashlib.sha1('blob').hexdigest()
        blob = volume.blobs.get(blob_digest)
        self.assertEqual('blob', ''.join(blob.iter_content()))

        self.assertEqual({
            'packet': 'ack',
            'from': self.node_routes.guid,
            'to': 'slave',
            'ack': [[1, 1]],
            'ranges': [[1, 2]],
            },
            next(reply).header)
        self.assertRaises(StopIteration, next, reply)

        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'ack': {
                        'slave': [[[[100, 100]], [[200, 200]]], [[[1, 2]], [[1, 1]]]],
                        },
                    })),
                response.headers['set-cookie'])

    def test_push_Forward(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))
        self.touch(('blob', 'blob'))

        patch = ''.join(parcel.encode([
            ('pull', {'ranges': [[1, None]]}, []),
            ('request', {'for': 1}, []),
            ], header={'to': self.node_routes.guid, 'from': 'slave'}))
        response = conn.request('POST', [], patch, params={'cmd': 'push'})
        reply = parcel.decode(response.raw)
        self.assertRaises(StopIteration, next, reply)

        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'pull': [[1, None]],
                    'ack': {'slave': []},
                    'request': [
                        {'to': '127.0.0.1:7777', 'from': 'slave', 'packet': 'request', 'for': 1},
                        ],
                    })),
                response.headers['set-cookie'])

    def test_pull(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': 'guid', 'ctime': 1, 'mtime': 1})
        self.utime('master/db/document/gu/guid', 1)
        blob = volume.blobs.post('a')
        self.touch(('master/files/foo/bar', 'bb'))

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'guid': 'guid', 'patch': {
                    'ctime': {'mtime': 1, 'value': 1},
                    'guid': {'mtime': 1, 'value': 'guid'},
                    'mtime': {'mtime': 1, 'value': 1},
                    }},
                {'content-length': '1', 'content-type': 'application/octet-stream'},
                {'content-length': '2', 'content-type': 'application/octet-stream', 'path': 'foo/bar'},
                {'commit': [[1, 3]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])
        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'id': 1,
                    'pull': [[1, None]],
                    })),
                response.headers['set-cookie'])

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={'cookie': response.headers['set-cookie']})
        assert not response.raw.read()
        self.assertEqual(
                'sugar_network_node=unset_sugar_network_node; Max-Age=3600; HttpOnly',
                response.headers['set-cookie'])

    def test_pull_ExcludeAcks(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('master/db/document/1/1', 1)
        blob = volume.blobs.post('blob')

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                'ack': {
                    'node': [[[[0, 0]], [[1, 1]]], [[[0, 0]], [[2, 2]]]],
                    },
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [{'resource': 'document'}]),
            ],
            [(packet.header, [record for record in packet]) for packet in parcel.decode(response.raw)])
        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'id': 1,
                    'pull': [[1, None]],
                    'ack': {
                        'node': [[[[0, 0]], [[1, 1]]], [[[0, 0]], [[2, 2]]]],
                        },
                    })),
                response.headers['set-cookie'])

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={'cookie': response.headers['set-cookie']})
        assert not response.raw.read()
        self.assertEqual(
                'sugar_network_node=unset_sugar_network_node; Max-Age=3600; HttpOnly',
                response.headers['set-cookie'])

    def test_pull_ExcludeOnlyAcksIntersection(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('master/db/document/1/1', 1)
        blob1 = volume.blobs.post('a')
        volume['document'].create({'guid': '2', 'ctime': 2, 'mtime': 2})
        self.utime('master/db/document/2/2', 2)
        blob2 = volume.blobs.post('bb')

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                'ack': {
                    'node1': [[[[0, 0]], [[1, 4]]]],
                    'node2': [[[[0, 0]], [[1, 4]]]],
                    },
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [{'resource': 'document'}]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                'ack': {
                    'node1': [[[[0, 0]], [[1, 4]]]],
                    'node2': [[[[0, 0]], [[2, 4]]]],
                    },
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'mtime': 1, 'value': '1'},
                    'ctime': {'mtime': 1, 'value': 1},
                    'mtime': {'mtime': 1, 'value': 1},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                'ack': {
                    'node1': [[[[0, 0]], [[1, 4]]]],
                    'node2': [[[[0, 0]], [[1, 3]]]],
                    },
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'content-length': '2', 'content-type': 'application/octet-stream'},
                {'commit': [[4, 4]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])

    def test_pull_ExcludeAckRequests(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1})
        self.utime('master/db/document/1/1', 1)
        blob = volume.blobs.post('blob')

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                'ack': {
                    'node1': [[[[0, 0]], [[1, 2]]]],
                    'node2': [],
                    },
                'request': [
                    {'from': 'node2', 'origin': 'node1', 'ranges': [[0, 0]]},
                    ],
                }))
            })
        reply = parcel.decode(response.raw)
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'to': 'node2', 'packet': 'ack', 'ack': [[1, 2]]}, []),
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [{'resource': 'document'}]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in reply])

    def test_pull_Limitted(self):
        RECORD = 1024 * 1024

        class Document(db.Resource):

            @db.stored_property()
            def prop(self):
                pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': '1', 'ctime': 1, 'mtime': 1, 'prop': '.' * RECORD})
        self.utime('master/db/document/1/1', 1)
        volume['document'].create({'guid': '2', 'ctime': 2, 'mtime': 2, 'prop': '.' * RECORD})
        self.utime('master/db/document/2/2', 2)
        volume['document'].create({'guid': '3', 'ctime': 3, 'mtime': 3, 'prop': '.' * RECORD})
        self.utime('master/db/document/3/3', 3)

        response = conn.request('GET', [], params={'cmd': 'pull', 'accept_length': int(RECORD * .5)}, headers={
            'cookie': 'sugar_network_node=%s' % b64encode(json.dumps({
                'pull': [[1, None]],
                }))
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])
        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'id': 1,
                    'pull': [[1, None]],
                    })),
                response.headers['set-cookie'])

        response = conn.request('GET', [], params={'cmd': 'pull', 'accept_length': int(RECORD * 1.5)}, headers={
            'cookie': response.headers['set-cookie'],
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'ctime': {'mtime': 1, 'value': 1},
                    'guid': {'mtime': 1, 'value': '1'},
                    'mtime': {'mtime': 1, 'value': 1},
                    'prop': {'mtime': 1, 'value': '.' * RECORD},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])
        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'id': 1,
                    'pull': [[1, None]],
                    })),
                response.headers['set-cookie'])

        response = conn.request('GET', [], params={'cmd': 'pull', 'accept_length': int(RECORD * 2.5)}, headers={
            'cookie': response.headers['set-cookie'],
            })
        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'guid': '2', 'patch': {
                    'ctime': {'mtime': 2, 'value': 2},
                    'guid': {'mtime': 2, 'value': '2'},
                    'mtime': {'mtime': 2, 'value': 2},
                    'prop': {'mtime': 2, 'value': '.' * RECORD},
                    }},
                {'guid': '3', 'patch': {
                    'ctime': {'mtime': 3, 'value': 3},
                    'guid': {'mtime': 3, 'value': '3'},
                    'mtime': {'mtime': 3, 'value': 3},
                    'prop': {'mtime': 3, 'value': '.' * RECORD},
                    }},
                {'commit': [[2, 3]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])
        self.assertEqual(
                'sugar_network_node=%s; Max-Age=3600; HttpOnly' % b64encode(json.dumps({
                    'id': 1,
                    'pull': [[2, None]],
                    })),
                response.headers['set-cookie'])

        response = conn.request('GET', [], params={'cmd': 'pull'}, headers={
            'cookie': response.headers['set-cookie'],
            })
        assert not response.raw.read()
        self.assertEqual(
                'sugar_network_node=unset_sugar_network_node; Max-Age=3600; HttpOnly',
                response.headers['set-cookie'])

    def test_sync(self):

        class Document(db.Resource):
            pass

        volume = self.start_master([User, Document])
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        volume['document'].create({'guid': 'guid', 'ctime': 1, 'mtime': 1})
        self.utime('master/db/document/gu/guid', 1)
        blob1 = volume.blobs.post('1')

        self.touch(('blob2', 'ccc'))
        patch = ''.join(parcel.encode([
            ('push', None, [
                {'resource': 'document'},
                {'guid': '2', 'patch': {
                    'guid': {'value': '2', 'mtime': 2},
                    'ctime': {'value': 2, 'mtime': 2},
                    'mtime': {'value': 2, 'mtime': 2},
                    }},
                File('./blob2', meta={'content-length': '3'}),
                {'commit': [[1, 2]]},
                ]),
            ('pull', {'ranges': [[1, None]]}, []),
            ], header={'to': '127.0.0.1:7777', 'from': 'node'}))
        response = conn.request('POST', [], patch, params={'cmd': 'sync'})
        blob2 = volume.blobs.get(hashlib.sha1('ccc').hexdigest())

        self.assertEqual([
            ({'from': '127.0.0.1:7777', 'to': 'node', 'packet': 'ack', 'ack': [[3, 3]], 'ranges': [[1, 2]]}, []),
            ({'from': '127.0.0.1:7777', 'packet': 'push'}, [
                {'resource': 'document'},
                {'guid': 'guid', 'patch': {
                    'ctime': {'mtime': 1, 'value': 1},
                    'guid': {'mtime': 1, 'value': 'guid'},
                    'mtime': {'mtime': 1, 'value': 1},
                    }},
                {'content-length': '1', 'content-type': 'application/octet-stream'},
                {'commit': [[1, 2]]},
                ]),
            ],
            [(packet.header, [dict(record) for record in packet]) for packet in parcel.decode(response.raw)])

        assert volume['document'].exists('2')
        self.assertEqual('ccc', ''.join(blob2.iter_content()))


if __name__ == '__main__':
    tests.main()
