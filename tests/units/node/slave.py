#!/usr/bin/env python
# sugar-lint: disable

import os
import json
import time
import hashlib
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection, keyfile
from sugar_network.node import master_api
from sugar_network.node.master import MasterRoutes
from sugar_network.node.slave import SlaveRoutes
from sugar_network.db.volume import Volume
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, File
from sugar_network.toolkit import coroutine, http, parcel


class SlaveTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class statvfs(object):
            f_blocks = 100
            f_bfree = 999999999
            f_frsize = 1

        self.statvfs = statvfs
        self.override(os, 'statvfs', lambda *args: statvfs())

        class Document(db.Resource):

            @db.indexed_property(db.Localized, slot=1, prefix='N', full_text=True)
            def title(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='D', full_text=True)
            def message(self, value):
                return value

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        self.Document = Document
        self.slave_volume = Volume('slave', [User, Document])
        self.slave_routes = SlaveRoutes(volume=self.slave_volume)
        self.slave_server = coroutine.WSGIServer(('127.0.0.1', 8888), Router(self.slave_routes))
        coroutine.spawn(self.slave_server.serve_forever)
        coroutine.dispatch()

    def test_online_sync_Push(self):
        self.fork_master([User, self.Document])
        master = Connection('http://127.0.0.1:7777', auth=http.SugarAuth(keyfile.value))
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        slave.post(cmd='online_sync')
        self.assertEqual([[1, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[1, None]], json.load(file('slave/var/push.ranges')))

        guid1 = slave.post(['document'], {'message': '1', 'title': ''})
        guid2 = slave.post(['document'], {'message': '2', 'title': ''})

        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'message': '1'},
            {'guid': guid2, 'message': '2'},
            ],
            master.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[2, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[5, None]], json.load(file('slave/var/push.ranges')))

        guid3 = slave.post(['document'], {'message': '3', 'title': ''})
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'message': '1'},
            {'guid': guid2, 'message': '2'},
            {'guid': guid3, 'message': '3'},
            ],
            master.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[3, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[6, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        slave.put(['document', guid2], {'message': '22'})
        slave.post(cmd='online_sync')
        self.assertEqual('22', master.get(['document', guid2, 'message']))
        self.assertEqual([[4, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[7, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        slave.delete(['document', guid1])
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid2, 'message': '22'},
            {'guid': guid3, 'message': '3'},
            ],
            master.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[5, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[8, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        slave.put(['document', guid1], {'message': 'a'})
        slave.put(['document', guid2], {'message': 'b'})
        slave.put(['document', guid3], {'message': 'c'})
        guid4 = slave.post(['document'], {'message': 'd', 'title': ''})
        slave.delete(['document', guid2])
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid3, 'message': 'c'},
            {'guid': guid4, 'message': 'd'},
            ],
            master.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[6, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[13, None]], json.load(file('slave/var/push.ranges')))

    def test_online_sync_Pull(self):
        self.fork_master([User, self.Document])
        master = Connection('http://127.0.0.1:7777', auth=http.SugarAuth(keyfile.value))
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        slave.post(cmd='online_sync')
        self.assertEqual([[1, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[1, None]], json.load(file('slave/var/push.ranges')))

        guid1 = master.post(['document'], {'message': '1', 'title': ''})
        guid2 = master.post(['document'], {'message': '2', 'title': ''})

        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'message': '1'},
            {'guid': guid2, 'message': '2'},
            ],
            slave.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[5, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[2, None]], json.load(file('slave/var/push.ranges')))

        guid3 = master.post(['document'], {'message': '3', 'title': ''})
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid1, 'message': '1'},
            {'guid': guid2, 'message': '2'},
            {'guid': guid3, 'message': '3'},
            ],
            slave.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[6, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[3, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        master.put(['document', guid2], {'message': '22'})
        slave.post(cmd='online_sync')
        self.assertEqual('22', slave.get(['document', guid2, 'message']))
        self.assertEqual([[7, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[4, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        master.delete(['document', guid1])
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid2, 'message': '22'},
            {'guid': guid3, 'message': '3'},
            ],
            slave.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[8, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[5, None]], json.load(file('slave/var/push.ranges')))

        coroutine.sleep(1)
        master.put(['document', guid1], {'message': 'a'})
        master.put(['document', guid2], {'message': 'b'})
        master.put(['document', guid3], {'message': 'c'})
        guid4 = master.post(['document'], {'message': 'd', 'title': ''})
        master.delete(['document', guid2])
        slave.post(cmd='online_sync')
        self.assertEqual([
            {'guid': guid3, 'message': 'c'},
            {'guid': guid4, 'message': 'd'},
            ],
            slave.get(['document'], reply=['guid', 'message'])['result'])
        self.assertEqual([[13, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[6, None]], json.load(file('slave/var/push.ranges')))

    def test_online_sync_PullBlobs(self):
        self.fork_master([User, self.Document])
        master = Connection('http://127.0.0.1:7777', auth=http.SugarAuth(keyfile.value))
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        slave.post(cmd='online_sync')
        self.assertEqual([[1, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[1, None]], json.load(file('slave/var/push.ranges')))

        guid = master.post(['document'], {'message': '1', 'title': ''})
        master.put(['document', guid, 'blob'], 'blob')
        self.touch(('master/files/foo/bar', 'file'))

        slave.post(cmd='online_sync')
        self.assertEqual('blob', slave.request('GET', ['document', guid, 'blob']).content)
        self.assertEqual('file', file('slave/files/foo/bar').read())

    def test_online_sync_PullFromPreviouslyMergedRecord(self):
        self.fork_master([User, self.Document])
        master = Connection('http://127.0.0.1:7777', auth=http.SugarAuth(keyfile.value))
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        slave.post(cmd='online_sync')
        self.assertEqual([[1, None]], json.load(file('slave/var/pull.ranges')))
        self.assertEqual([[1, None]], json.load(file('slave/var/push.ranges')))

        guid = slave.post(['document'], {'message': '1', 'title': '1'})
        slave.post(cmd='online_sync')

        coroutine.sleep(1)
        master.put(['document', guid], {'message': '1_'})
        slave.put(['document', guid], {'title': '1_'})
        slave.post(cmd='online_sync')

        self.assertEqual('1_', master.get(['document', guid, 'message']))
        self.assertEqual('1_', master.get(['document', guid, 'title']))
        self.assertEqual('1_', slave.get(['document', guid, 'message']))
        self.assertEqual('1_', slave.get(['document', guid, 'title']))

    def test_offline_sync_Import(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        self.touch(('blob1', 'a'))
        self.touch(('blob2', 'bb'))
        parcel.encode_dir([
            ('push', {'from': '127.0.0.1:7777'}, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    'title': {'value': {}, 'mtime': 0},
                    'message': {'value': {}, 'mtime': 0},
                    }},
                File('./blob1', meta={'content-length': '1'}),
                File('./blob2', meta={'content-length': '2', 'path': 'foo/bar'}),
                {'commit': [[1, 2]]},
                ]),
            ('ack', {'ack': [[101, 103]], 'ranges': [[1, 3]], 'from': '127.0.0.1:7777', 'to': self.slave_routes.guid}, []),
            ],
            root='sync', limit=99999999)
        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')

        self.assertEqual(1, slave.get(['document', '1', 'ctime']))
        self.assertEqual('a', file(self.slave_volume.blobs.get(hashlib.sha1('a').hexdigest()).path).read())
        self.assertEqual('bb', file(self.slave_volume.blobs.get('foo/bar').path).read())
        self.assertEqual([[4, None]], json.load(file('slave/var/push.ranges')))
        self.assertEqual([[3, 100], [104, None]], json.load(file('slave/var/pull.ranges')))

        self.assertEqual(
                sorted([
                    ({'from': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': '1', 'patch': {
                            'guid': {'value': '1', 'mtime': 0},
                            'ctime': {'value': 1, 'mtime': 0},
                            'mtime': {'value': 1, 'mtime': 0},
                            'title': {'value': {}, 'mtime': 0},
                            'message': {'value': {}, 'mtime': 0},
                            }},
                        {'content-length': '1'},
                        {'content-length': '2', 'path': 'foo/bar'},
                        {'commit': [[1, 2]]},
                        ]),
                    ({'ack': [[101, 103]], 'from': '127.0.0.1:7777', 'packet': 'ack', 'ranges': [[1, 3]], 'to': self.slave_routes.guid}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'push', 'to': '127.0.0.1:7777'}, [
                        {'resource': 'document'},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[3, 100], [104, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

    def test_offline_sync_ImportPush(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        self.touch(('blob1', 'a'))
        self.touch(('blob2', 'bb'))
        parcel.encode_dir([
            ('push', {'from': '127.0.0.1:7777'}, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    'title': {'value': {}, 'mtime': 0},
                    'message': {'value': {}, 'mtime': 0},
                    }},
                File('./blob1', meta={'content-length': '1'}),
                File('./blob2', meta={'content-length': '2', 'path': 'foo/bar'}),
                {'commit': [[1, 2]]},
                ]),
            ],
            root='sync', limit=99999999)
        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')

        self.assertEqual(1, slave.get(['document', '1', 'ctime']))
        self.assertEqual('a', file(self.slave_volume.blobs.get(hashlib.sha1('a').hexdigest()).path).read())
        self.assertEqual('bb', file(self.slave_volume.blobs.get('foo/bar').path).read())
        self.assertEqual([[2, None]], json.load(file('slave/var/push.ranges')))
        self.assertEqual([[3, None]], json.load(file('slave/var/pull.ranges')))

        self.assertEqual(
                sorted([
                    ({'from': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': '1', 'patch': {
                            'guid': {'value': '1', 'mtime': 0},
                            'ctime': {'value': 1, 'mtime': 0},
                            'mtime': {'value': 1, 'mtime': 0},
                            'title': {'value': {}, 'mtime': 0},
                            'message': {'value': {}, 'mtime': 0},
                            }},
                        {'content-length': '1'},
                        {'content-length': '2', 'path': 'foo/bar'},
                        {'commit': [[1, 2]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'push', 'to': '127.0.0.1:7777'}, [
                        {'resource': 'document'},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[3, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [dict(i) for i in packet]) for packet in parcel.decode_dir('sync')]))

    def test_offline_sync_ImportAck(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        parcel.encode_dir([
            ('ack', {'ack': [[101, 103]], 'ranges': [[1, 3]], 'from': '127.0.0.1:7777', 'to': self.slave_routes.guid}, []),
            ],
            root='sync', limit=99999999)
        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')

        self.assertEqual([[4, None]], json.load(file('slave/var/push.ranges')))
        self.assertEqual([[1, 100], [104, None]], json.load(file('slave/var/pull.ranges')))

        self.assertEqual(
                sorted([
                    ({'ack': [[101, 103]], 'from': '127.0.0.1:7777', 'packet': 'ack', 'ranges': [[1, 3]], 'to': self.slave_routes.guid}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'push', 'to': '127.0.0.1:7777'}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, 100], [104, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

    def test_offline_sync_GenerateRequestAfterImport(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        parcel.encode_dir([
            ('push', {'from': 'another-slave'}, [
                {'resource': 'document'},
                {'guid': '1', 'patch': {
                    'guid': {'value': '1', 'mtime': 0},
                    'ctime': {'value': 1, 'mtime': 0},
                    'mtime': {'value': 1, 'mtime': 0},
                    'title': {'value': {}, 'mtime': 0},
                    'message': {'value': {}, 'mtime': 0},
                    }},
                {'commit': [[1, 1]]},
                ]),
            ],
            root='sync', limit=99999999)
        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')

        self.assertEqual(1, slave.get(['document', '1', 'ctime']))
        self.assertEqual([[2, None]], json.load(file('slave/var/push.ranges')))
        self.assertEqual([[1, None]], json.load(file('slave/var/pull.ranges')))

        self.assertEqual(
                sorted([
                    ({'from': 'another-slave', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': '1', 'patch': {
                            'guid': {'value': '1', 'mtime': 0},
                            'ctime': {'value': 1, 'mtime': 0},
                            'mtime': {'value': 1, 'mtime': 0},
                            'title': {'value': {}, 'mtime': 0},
                            'message': {'value': {}, 'mtime': 0},
                            }},
                        {'commit': [[1, 1]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'request', 'to': '127.0.0.1:7777', 'origin': 'another-slave', 'ranges': [[1, 1]]}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'push', 'to': '127.0.0.1:7777'}, [
                        {'resource': 'document'},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

    def test_offline_sync_Export(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        class statvfs(object):

            f_bfree = None
            f_frsize = 1

        self.override(os, 'statvfs', lambda *args: statvfs())
        statvfs.f_bfree = 999999999
        self.override(time, 'time', lambda: 0)

        guid = slave.post(['document'], {'message': '', 'title': ''})
        push_seqno = self.slave_volume.seqno.value + 1
        self.slave_routes._push_r.value = [[push_seqno, None]]
        slave.put(['document', guid, 'title'], 'probe')
        self.slave_volume.blobs.post('a')
        self.touch(('slave/files/foo/bar', 'bb'))
        self.slave_volume.blobs.populate()

        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')

        self.assertEqual(
                sorted([
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': 'probe'}, 'mtime': self.slave_volume['document'].get(guid).meta('title')['mtime']},
                            }},
                        {'resource': 'user'},
                        {'content-length': '1', 'content-type': 'application/octet-stream'},
                        {'commit': [[push_seqno, push_seqno + 1]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

    def test_offline_sync_ContinuousExport(self):
        slave = Connection('http://127.0.0.1:8888', auth=http.SugarAuth(keyfile.value))

        class statvfs(object):

            f_bfree = None
            f_frsize = 1

        self.override(os, 'statvfs', lambda *args: statvfs())
        self.override(time, 'time', lambda: 0)

        guid1 = slave.post(['document'], {'message': '', 'title': ''})
        guid2 = slave.post(['document'], {'message': '', 'title': ''})
        push_seqno = self.slave_volume.seqno.value + 1
        self.slave_routes._push_r.value = [[push_seqno, None]]

        RECORD = 1024 * 1024
        slave.put(['document', guid1, 'title'], '.' * RECORD)
        slave.put(['document', guid2, 'title'], '.' * RECORD)
        statvfs.f_bfree = parcel._RESERVED_DISK_SPACE + RECORD * 1.5

        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')
        self.assertEqual(
                sorted([
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid1, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid1).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': '.' * RECORD}, 'mtime': self.slave_volume['document'].get(guid1).meta('title')['mtime']},
                            }},
                        {'commit': [[push_seqno, push_seqno]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')
        self.assertEqual(
                sorted([
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid1, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid1).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': '.' * RECORD}, 'mtime': self.slave_volume['document'].get(guid1).meta('title')['mtime']},
                            }},
                        {'commit': [[push_seqno, push_seqno]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid2, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid2).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': '.' * RECORD}, 'mtime': self.slave_volume['document'].get(guid2).meta('title')['mtime']},
                            }},
                        {'resource': 'user'},
                        {'commit': [[push_seqno + 1, push_seqno + 1]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))

        slave.post(cmd='offline_sync', path=tests.tmpdir + '/sync')
        self.assertEqual(
                sorted([
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid1, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid1).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': '.' * RECORD}, 'mtime': self.slave_volume['document'].get(guid1).meta('title')['mtime']},
                            }},
                        {'commit': [[push_seqno, push_seqno]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'guid': guid2, 'patch': {
                            'mtime': {'value': 0, 'mtime': self.slave_volume['document'].get(guid2).meta('mtime')['mtime']},
                            'title': {'value': {'en-us': '.' * RECORD}, 'mtime': self.slave_volume['document'].get(guid2).meta('title')['mtime']},
                            }},
                        {'resource': 'user'},
                        {'commit': [[push_seqno + 1, push_seqno + 1]]},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ({'from': self.slave_routes.guid, 'to': '127.0.0.1:7777', 'packet': u'push'}, [
                        {'resource': 'document'},
                        {'resource': 'user'},
                        ]),
                    ({'from': self.slave_routes.guid, 'packet': 'pull', 'ranges': [[1, None]], 'to': '127.0.0.1:7777'}, [
                        ]),
                    ]),
                sorted([(packet.header, [i for i in packet]) for packet in parcel.decode_dir('sync')]))


if __name__ == '__main__':
    tests.main()
