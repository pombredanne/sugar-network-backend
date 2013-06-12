#!/usr/bin/env python
# sugar-lint: disable

import os

import xapian

from __init__ import tests

from sugar_network import db
from sugar_network.db.router import Router, route
from sugar_network.resources import implementation
from sugar_network.resources.volume import Volume
from sugar_network.resources.implementation import _encode_version, Implementation
from sugar_network.node.commands import NodeCommands
from sugar_network.client import IPCClient
from sugar_network.toolkit import http, coroutine


class ImplementationTest(tests.Test):

    def test_encode_version(self):
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''5''000')),
                _encode_version('1'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0002''0000''5''000')),
                _encode_version('1.2'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0020''0300''5''000')),
                _encode_version('1.20.300'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0020''0300''5''000')),
                _encode_version('1.20.300.4444'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''9999''0000''5''000')),
                _encode_version('10001.99999.10000'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''3''000')),
                _encode_version('1-pre'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''4''000')),
                _encode_version('1-rc'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''5''000')),
                _encode_version('1-'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''000')),
                _encode_version('1-post'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''3''001')),
                _encode_version('1-pre1'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''4''002')),
                _encode_version('1-rc2'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''003')),
                _encode_version('1-post3'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''000')),
                _encode_version('1-post-2-3'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''001')),
                _encode_version('1-post1.2-3'))

    def test_ActivitityFiles(self):
        self.start_online_client()
        client = IPCClient()

        context = client.post(['context'], {
            'type': 'content',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        client.request('PUT', ['implementation', impl, 'data'], 'blob', {'Content-Type': 'image/png'})
        self.assertEqual('image/png', self.node_volume['implementation'].get(impl).meta('data')['mime_type'])

        client.put(['context', context, 'type'], 'activity')
        client.request('PUT', ['implementation', impl, 'data'], self.zips(('topdir/probe', 'probe')))

        data = self.node_volume['implementation'].get(impl).meta('data')
        self.assertEqual('application/vnd.olpc-sugar', data['mime_type'])
        self.assertNotEqual(5, data['blob_size'])
        self.assertEqual(5, data.get('unpack_size'))

    def test_ActivityUrls(self):
        bundle = self.zips(('topdir/probe', 'probe'))
        unpack_size = len('probe')

        class Files(db.CommandsProcessor):

            @route('GET', '/bundle')
            def bundle(self, request, response):
                return bundle

        self.start_online_client()
        client = IPCClient()
        files_server = coroutine.WSGIServer(('127.0.0.1', 9999), Router(Files()))
        coroutine.spawn(files_server.serve_forever)
        coroutine.dispatch()

        context = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        client.put(['implementation', impl, 'data'], {'url': 'http://127.0.0.1:9999/bundle'})

        data = self.node_volume['implementation'].get(impl).meta('data')
        self.assertEqual('application/vnd.olpc-sugar', data['mime_type'])
        self.assertEqual(len(bundle), data['blob_size'])
        self.assertEqual(unpack_size, data.get('unpack_size'))
        self.assertEqual('http://127.0.0.1:9999/bundle', data['url'])
        assert 'blob' not in data

    def test_ActivityASLOUrls(self):
        implementation._ASLO_PATH = '.'
        bundle = self.zips(('topdir/probe', 'probe'))
        with file('bundle', 'w') as f:
            f.write(bundle)
        unpack_size = len('probe')

        self.start_online_client()
        client = IPCClient()

        context = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        impl = client.post(['implementation'], {
            'context': context,
            'license': 'GPLv3+',
            'version': '1',
            'stability': 'stable',
            'notes': '',
            })
        client.put(['implementation', impl, 'data'], {'url': 'http://download.sugarlabs.org/activities/bundle'})

        data = self.node_volume['implementation'].get(impl).meta('data')
        self.assertEqual('application/vnd.olpc-sugar', data['mime_type'])
        self.assertEqual(len(bundle), data['blob_size'])
        self.assertEqual(unpack_size, data.get('unpack_size'))
        self.assertEqual('http://download.sugarlabs.org/activities/bundle', data['url'])
        assert 'blob' not in data

    def test_WrongAuthor(self):
        self.start_online_client()
        client = IPCClient()

        self.node_volume['context'].create({
                'guid': 'context',
                'type': 'content',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                'author': {'fake': None},
                })

        impl = {'context': 'context',
                'license': 'GPLv3+',
                'version': '1',
                'stability': 'stable',
                'notes': '',
                }
        self.assertRaises(http.Forbidden, client.post, ['implementation'], impl)
        self.assertEqual(0, self.node_volume['implementation'].find()[1])

        self.node_volume['context'].update('context', {'author': {tests.UID: None}})
        guid = client.post(['implementation'], impl)
        assert self.node_volume['implementation'].exists(guid)


if __name__ == '__main__':
    tests.main()
