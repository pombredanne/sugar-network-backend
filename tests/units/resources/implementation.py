#!/usr/bin/env python
# sugar-lint: disable

import xapian

from __init__ import tests

from sugar_network.toolkit import sugar
from sugar_network.toolkit.router import Request
from sugar_network.resources.volume import Volume
from sugar_network.resources.implementation import _encode_version, Implementation
from sugar_network.node.commands import NodeCommands
from sugar_network.client import IPCClient


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

    def test_SetMimeTypeForActivities(self):
        self.start_server()
        client = IPCClient(params={'mountpoint': '~'})

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
        self.assertEqual('image/png', self.mounts.volume['implementation'].get(impl).meta('data')['mime_type'])

        client.put(['context', context, 'type'], 'activity')
        client.request('PUT', ['implementation', impl, 'data'], 'blob', {'Content-Type': 'image/png'})
        self.assertEqual('application/vnd.olpc-sugar', self.mounts.volume['implementation'].get(impl).meta('data')['mime_type'])

    def test_WrongAuthor(self):
        volume = self.start_server()
        client = IPCClient(params={'mountpoint': '~'})

        volume['context'].create(
                guid='context',
                type='content',
                title='title',
                summary='summary',
                description='description',
                author={'fake': None}
                )

        impl = {'context': 'context',
                'license': 'GPLv3+',
                'version': '1',
                'stability': 'stable',
                'notes': '',
                }
        self.assertRaises(RuntimeError, client.post, ['implementation'], impl)
        self.assertEqual(0, volume['implementation'].find()[1])

        volume['context'].update('context', author={sugar.uid(): None})
        guid = client.post(['implementation'], impl)
        assert volume['implementation'].exists(guid)


if __name__ == '__main__':
    tests.main()
