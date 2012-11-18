#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.toolkit.router import Request
from sugar_network.resources.volume import Volume
from sugar_network.resources.implementation import _encode_version, Implementation
from sugar_network.node.commands import NodeCommands
from sugar_network import IPCClient


class ImplementationTest(tests.Test):

    def test_encode_version(self):
        self.assertEqual(
                '00000''00000''00001' '10',
                _encode_version('1'))
        self.assertEqual(
                '00000''00001''00002' '10',
                _encode_version('1.2'))
        self.assertEqual(
                '00001''00020''00300' '10',
                _encode_version('1.20.300'))
        self.assertEqual(
                '00020''00300''04000' '10',
                _encode_version('1.20.300.4000'))

        self.assertEqual(
                '00000''00000''00001' '10' '00002''00003''00004' '10',
                _encode_version('1-2.3.4'))
        self.assertEqual(
                '00000''00000''00001' '10' '00002''00003''00004' '10' '00006''00007''00008' '10',
                _encode_version('1-2.3.4-5.6.7.8'))

        self.assertEqual(
                '00000''00000''00001' '08',
                _encode_version('1-pre'))
        self.assertEqual(
                '00000''00000''00001' '09',
                _encode_version('1-rc'))
        self.assertEqual(
                '00000''00000''00001' '10',
                _encode_version('1-'))
        self.assertEqual(
                '00000''00000''00001' '11',
                _encode_version('1-post'))

        self.assertEqual(
                '00000''00000''00001' '08' '00003''00004''00005' '10',
                _encode_version('1-pre2.3.4.5'))

    def test_SetMimeTypeForActivities(self):
        self.start_server()
        client = IPCClient(mountpoint='~')

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


if __name__ == '__main__':
    tests.main()
