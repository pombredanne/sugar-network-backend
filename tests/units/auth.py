#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

import active_document as ad
from sugar_network.node import auth
from sugar_network import IPCClient, Client
from sugar_network.resources.volume import Request
from active_toolkit import enforce


class AuthTest(tests.Test):

    def test_Config(self):
        self.touch(('authorization.conf', [
            '[user_1]',
            'role_1 = True',
            '[user_2]',
            'role_2 = False',
            ]))

        request = Request()
        request.principal = 'user_1'
        self.assertEqual(True, auth.try_validate(request, 'role_1'))
        auth.validate(request, 'role_1')

        request.principal = 'user_2'
        self.assertEqual(False, auth.try_validate(request, 'role_2'))
        self.assertRaises(ad.Forbidden, auth.validate, request, 'role_2')

        request.principal = 'user_3'
        self.assertEqual(False, auth.try_validate(request, 'role_1'))
        self.assertEqual(False, auth.try_validate(request, 'role_2'))
        self.assertRaises(ad.Forbidden, auth.validate, request, 'role_1')
        self.assertRaises(ad.Forbidden, auth.validate, request, 'role_2')

    def test_FullWriteForRoot(self):
        client = Client()

        self.start_master()
        client.post(['context'], {
            'implement': 'guid',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertNotEqual('probe', client.get(['context', 'guid', 'title']))
        self.stop_servers()

        self.touch((
            'master/context/gu/guid/user',
            '{"seqno": 1, "value": ["fake"]}',
            ))

        self.start_master()
        self.assertRaises(RuntimeError, client.put, ['context', 'guid'], {'title': 'probe'})

        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))
        auth._config = None
        client.put(['context', 'guid'], {'title': 'probe'})
        self.assertEqual('probe', client.get(['context', 'guid', 'title']))


if __name__ == '__main__':
    tests.main()
