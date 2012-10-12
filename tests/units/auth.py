#!/usr/bin/env python
# sugar-lint: disable

import os

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
        auth.reset()
        client.put(['context', 'guid'], {'title': 'probe'})
        self.assertEqual('probe', client.get(['context', 'guid', 'title']))

    def test_Anonymous(self):
        client = Client(sugar_auth=False)

        props = {'implement': 'guid',
                 'type': 'package',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }
        self.start_master()

        self.assertRaises(RuntimeError, client.post, ['context'], props)

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            ]))
        auth.reset()
        client.post(['context'], props)
        self.assertEqual('title', client.get(['context', 'guid', 'title']))
        self.assertEqual(['anonymous'], client.get(['context', 'guid', 'user']))

        self.stop_servers()
        self.touch((
            'master/context/gu/guid/user',
            '{"seqno": 1, "value": ["fake"]}',
            ))
        self.start_master()

        auth.reset()
        self.assertRaises(RuntimeError, client.put, ['context', 'guid'], {'title': 'probe'})

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            'root = True',
            ]))
        auth.reset()
        client.put(['context', 'guid'], {'title': 'probe'})
        self.assertEqual('probe', client.get(['context', 'guid', 'title']))
        self.assertEqual(['fake'], client.get(['context', 'guid', 'user']))

    def test_LiveUpdate(self):
        client = Client(sugar_auth=False)

        props = {'implement': 'guid',
                 'type': 'package',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }
        self.start_master()

        self.touch(('authorization.conf', ''))
        os.utime('authorization.conf', (1, 1))
        self.assertRaises(RuntimeError, client.post, ['context'], props)

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            ]))
        os.utime('authorization.conf', (2, 2))
        client.post(['context'], props)
        self.assertEqual(['anonymous'], client.get(['context', 'guid', 'user']))

        self.touch(('authorization.conf', ''))
        os.utime('authorization.conf', (3, 3))
        self.assertRaises(RuntimeError, client.post, ['context'], props)


if __name__ == '__main__':
    tests.main()
