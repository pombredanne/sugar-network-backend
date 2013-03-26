#!/usr/bin/env python
# sugar-lint: disable

import os
import json

from __init__ import tests

from sugar_network import db, client
from sugar_network.node import auth
from sugar_network.client import IPCClient, Client
from sugar_network.resources.user import User
from sugar_network.toolkit import http, enforce


class AuthTest(tests.Test):

    def test_Config(self):
        self.touch(('authorization.conf', [
            '[user_1]',
            'role_1 = True',
            '[user_2]',
            'role_2 = False',
            ]))

        request = db.Request()
        request.principal = 'user_1'
        self.assertEqual(True, auth.try_validate(request, 'role_1'))
        auth.validate(request, 'role_1')

        request.principal = 'user_2'
        self.assertEqual(False, auth.try_validate(request, 'role_2'))
        self.assertRaises(http.Forbidden, auth.validate, request, 'role_2')

        request.principal = 'user_3'
        self.assertEqual(False, auth.try_validate(request, 'role_1'))
        self.assertEqual(False, auth.try_validate(request, 'role_2'))
        self.assertRaises(http.Forbidden, auth.validate, request, 'role_1')
        self.assertRaises(http.Forbidden, auth.validate, request, 'role_2')

    def test_FullWriteForRoot(self):
        conn = Client()

        self.start_master()
        conn.post(['context'], {
            'guid': 'guid',
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertNotEqual('probe', conn.get(['context', 'guid', 'title']))
        self.stop_nodes()

        self.touch((
            'master/context/gu/guid/author',
            json.dumps({"seqno": 1, "value": {"fake": {"role": 3}}}),
            ))

        self.start_master()
        self.assertRaises(http.Forbidden, conn.put, ['context', 'guid'], {'title': 'probe'})

        self.touch(('authorization.conf', [
            '[%s]' % tests.UID,
            'root = True',
            ]))
        auth.reset()
        conn.put(['context', 'guid'], {'title': 'probe'})
        self.assertEqual('probe', conn.get(['context', 'guid', 'title']))

    def test_Anonymous(self):
        conn = http.Client(client.api_url.value)

        props = {'guid': 'guid',
                 'type': 'package',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }
        self.start_master()

        self.assertRaises(RuntimeError, conn.post, ['context'], props)

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            ]))
        auth.reset()
        conn.post(['context'], props)
        self.assertEqual('title', conn.get(['context', 'guid', 'title']))
        self.assertEqual([], conn.get(['context', 'guid', 'author']))

        self.stop_nodes()
        self.touch((
            'master/context/gu/guid/author',
            json.dumps({"seqno": 1, "value": {"fake": {"role": 3}}}),
            ))
        self.start_master()

        auth.reset()
        self.assertRaises(http.Forbidden, conn.put, ['context', 'guid'], {'title': 'probe'})

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            'root = True',
            ]))
        auth.reset()
        conn.put(['context', 'guid'], {'title': 'probe'})
        self.assertEqual('probe', conn.get(['context', 'guid', 'title']))
        self.assertEqual([{'name': 'fake', 'role': 3}], conn.get(['context', 'guid', 'author']))

    def test_LiveUpdate(self):
        conn = http.Client(client.api_url.value)

        props = {'guid': 'guid',
                 'type': 'package',
                 'title': 'title',
                 'summary': 'summary',
                 'description': 'description',
                 }
        self.start_master()

        self.touch(('authorization.conf', ''))
        os.utime('authorization.conf', (1, 1))
        self.assertRaises(RuntimeError, conn.post, ['context'], props)

        self.touch(('authorization.conf', [
            '[anonymous]',
            'user = True',
            ]))
        os.utime('authorization.conf', (2, 2))
        conn.post(['context'], props)
        self.assertEqual([], conn.get(['context', 'guid', 'author']))

        self.touch(('authorization.conf', ''))
        os.utime('authorization.conf', (3, 3))
        self.assertRaises(RuntimeError, conn.post, ['context'], props)

    def test_DefaultAuthorization(self):

        class Document(db.Document):

            @db.document_command(method='GET', cmd='probe1',
                    mime_type='application/json')
            def probe1(cls, directory):
                return 'ok1'

            @db.document_command(method='GET', cmd='probe2',
                    permissions=db.ACCESS_AUTH, mime_type='application/json')
            def probe2(cls, directory):
                return 'ok2'

        self.start_master([User, Document])
        conn = Client()

        guid = conn.post(['document'], {})
        self.assertEqual('ok1', conn.get(['document', guid], cmd='probe1'))
        self.assertEqual('ok2', conn.get(['document', guid], cmd='probe2'))


if __name__ == '__main__':
    tests.main()
