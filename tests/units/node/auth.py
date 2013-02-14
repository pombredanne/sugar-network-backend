#!/usr/bin/env python
# sugar-lint: disable

import os
import cPickle as pickle

from __init__ import tests

from sugar_network import db
from sugar_network.node import auth
from sugar_network.client import IPCClient, Client
from sugar_network.toolkit.router import Request
from sugar_network.resources.user import User
from sugar_network.toolkit import enforce


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
        self.assertRaises(db.Forbidden, auth.validate, request, 'role_2')

        request.principal = 'user_3'
        self.assertEqual(False, auth.try_validate(request, 'role_1'))
        self.assertEqual(False, auth.try_validate(request, 'role_2'))
        self.assertRaises(db.Forbidden, auth.validate, request, 'role_1')
        self.assertRaises(db.Forbidden, auth.validate, request, 'role_2')

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
            'master/context/gu/guid/author',
            pickle.dumps({"seqno": 1, "value": {"fake": {"role": 3}}}),
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
        self.assertEqual([], client.get(['context', 'guid', 'author']))

        self.stop_servers()
        self.touch((
            'master/context/gu/guid/author',
            pickle.dumps({"seqno": 1, "value": {"fake": {"role": 3}}}),
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
        self.assertEqual([{'name': 'fake', 'role': 3}], client.get(['context', 'guid', 'author']))

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
        self.assertEqual([], client.get(['context', 'guid', 'author']))

        self.touch(('authorization.conf', ''))
        os.utime('authorization.conf', (3, 3))
        self.assertRaises(RuntimeError, client.post, ['context'], props)

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
        client = Client(sugar_auth=True)

        guid = client.post(['document'], {})
        self.assertEqual('ok1', client.get(['document', guid], cmd='probe1'))
        self.assertEqual('ok2', client.get(['document', guid], cmd='probe2'))


if __name__ == '__main__':
    tests.main()
