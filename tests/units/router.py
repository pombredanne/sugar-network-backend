#!/usr/bin/env python
# sugar-lint: disable

import time
import hashlib
import tempfile
from cStringIO import StringIO

from __init__ import tests

import active_document as ad
from sugar_network.node.router import Router, _Request, _parse_accept_language
from active_toolkit import util
from sugar_network.resources.volume import Volume


class RouterTest(tests.Test):

    def test_Walkthrough(self):
        self.fork(self.restful_server, [User, Document])
        rest = tests.Request('http://localhost:8800')

        guid_1 = rest.post('/document', {'term': 'term', 'stored': 'stored'})

        self.assertEqual({
            'stored': 'stored',
            'term': 'term',
            'guid': guid_1,
            'layer': ['public'],
            'user': [rest.uid],
            },
            rest.get('/document/' + guid_1, reply='stored,term,guid,layer,user'))

        guid_2 = rest.post('/document', {'term': 'term2', 'stored': 'stored2'})

        self.assertEqual({
            'stored': 'stored2',
            'term': 'term2',
            'guid': guid_2,
            'layer': ['public'],
            'user': [rest.uid],
            },
            rest.get('/document/' + guid_2, reply='stored,term,guid,layer,user'))

        reply = rest.get('/document', reply='guid,stored,term')
        self.assertEqual(2, reply['total'])
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                    {'guid': guid_2, 'stored': 'stored2', 'term': 'term2'},
                    ]),
                sorted(reply['result']))

        rest.put('/document/' + guid_2, {'stored': 'stored3', 'term': 'term3'})

        self.assertEqual({
            'stored': 'stored3',
            'term': 'term3',
            'guid': guid_2,
            'layer': ['public'],
            'user': [rest.uid],
            },
            rest.get('/document/' + guid_2, reply='stored,term,guid,layer,user'))

        self.assertEqual(
                {'total': 2,
                    'result': sorted([
                        {'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                        {'guid': guid_2, 'stored': 'stored3', 'term': 'term3'},
                        ])},
                rest.get('/document', reply='guid,stored,term'))

        rest.delete('/document/' + guid_1)

        self.assertEqual(
                {'total': 1,
                    'result': sorted([
                        {'guid': guid_2, 'stored': 'stored3', 'term': 'term3'},
                        ])},
                rest.get('/document', reply='guid,stored,term'))

        self.assertEqual(
                'term3',
                rest.get('/document/' + guid_2 + '/term'))
        rest.put('/document/' + guid_2 + '/term', 'term4')
        self.assertEqual(
                'term4',
                rest.get('/document/' + guid_2 + '/term'))

        payload = 'blob'
        rest.put('/document/' + guid_2 + '/blob', payload, headers={'Content-Type': 'application/octet-stream'})
        self.assertEqual(
                payload,
                rest.get('/document/' + guid_2 + '/blob'))

        rest.delete('/document/' + guid_2)

        self.assertEqual(
                {'total': 0,
                    'result': sorted([])},
                rest.get('/document', reply='guid,stored,term'))

    def test_JsonAutoEncoding(self):
        self.fork(self.restful_server, [User, Document])
        rest = tests.Request('http://localhost:8800')

        guid = rest.post('/document', {'term': 'term'})

        self.assertRaises(RuntimeError, rest.get, '/document/' + guid + '/json')

        rest.put('/document/' + guid + '/json', -1)
        self.assertEqual(
                -1,
                rest.get('/document/' + guid + '/json'))

        rest.put('/document/' + guid + '/json', {'foo': None})
        self.assertEqual(
                {'foo': None},
                rest.get('/document/' + guid + '/json'))

    def test_StreamedResponse(self):

        class CommandsProcessor(ad.CommandsProcessor):

            @ad.volume_command()
            def get_stream(self, response):
                return StringIO('stream')

        cp = CommandsProcessor()
        router = Router(cp)

        response = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda *args: None)
        self.assertEqual('stream', ''.join([i for i in response]))

    def test_EmptyResponse(self):

        class CommandsProcessor(ad.CommandsProcessor):

            @ad.volume_command(cmd='1', mime_type='application/octet-stream')
            def get_binary(self, response):
                pass

            @ad.volume_command(cmd='2')
            def get_json(self, response):
                pass

        cp = CommandsProcessor()
        router = Router(cp)

        response = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'cmd=1',
            },
            lambda *args: None)
        self.assertEqual('', ''.join([i for i in response]))

        response = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'cmd=2',
            },
            lambda *args: None)
        self.assertEqual('null', ''.join([i for i in response]))

    def test_Register(self):
        self.fork(self.restful_server, [User, Document])

        self.assertRaises(RuntimeError, tests.Request, 'http://localhost:8800',
                uid=tests.UID, privkey=tests.PRIVKEY,
                pubkey=tests.INVALID_PUBKEY)

        rest = tests.Request('http://localhost:8800',
                uid=tests.UID, privkey=tests.PRIVKEY, pubkey=tests.PUBKEY)
        self.assertEqual(
                {'total': 1,
                    'result': sorted([
                        {'guid': tests.UID},
                        ]),
                    },
                rest.get('/user'))

    def test_Authenticate(self):
        pid = self.fork(self.restful_server, [User])
        rest = tests.Request('http://localhost:8800')
        self.waitpid(pid)

        with Volume(tests.tmpdir + '/remote', [User]) as documents:
            cp = ad.VolumeCommands(documents)
            router = Router(cp)

            request = _Request({
                'HTTP_SUGAR_USER': 'foo',
                'HTTP_SUGAR_USER_SIGNATURE': tests.sign(tests.PRIVKEY, 'foo'),
                'PATH_INFO': '/foo',
                'REQUEST_METHOD': 'GET',
                })
            self.assertRaises(ad.Unauthorized, router._authenticate, request)

            request.environ['HTTP_SUGAR_USER'] = tests.UID
            request.environ['HTTP_SUGAR_USER_SIGNATURE'] = tests.sign(tests.PRIVKEY, tests.UID)
            user = router._authenticate(request)
            self.assertEqual(tests.UID, user)

    def test_Authorization(self):
        self.fork(self.restful_server, [User, Document])

        rest_1 = tests.Request('http://localhost:8800')
        guid = rest_1.post('/document', {'term': '', 'stored': ''})

        rest_2 = tests.Request('http://localhost:8800', tests.UID2, tests.PRIVKEY2, tests.PUBKEY2)
        self.assertRaises(RuntimeError, rest_2.put, '/document/' + guid, {'term': 'new'})
        self.assertRaises(RuntimeError, rest_2.delete, '/document/' + guid)

    def test_UrlPath(self):
        self.fork(self.restful_server, [User, Document])
        rest = tests.Request('http://localhost:8800')

        # Should `urllib.splithost('//foo')` return `('foo', '')` ?
        self.assertRaises(RuntimeError, rest.post, '//document/', {'term': 'probe'})

        guid = rest.post('/document//', {'term': 'probe'})
        self.assertEqual(
                {'term': 'probe'},
                rest.get('///document//%s/' % guid, reply='term'))

    def test_Request_MultipleQueryArguments(self):
        request = _Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'a1=v1&a2=v2&a1=v3&a3=v4&a1=v5&a3=v6',
            })
        self.assertEqual(
                {'a1': ['v1', 'v3', 'v5'], 'a2': 'v2', 'a3': ['v4', 'v6'], 'method': 'GET'},
                request)

    def test_parse_accept_language(self):
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('  ru , en   ,  es'))
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('  en;q=.4 , ru, es;q=0.1'))
        self.assertEqual(
                ['ru', 'en', 'es'],
                _parse_accept_language('ru;q=1,en;q=1,es;q=0.5'))


class Document(ad.Document):

    @ad.active_property(slot=1, prefix='A', full_text=True)
    def term(self, value):
        return value

    @ad.active_property(ad.StoredProperty, default='')
    def stored(self, value):
        return value

    @ad.active_property(ad.BlobProperty)
    def blob(self, value):
        return value

    @ad.active_property(ad.BlobProperty, mime_type='application/json')
    def json(self, value):
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
