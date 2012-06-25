#!/usr/bin/env python
# sugar-lint: disable

import time
import hashlib

from __init__ import tests
from tests import Resource

import active_document as ad
from restful_document.router import Router
from restful_document.http import Request


class RouterTest(tests.Test):

    def test_Walkthrough(self):
        self.httpd(8200, [tests.User, Document])
        rest = Resource('http://localhost:8200')

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
        self.httpd(8200, [tests.User, Document])
        rest = Resource('http://localhost:8200')

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


    def test_ServerCrash(self):
        self.httpd(8200, [tests.User, Document])
        rest = Resource('http://localhost:8200')

        guid_1 = rest.post('/document', {'term': 'term', 'stored': 'stored'})
        guid_2 = rest.post('/document', {'term': 'term2', 'stored': 'stored2'})

        reply = rest.get('/document', reply='guid,stored,term')
        self.assertEqual(2, reply['total'])
        self.assertEqual(
                sorted([{'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                        {'guid': guid_2, 'stored': 'stored2', 'term': 'term2'},
                        ]),
                sorted(reply['result']))

        self.httpdown(8200)
        self.httpd(8100, [tests.User, Document])
        rest = Resource('http://localhost:8100')

        reply = rest.get('/document', reply='guid,stored,term')
        self.assertEqual(2, reply['total'])
        self.assertEqual(
                sorted([{'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                        {'guid': guid_2, 'stored': 'stored2', 'term': 'term2'},
                        ]),
                sorted(reply['result']))

    def test_Register(self):
        self.httpd(8200, [tests.User])

        self.assertRaises(RuntimeError, Resource, 'http://localhost:8200',
                uid=tests.UID, privkey=tests.PRIVKEY,
                pubkey=tests.INVALID_PUBKEY)

        rest = Resource('http://localhost:8200',
                uid=tests.UID, privkey=tests.PRIVKEY, pubkey=tests.PUBKEY)
        self.assertEqual(
                {'total': 1,
                    'result': sorted([
                        {'guid': tests.UID},
                        ]),
                    },
                rest.get('/user'))

    def test_Authenticate(self):
        self.httpd(8888, [tests.User])
        rest = Resource('http://localhost:8888')
        self.httpdown(8888)

        with ad.SingleVolume(tests.tmpdir + '/db', [tests.User]) as documents:
            cp = ad.VolumeCommands(documents)
            router = Router(cp)

            request = Request({
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
        self.httpd(8200, [tests.User, Document])

        rest_1 = Resource('http://localhost:8200')
        guid = rest_1.post('/document', {'term': '', 'stored': ''})

        rest_2 = Resource('http://localhost:8200', tests.UID2, tests.PRIVKEY2, tests.PUBKEY2)
        self.assertRaises(RuntimeError, rest_2.put, '/document/' + guid, {'term': 'new'})
        self.assertRaises(RuntimeError, rest_2.delete, '/document/' + guid)

    def test_UrlPath(self):
        self.httpd(8200, [tests.User, Document])
        rest = Resource('http://localhost:8200')

        # Should `urllib.splithost('//foo')` return `('foo', '')` ?
        self.assertRaises(RuntimeError, rest.post, '//document/', {'term': 'probe'})

        guid = rest.post('/document//', {'term': 'probe'})
        self.assertEqual(
                {'term': 'probe'},
                rest.get('///document//%s/' % guid, reply='term'))

    def test_HandleDeletes(self):
        self.httpd(8200, [tests.User, Document])
        rest = Resource('http://localhost:8200')

        guid = rest.post('/document', {'term': 'probe'})
        self.assertEqual(
                {'total': 1, 'result': [{'guid': guid}]},
                rest.get('/document'))
        self.assertEqual(
                {'layer': ['public']},
                rest.get('/document/' + guid, reply='layer'))

        rest.delete('/document/' + guid)
        self.assertEqual(
                {'total': 0, 'result': []},
                rest.get('/document'))
        self.assertRaises(RuntimeError, rest.get, '/document/' + guid)

        self.httpdown(8200)

        volume = ad.SingleVolume(tests.tmpdir + '/db', [Document])
        self.assertEqual(['deleted'], volume['document'].get(guid)['layer'])


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


if __name__ == '__main__':
    tests.main()
