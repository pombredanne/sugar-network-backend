#!/usr/bin/env python
# sugar-lint: disable

import re
import os
import time
import json
import urllib2
import hashlib
import tempfile
from email.utils import formatdate
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network import node, sugar, static, Client
from sugar_network.toolkit.router import Router, _Request, _parse_accept_language, Unauthorized, route, Redirect, NotModified
from active_toolkit import util
from sugar_network.resources.user import User
from sugar_network.resources.volume import Volume, Resource


class RouterTest(tests.Test):

    def test_Walkthrough(self):

        class Document(Resource):

            @ad.active_property(slot=1, prefix='A', full_text=True, default='')
            def term(self, value):
                return value

            @ad.active_property(ad.StoredProperty, default='')
            def stored(self, value):
                return value

            @ad.active_property(ad.BlobProperty)
            def blob(self, value):
                return value

        self.fork(self.restful_server, [User, Document])
        client = Client('http://localhost:8800', sugar_auth=True)

        guid_1 = client.post(['document'], {'term': 'term', 'stored': 'stored'})

        self.assertEqual({
            'stored': 'stored',
            'term': 'term',
            'guid': guid_1,
            'layer': ['public'],
            },
            client.get(['document', guid_1], reply='stored,term,guid,layer'))

        guid_2 = client.post(['document'], {'term': 'term2', 'stored': 'stored2'})

        self.assertEqual({
            'stored': 'stored2',
            'term': 'term2',
            'guid': guid_2,
            'layer': ['public'],
            },
            client.get(['document', guid_2], reply='stored,term,guid,layer'))

        reply = client.get(['document'], reply='guid,stored,term')
        self.assertEqual(2, reply['total'])
        self.assertEqual(
                sorted([
                    {'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                    {'guid': guid_2, 'stored': 'stored2', 'term': 'term2'},
                    ]),
                sorted(reply['result']))

        client.put(['document', guid_2], {'stored': 'stored3', 'term': 'term3'})

        self.assertEqual({
            'stored': 'stored3',
            'term': 'term3',
            'guid': guid_2,
            'layer': ['public'],
            },
            client.get(['document', guid_2], reply='stored,term,guid,layer'))

        self.assertEqual(
                {'total': 2,
                    'result': sorted([
                        {'guid': guid_1, 'stored': 'stored', 'term': 'term'},
                        {'guid': guid_2, 'stored': 'stored3', 'term': 'term3'},
                        ])},
                client.get(['document'], reply='guid,stored,term'))

        client.delete(['document', guid_1])

        self.assertEqual(
                {'total': 1,
                    'result': sorted([
                        {'guid': guid_2, 'stored': 'stored3', 'term': 'term3'},
                        ])},
                client.get(['document'], reply='guid,stored,term'))

        self.assertEqual(
                'term3',
                client.get(['document', guid_2, 'term']))
        client.put(['document', guid_2, 'term'], 'term4')
        self.assertEqual(
                'term4',
                client.get(['document', guid_2, 'term']))

        payload = 'blob'
        client.put(['document', guid_2, 'blob'], payload)
        self.assertEqual(
                payload,
                client.request('GET', ['document', guid_2, 'blob']).content)

        client.delete(['document', guid_2])

        self.assertEqual(
                {'total': 0,
                    'result': sorted([])},
                client.get(['document'], reply='guid,stored,term'))

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

            @ad.volume_command(cmd='2', mime_type='application/json')
            def get_json(self, response):
                pass

            @ad.volume_command(cmd='3')
            def no_get(self, response):
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

        response = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'cmd=3',
            },
            lambda *args: None)
        self.assertEqual('', ''.join([i for i in response]))

    def test_Register(self):

        class Document(Resource):

            @ad.active_property(slot=1, prefix='A', full_text=True, default='')
            def term(self, value):
                return value

            @ad.active_property(ad.StoredProperty, default='')
            def stored(self, value):
                return value

        self.fork(self.restful_server, [User, Document])

        client = Client('http://localhost:8800', sugar_auth=False)
        self.assertRaises(RuntimeError, client.post, ['document'], {'term': 'term', 'stored': 'stored'})
        self.assertRaises(RuntimeError, client.get, ['user', sugar.uid()])

        client = Client('http://localhost:8800', sugar_auth=True)
        client.post(['document'], {'term': 'term', 'stored': 'stored'})
        self.assertEqual(sugar.uid(), client.get(['user', sugar.uid(), 'guid']))

    def test_Authenticate(self):

        class Document(Resource):

            @ad.active_property(slot=1, prefix='A', full_text=True, default='')
            def term(self, value):
                return value

            @ad.active_property(ad.StoredProperty, default='')
            def stored(self, value):
                return value

        pid = self.fork(self.restful_server, [User, Document])
        client = Client('http://localhost:8800', sugar_auth=True)
        client.post(['document'], {'term': 'term', 'stored': 'stored'})
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
            self.assertRaises(Unauthorized, router.authenticate, request)

            request.environ['HTTP_SUGAR_USER'] = tests.UID
            request.environ['HTTP_SUGAR_USER_SIGNATURE'] = tests.sign(tests.PRIVKEY, tests.UID)
            user = router.authenticate(request)
            self.assertEqual(tests.UID, user)

    def test_HandleRedirects(self):
        URL = 'http://sugarlabs.org'

        class Document2(Resource):

            @ad.active_property(ad.BlobProperty)
            def blob(self, value):
                raise Redirect(URL)

            @ad.active_property(slot=1, prefix='A', full_text=True, default='')
            def term(self, value):
                return value

        self.fork(self.restful_server, [User, Document2])
        client = Client('http://localhost:8800', sugar_auth=True)
        guid = client.post(['document2'], {'term': 'probe'})
        content = urllib2.urlopen(URL).read()
        assert content == client.request('GET', ['document2', guid, 'blob']).content

    def test_Request_MultipleQueryArguments(self):
        request = _Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'a1=v1&a2=v2&a1=v3&a3=v4&a1=v5&a3=v6',
            })
        self.assertEqual(
                {'a1': ['v1', 'v3', 'v5'], 'a2': 'v2', 'a3': ['v4', 'v6'], 'method': 'GET'},
                request)

    def test_Register_UrlPath(self):
        self.assertEqual(
                [],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': ''}).path)
        self.assertEqual(
                [],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/'}).path)
        self.assertEqual(
                ['foo'],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': 'foo'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': 'foo/bar'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/bar/'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                _Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '///foo////bar////'}).path)

    def test_Request_FailOnRelativePaths(self):
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '..'})
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/..'})
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/../'})
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '../bar'})
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/../bar'})
        self.assertRaises(RuntimeError, _Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/..'})

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

    def test_CustomRoutes(self):
        calls = []

        class TestRouterBase(Router):

            @route('GET', '/foo')
            def route1(self, request, response):
                calls.append('route1')

        class TestRouter(TestRouterBase):

            @route('PUT', '/foo')
            def route2(self, request, response):
                calls.append('route2')

            @route('GET', '/bar')
            def route3(self, request, response):
                calls.append('route3')

        class CommandsProcessor(object):

            def call(self, request, response):
                calls.append('default')

        cp = CommandsProcessor()
        router = TestRouter(cp)

        [i for i in router({'PATH_INFO': '/', 'REQUEST_METHOD': 'GET'}, lambda *args: None)]
        self.assertEqual(['default'], calls)
        del calls[:]

        [i for i in router({'PATH_INFO': '//foo//', 'REQUEST_METHOD': 'GET'}, lambda *args: None)]
        self.assertEqual(['route1'], calls)
        del calls[:]

        [i for i in router({'PATH_INFO': '/foo', 'REQUEST_METHOD': 'PUT'}, lambda *args: None)]
        self.assertEqual(['route2'], calls)
        del calls[:]

        [i for i in router({'PATH_INFO': '/foo', 'REQUEST_METHOD': 'POST'}, lambda *args: None)]
        self.assertEqual(['default'], calls)
        del calls[:]

        [i for i in router({'PATH_INFO': '/bar/foo/probe', 'REQUEST_METHOD': 'GET'}, lambda *args: None)]
        self.assertEqual(['route3'], calls)
        del calls[:]

    def test_GetLocalizedProps(self):

        class TestDocument(Resource):

            @ad.active_property(slot=100, localized=True)
            def prop(self, value):
                return value

        self.fork(self.restful_server, [User, TestDocument])

        self.override(ad, 'default_lang', lambda: 'en')
        client = Client('http://localhost:8800', sugar_auth=True)
        guid = client.post(['testdocument'], {'prop': 'en'})
        self.assertEqual('en', client.get(['testdocument', guid, 'prop']))

        self.override(ad, 'default_lang', lambda: 'ru')
        client = Client('http://localhost:8800', sugar_auth=True)
        self.assertEqual('en', client.get(['testdocument', guid, 'prop']))
        client.put(['testdocument', guid, 'prop'], 'ru')
        self.assertEqual('ru', client.get(['testdocument', guid, 'prop']))

        self.override(ad, 'default_lang', lambda: 'es')
        client = Client('http://localhost:8800', sugar_auth=True)
        self.assertEqual('en', client.get(['testdocument', guid, 'prop']))
        client.put(['testdocument', guid, 'prop'], 'es')
        self.assertEqual('es', client.get(['testdocument', guid, 'prop']))

        self.override(ad, 'default_lang', lambda: 'ru')
        client = Client('http://localhost:8800', sugar_auth=True)
        self.assertEqual('ru', client.get(['testdocument', guid, 'prop']))

        self.override(ad, 'default_lang', lambda: 'en')
        client = Client('http://localhost:8800', sugar_auth=True)
        self.assertEqual('en', client.get(['testdocument', guid, 'prop']))

        self.override(ad, 'default_lang', lambda: 'foo')
        client = Client('http://localhost:8800', sugar_auth=True)
        self.assertEqual('en', client.get(['testdocument', guid, 'prop']))

    def test_IfModifiedSince(self):

        class TestDocument(Resource):

            @ad.active_property(slot=100, typecast=int)
            def prop(self, value):
                if not self.request.if_modified_since or self.request.if_modified_since >= value:
                    return value
                else:
                    raise NotModified()

        self.start_master([User, TestDocument])
        client = Client('http://localhost:8800', sugar_auth=True)

        guid = client.post(['testdocument'], {'prop': 10})
        self.assertEqual(
                200,
                client.request('GET', ['testdocument', guid, 'prop']).status_code)
        self.assertEqual(
                200,
                client.request('GET', ['testdocument', guid, 'prop'], headers={
                    'If-Modified-Since': formatdate(11, localtime=False, usegmt=True),
                    }).status_code)
        self.assertEqual(
                304,
                client.request('GET', ['testdocument', guid, 'prop'], headers={
                    'If-Modified-Since': formatdate(9, localtime=False, usegmt=True),
                    }).status_code)

    def test_LastModified(self):

        class TestDocument(Resource):

            @ad.active_property(slot=100, typecast=int)
            def prop1(self, value):
                self.request.response.last_modified = value
                return value

            @ad.active_property(slot=101, typecast=int)
            def prop2(self, value):
                return value

            @ad.active_property(ad.BlobProperty)
            def prop3(self, value):
                return value

        self.start_master([User, TestDocument])
        client = Client('http://localhost:8800', sugar_auth=True)

        guid = client.post(['testdocument'], {'prop1': 10, 'prop2': 20, 'prop3': 'blob'})
        self.assertEqual(
                formatdate(10, localtime=False, usegmt=True),
                client.request('GET', ['testdocument', guid, 'prop1']).headers['Last-Modified'])
        self.assertEqual(
                None,
                client.request('GET', ['testdocument', guid, 'prop2']).headers['Last-Modified'])
        mtime = os.stat('master/testdocument/%s/%s/prop3' % (guid[:2], guid)).st_mtime
        self.assertEqual(
                formatdate(mtime, localtime=False, usegmt=True),
                client.request('GET', ['testdocument', guid, 'prop3']).headers['Last-Modified'])

    def test_StaticFiles(self):

        class TestDocument(Resource):
            pass

        self.start_master([User, TestDocument])
        client = Client('http://localhost:8800', sugar_auth=True)
        guid = client.post(['testdocument'], {})

        local_path = '../../../sugar_network/static/httpdocs/images/missing.png'
        response = client.request('GET', ['static', 'images', 'missing.png'])
        self.assertEqual(200, response.status_code)
        assert file(local_path).read() == response.content
        self.assertEqual(
                formatdate(os.stat(local_path).st_mtime, localtime=False, usegmt=True),
                response.headers['Last-Modified'])

    def test_StaticFilesIfModifiedSince(self):

        class TestDocument(Resource):
            pass

        self.start_master([User, TestDocument])
        client = Client('http://localhost:8800', sugar_auth=True)
        guid = client.post(['testdocument'], {})

        mtime = os.stat('../../../sugar_network/static/httpdocs/images/missing.png').st_mtime
        self.assertEqual(
                304,
                client.request('GET', ['static', 'images', 'missing.png'], headers={
                    'If-Modified-Since': formatdate(mtime, localtime=False, usegmt=True),
                    }).status_code)
        self.assertEqual(
                200,
                client.request('GET', ['static', 'images', 'missing.png'], headers={
                    'If-Modified-Since': formatdate(mtime - 1, localtime=False, usegmt=True),
                    }).status_code)
        self.assertEqual(
                304,
                client.request('GET', ['static', 'images', 'missing.png'], headers={
                    'If-Modified-Since': formatdate(mtime + 1, localtime=False, usegmt=True),
                    }).status_code)

    def test_IfModifiedSinceForBlobs(self):

        class TestDocument(Resource):

            @ad.active_property(ad.BlobProperty)
            def blob(self, value):
                return value

        self.start_master([User, TestDocument])
        client = Client('http://localhost:8800', sugar_auth=True)

        guid = client.post(['testdocument'], {'blob': 'value'})
        blob_path = 'master/testdocument/%s/%s/blob' % (guid[:2], guid)

        os.utime(blob_path, (10, 10))
        self.assertEqual(
                304,
                client.request('GET', ['testdocument', guid, 'blob'], headers={
                    'If-Modified-Since': formatdate(11, localtime=False, usegmt=True),
                    }).status_code)
        self.assertEqual(
                304,
                client.request('GET', ['testdocument', guid, 'blob'], headers={
                    'If-Modified-Since': formatdate(10, localtime=False, usegmt=True),
                    }).status_code)

        self.assertEqual(
                200,
                client.request('GET', ['testdocument', guid, 'blob'], headers={
                    'If-Modified-Since': formatdate(9, localtime=False, usegmt=True),
                    }).status_code)

    def test_JsonpCallback(self):

        class Document(Resource):

            @ad.active_property(slot=1, prefix='A', full_text=True, default='')
            def term(self, value):
                return value

        self.start_master([User, Document])
        client = Client('http://localhost:8800')

        response = client.request(
                'POST',
                ['document'],
                json.dumps({'term': 'value'}),
                params={'callback': 'foo'},
                headers={'Content-Type': 'application/json'})
        guid = re.match('foo\("([^"]+)"\);', response.content)
        assert guid is not None
        guid = guid.groups()[0]

        response = client.request(
                'GET',
                ['document'],
                params={'callback': 'bar', 'reply': 'guid'},
                headers={'Content-Type': 'application/json'})
        self.assertEqual(
                'bar({"total": 1, "result": [{"guid": "%s"}]});' % guid,
                response.content)


if __name__ == '__main__':
    tests.main()
