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

from __init__ import tests, src_root

from sugar_network import db, node, static, toolkit
from sugar_network.db.router import Router, _Request, _parse_accept_language, route, _filename
from sugar_network.toolkit import util, default_lang, http
from sugar_network.resources.user import User
from sugar_network.resources.volume import Volume, Resource
from sugar_network import client as local


class RouterTest(tests.Test):

    def test_StreamedResponse(self):

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command()
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

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(cmd='1', mime_type='application/octet-stream')
            def get_binary(self, response):
                pass

            @db.volume_command(cmd='2', mime_type='application/json')
            def get_json(self, response):
                pass

            @db.volume_command(cmd='3')
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

    def test_StatusWOResult(self):

        class Status(http.Status):
            status = '001 Status'
            headers = {'Status-Header': 'value'}

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, response):
                raise Status('Status-Error')

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        error = json.dumps({'request': '/', 'error': 'Status-Error'})
        self.assertEqual(error, ''.join([i for i in reply]))
        self.assertEqual([
            '001 Status',
            {'Content-Length': str(len(error)), 'Content-Type': 'application/json', 'Status-Header': 'value'},
            ],
            response)

    def test_StatusWResult(self):

        class Status(http.Status):
            status = '001 Status'
            headers = {'Status-Header': 'value'}
            result = 'result'

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, response):
                raise Status('Status-Error')

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        error = 'result'
        self.assertEqual(error, ''.join([i for i in reply]))
        self.assertEqual([
            '001 Status',
            {'Content-Length': str(len(error)), 'Status-Header': 'value'},
            ],
            response)

    def test_StatusPass(self):

        class StatusPass(http.StatusPass):
            status = '001 StatusPass'
            headers = {'StatusPass-Header': 'value'}
            result = 'result'

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, response):
                raise StatusPass('Status-Error')

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        error = ''
        self.assertEqual(error, ''.join([i for i in reply]))
        self.assertEqual([
            '001 StatusPass',
            {'Content-Length': str(len(error)), 'StatusPass-Header': 'value'},
            ],
            response)

    def test_BlobsRedirects(self):
        URL = 'http://sugarlabs.org'

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, response):
                return db.PropertyMetadata(url=URL)

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        error = ''
        self.assertEqual(error, ''.join([i for i in reply]))
        self.assertEqual([
            '303 See Other',
            {'Content-Length': '0', 'Location': URL},
            ],
            response)

    def test_LastModified(self):

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, request):
                request.response.last_modified = 10
                return 'ok'

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'ok'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {'Last-Modified': formatdate(10, localtime=False, usegmt=True), 'Content-Length': str(len(result))},
            ],
            response)

    def test_IfModifiedSince(self):

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, request):
                if not request.if_modified_since or request.if_modified_since >= 10:
                    return 'ok'
                else:
                    raise http.NotModified()

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'ok'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {'Content-Length': str(len(result))},
            ],
            response)

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'HTTP_IF_MODIFIED_SINCE': formatdate(11, localtime=False, usegmt=True),
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'ok'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {'Content-Length': str(len(result))},
            ],
            response)

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'HTTP_IF_MODIFIED_SINCE': formatdate(9, localtime=False, usegmt=True),
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = ''
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '304 Not Modified',
            {'Content-Length': str(len(result))},
            ],
            response)

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
        self.assertEqual(
                ['ru-ru', 'es-br'],
                _parse_accept_language('ru-RU,es_BR'))

    def test_StaticFiles(self):
        router = Router(db.CommandsProcessor())
        local_path = src_root + '/sugar_network/static/httpdocs/images/missing.png'

        response = []
        reply = router({
            'PATH_INFO': '/static/images/missing.png',
            'REQUEST_METHOD': 'GET',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = file(local_path).read()
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {
                'Last-Modified': formatdate(os.stat(local_path).st_mtime, localtime=False, usegmt=True),
                'Content-Length': str(len(result)),
                'Content-Type': 'image/png',
                'Content-Disposition': 'attachment; filename="missing.png"',
                }
            ],
            response)

    def test_StaticFilesIfModifiedSince(self):
        router = Router(db.CommandsProcessor())
        local_path = src_root + '/sugar_network/static/httpdocs/images/missing.png'
        mtime = os.stat(local_path).st_mtime

        response = []
        reply = router({
            'PATH_INFO': '/static/images/missing.png',
            'REQUEST_METHOD': 'GET',
            'HTTP_IF_MODIFIED_SINCE': formatdate(mtime - 1, localtime=False, usegmt=True),
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = file(local_path).read()
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {
                'Last-Modified': formatdate(mtime, localtime=False, usegmt=True),
                'Content-Length': str(len(result)),
                'Content-Type': 'image/png',
                'Content-Disposition': 'attachment; filename="missing.png"',
                }
            ],
            response)

        response = []
        reply = router({
            'PATH_INFO': '/static/images/missing.png',
            'REQUEST_METHOD': 'GET',
            'HTTP_IF_MODIFIED_SINCE': formatdate(mtime, localtime=False, usegmt=True),
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = ''
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '304 Not Modified',
            {'Content-Length': str(len(result))},
            ],
            response)

        response = []
        reply = router({
            'PATH_INFO': '/static/images/missing.png',
            'REQUEST_METHOD': 'GET',
            'HTTP_IF_MODIFIED_SINCE': formatdate(mtime + 1, localtime=False, usegmt=True),
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = ''
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '304 Not Modified',
            {'Content-Length': str(len(result))},
            ],
            response)

    def test_JsonpCallback(self):

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET')
            def get(self, request):
                return 'ok'

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'callback=foo',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'foo("ok");'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {'Content-Length': str(len(result))},
            ],
            response)

    def test_filename(self):
        self.assertEqual('Foo', _filename('foo', None))
        self.assertEqual('Foo-Bar', _filename(['foo', 'bar'], None))
        self.assertEqual('FOO-BaR', _filename([' f o o', ' ba r   '], None))

        self.assertEqual('Foo-3', _filename(['foo', 3], None))

        self.assertEqual('12-3', _filename(['/1/2/', '/3/'], None))

        self.assertEqual('Foo.png', _filename('foo', 'image/png'))
        self.assertEqual('Foo-Bar.gif', _filename(['foo', 'bar'], 'image/gif'))
        self.assertEqual('Fake', _filename('fake', 'foo/bar'))

        self.assertEqual('Eng', _filename({default_lang(): 'eng'}, None))
        self.assertEqual('Eng', _filename([{default_lang(): 'eng'}], None))
        self.assertEqual('Bar-1', _filename([{'lang': 'foo', default_lang(): 'bar'}, 1], None))

    def test_BlobsDisposition(self):
        self.touch(('blob.data', 'value'))

        class CommandsProcessor(db.CommandsProcessor):

            @db.volume_command(method='GET', cmd='1')
            def cmd1(self, request):
                return db.PropertyMetadata(name='foo', blob='blob.data')

            @db.volume_command(method='GET', cmd='2')
            def cmd2(self, request):
                return db.PropertyMetadata(filename='foo.bar', blob='blob.data')

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'cmd=1',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'value'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {
                'Last-Modified': formatdate(os.stat('blob.data').st_mtime, localtime=False, usegmt=True),
                'Content-Length': str(len(result)),
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': 'attachment; filename="Foo.obj"',
                }
            ],
            response)

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'cmd=2',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        result = 'value'
        self.assertEqual(result, ''.join([i for i in reply]))
        self.assertEqual([
            '200 OK',
            {
                'Last-Modified': formatdate(os.stat('blob.data').st_mtime, localtime=False, usegmt=True),
                'Content-Length': str(len(result)),
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': 'attachment; filename="foo.bar"',
                }
            ],
            response)


if __name__ == '__main__':
    tests.main()
