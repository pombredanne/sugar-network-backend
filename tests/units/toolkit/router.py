#!/usr/bin/env python
# sugar-lint: disable

import os
import json
from email.utils import formatdate
from cStringIO import StringIO

from __init__ import tests, src_root

from sugar_network import db
from sugar_network.toolkit.router import Blob, Router, Request, _parse_accept_language, route, fallbackroute, preroute, postroute, _filename
from sugar_network.toolkit import default_lang, http


class RouterTest(tests.Test):

    def test_routes_Exact(self):

        class Routes(object):

            @route('PROBE')
            def command_1(self):
                return 'command_1'

            @route('PROBE', cmd='command_2')
            def command_2(self):
                return 'command_2'

            @route('PROBE', ['resource'])
            def command_3(self):
                return 'command_3'

            @route('PROBE', ['resource'], cmd='command_4')
            def command_4(self):
                return 'command_4'

            @route('PROBE', ['resource', 'guid'])
            def command_5(self):
                return 'command_5'

            @route('PROBE', ['resource', 'guid'], cmd='command_6')
            def command_6(self):
                return 'command_6'

            @route('PROBE', ['resource', 'guid', 'prop'])
            def command_7(self):
                return 'command_7'

            @route('PROBE', ['resource', 'guid', 'prop'], cmd='command_8')
            def command_8(self):
                return 'command_8'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['command_1'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_1')))]), status[-1])

        self.assertEqual(
                ['command_2'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'cmd=command_2',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_2')))]), status[-1])

        self.assertEqual(
                ['command_3'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_3')))]), status[-1])

        self.assertEqual(
                ['command_4'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource',
                    'QUERY_STRING': 'cmd=command_4',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_4')))]), status[-1])

        self.assertEqual(
                ['command_5'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_5')))]), status[-1])

        self.assertEqual(
                ['command_6'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid',
                    'QUERY_STRING': 'cmd=command_6',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_6')))]), status[-1])

        self.assertEqual(
                ['command_7'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid/prop',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_7')))]), status[-1])

        self.assertEqual(
                ['command_8'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid/prop',
                    'QUERY_STRING': 'cmd=command_8',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_8')))]), status[-1])

        self.assertEqual(
                ['{"request": "/*/*/*", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*/*/*'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/?cmd=*", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/', 'QUERY_STRING': 'cmd=*'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/'}, lambda *args: None)])

    def test_routes_TailWildcards(self):

        class Routes(object):

            @route('PROBE', ['resource', 'guid', None])
            def command_1(self):
                return 'command_1'

            @route('PROBE', ['resource', 'guid', None], cmd='command_2')
            def command_2(self):
                return 'command_2'

            @route('PROBE', ['resource', None, None])
            def command_3(self):
                return 'command_3'

            @route('PROBE', ['resource', None, None], cmd='command_4')
            def command_4(self):
                return 'command_4'

            @route('PROBE', [None, None, None])
            def command_5(self):
                return 'command_5'

            @route('PROBE', [None, None, None], cmd='command_6')
            def command_6(self):
                return 'command_6'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['command_1'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid/*',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_1')))]), status[-1])

        self.assertEqual(
                ['command_2'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid/*',
                    'QUERY_STRING': 'cmd=command_2',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_2')))]), status[-1])

        self.assertEqual(
                ['command_3'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid2/prop',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_3')))]), status[-1])

        self.assertEqual(
                ['command_4'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource/guid2/prop',
                    'QUERY_STRING': 'cmd=command_4',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_4')))]), status[-1])

        self.assertEqual(
                ['command_5'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/*/guid/prop',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_5')))]), status[-1])

        self.assertEqual(
                ['command_6'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/*/guid/prop',
                    'QUERY_STRING': 'cmd=command_6',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_6')))]), status[-1])

        self.assertEqual(
                ['{"request": "/", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*/*/*?cmd=*", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*/*/*', 'QUERY_STRING': 'cmd=*'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*/*/*", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/*/*/*'}, lambda *args: None)])

    def test_routes_FreeWildcards(self):

        class Routes(object):

            @route('PROBE', ['resource1', None, 'prop1'])
            def command_1(self):
                return 'command_1'

            @route('PROBE', ['resource1', None, 'prop1'], cmd='command_2')
            def command_2(self):
                return 'command_2'

            @route('PROBE', [None, None, 'prop2'])
            def command_3(self):
                return 'command_3'

            @route('PROBE', [None, None, 'prop2'], cmd='command_4')
            def command_4(self):
                return 'command_4'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['command_1'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource1/*/prop1',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_1')))]), status[-1])

        self.assertEqual(
                ['command_2'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/resource1/*/prop1',
                    'QUERY_STRING': 'cmd=command_2',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_2')))]), status[-1])

        self.assertEqual(
                ['command_3'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/*/*/prop2',
                    'QUERY_STRING': '',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_3')))]), status[-1])

        self.assertEqual(
                ['command_4'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/*/*/prop2',
                    'QUERY_STRING': 'cmd=command_4',
                    }, lambda *args: status.append(args))])
        self.assertEqual(('200 OK', [('content-length', str(len('command_4')))]), status[-1])

        self.assertEqual(
                ['{"request": "/*/*/prop3", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*/*/prop3'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*/*/prop2", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/*/*/prop2'}, lambda *args: None)])

        self.assertEqual(
                ['{"request": "/", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*/*/*?cmd=*", "error": "Path not found"}'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*/*/*', 'QUERY_STRING': 'cmd=*'}, lambda *args: None)])
        self.assertEqual(
                ['{"request": "/*/*/prop2", "error": "No such operation"}'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/*/*/prop2'}, lambda *args: None)])

    def test_routes_Fallback(self):

        class Routes(object):

            @fallbackroute()
            def fallback(self):
                return 'fallback'

            @fallbackroute('PROBE2')
            def fallback2(self):
                return 'fallback2'

            @route('PROBE', ['exists'])
            def exists(self):
                return 'exists'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['exists'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/exists'}, lambda *args: None)])
        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/*'}, lambda *args: None)])
        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/*/*/*'}, lambda *args: None)])

        self.assertEqual(
                ['fallback2'],
                [i for i in router({'REQUEST_METHOD': 'PROBE2', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['fallback2'],
                [i for i in router({'REQUEST_METHOD': 'PROBE2', 'PATH_INFO': '/*/*/*'}, lambda *args: None)])
        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'PROBE3', 'PATH_INFO': '/*/*/*/*/*'}, lambda *args: None)])

    def test_routes_FallbackForCommands(self):

        class Routes(object):

            @fallbackroute()
            def fallback(self):
                return 'fallback'

            @fallbackroute('PROBE1', ['raise', 'fail'])
            def fallback2(self):
                return 'fallback2'

            @route('PROBE2', [None, None])
            def exists(self):
                return 'exists'

            @route('PROBE3', [None, None], cmd='CMD')
            def exists2(self):
                return 'exists2'

        router = Router(Routes())

        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'PROBE3', 'PATH_INFO': '/raise/fail', 'QUERY_STRING': 'cmd=FOO'}, lambda *args: None)])

    def test_routes_FallbackAndRegularRouteOnTheSameLevel(self):

        class Routes(object):

            @fallbackroute()
            def fallback(self):
                return 'fallback'

            @route('PROBE')
            def exists(self):
                return 'exists'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['exists'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/'}, lambda *args: None)])

    def test_routes_CheckFallbacksBeforeWildecards(self):

        class Routes(object):

            @fallbackroute('PROBE', ['static'])
            def fallback(self):
                return 'fallback'

            @route('PROBE', [None])
            def wildcards(self):
                return 'wildcards'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['fallback'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/static'}, lambda *args: None)])
        self.assertEqual(
                ['wildcards'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/foo'}, lambda *args: None)])

    def test_routes_FallbackForTailedPaths(self):

        class Routes(object):

            @fallbackroute('PROBE', ['static'])
            def fallback(self, request):
                return '/'.join(request.path)

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['static'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/static'}, lambda *args: None)])
        self.assertEqual(
                ['static/foo'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/static/foo'}, lambda *args: None)])
        self.assertEqual(
                ['static/foo/bar'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/static/foo/bar'}, lambda *args: None)])

    def test_routes_ParentClasses(self):
        calls = []

        class Parent(object):

            @route('PROBE')
            def probe(self):
                return 'probe'

        class Child(Parent):
            pass

        router = Router(Child())

        self.assertEqual(
                ['probe'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])

    def test_routes_OverrideInChildClass(self):
        calls = []

        class Parent(object):

            @route('PROBE')
            def probe(self):
                return 'probe-1'

            @route('COMMON')
            def common(self):
                return 'common'

        class Child(Parent):

            @route('PROBE')
            def probe(self):
                return 'probe-2'

            @route('PARTICULAR')
            def particular(self):
                return 'particular'

        router = Router(Child())

        self.assertEqual(
                ['probe-2'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['common'],
                [i for i in router({'REQUEST_METHOD': 'COMMON', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(
                ['particular'],
                [i for i in router({'REQUEST_METHOD': 'PARTICULAR', 'PATH_INFO': '/'}, lambda *args: None)])

    def test_routes_Pre(self):

        class Routes(object):

            @route('PROBE')
            def ok(self, request, response):
                return request['probe']

            @preroute
            def preroute(self, op, request):
                request['probe'] = 'request'

        router = Router(Routes())

        self.assertEqual(
                ['request'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/'}, lambda *args: None)])

    def test_routes_Post(self):
        postroutes = []

        class Routes(object):

            @route('OK')
            def ok(self):
                return 'ok'

            @route('FAIL')
            def fail(self, request, response):
                raise Exception('fail')

            @postroute
            def postroute(self, request, response, result, exception):
                postroutes.append((result, str(exception)))

        router = Router(Routes())

        self.assertEqual(
                ['ok'],
                [i for i in router({'REQUEST_METHOD': 'OK', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual(('ok', 'None'), postroutes[-1])

        self.assertEqual(
                ['{"request": "/", "error": "fail"}'],
                [i for i in router({'REQUEST_METHOD': 'FAIL', 'PATH_INFO': '/'}, lambda *args: None)])
        self.assertEqual((None, 'fail'), postroutes[-1])

    def test_routes_WildcardsAsLastResort(self):

        class Routes(object):

            @route('PROBE', ['exists'])
            def exists(self):
                return 'exists'

            @route('PROBE', ['exists', 'deep'])
            def exists_deep(self):
                return 'exists/deep'

            @route('GET', [None])
            def wildcards(self):
                return '*'

            @route('GET', [None, None])
            def wildcards_deep(self):
                return '*/*'

        router = Router(Routes())
        status = []

        self.assertEqual(
                ['exists'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/exists'}, lambda *args: None)])
        self.assertEqual(
                ['exists/deep'],
                [i for i in router({'REQUEST_METHOD': 'PROBE', 'PATH_INFO': '/exists/deep'}, lambda *args: None)])
        self.assertEqual(
                ['*'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/exists'}, lambda *args: None)])
        self.assertEqual(
                ['*/*'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/exists/deep'}, lambda *args: None)])
        self.assertEqual(
                ['*'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo'}, lambda *args: None)])
        self.assertEqual(
                ['*/*'],
                [i for i in router({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/bar'}, lambda *args: None)])

    def test_Request_Read(self):

        class Stream(object):

            def __init__(self, value):
                self.pos = 0
                self.value = value

            def read(self, size):
                print self.pos, size, len(self.value)
                assert self.pos + size <= len(self.value)
                result = self.value[self.pos:self.pos + size]
                self.pos += size
                return result

        request = Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'CONTENT_LENGTH': '3',
            'wsgi.input': Stream('123'),
            })
        self.assertEqual('123', request.content_stream.read())
        self.assertEqual('', request.content_stream.read())
        self.assertEqual('', request.content_stream.read(10))

        request = Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'CONTENT_LENGTH': '3',
            'wsgi.input': Stream('123'),
            })
        self.assertEqual('123', request.content_stream.read(10))

        request = Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'CONTENT_LENGTH': '3',
            'wsgi.input': Stream('123'),
            })
        self.assertEqual('1', request.content_stream.read(1))
        self.assertEqual('2', request.content_stream.read(1))
        self.assertEqual('3', request.content_stream.read())
        self.assertEqual('', request.content_stream.read())

    def test_IntArguments(self):

        class Routes(object):

            @route('PROBE', arguments={'arg': int})
            def probe(self, arg):
                return json.dumps(arg)

        router = Router(Routes())

        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])
        self.assertEqual(
                ['-1'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=-1',
                    }, lambda *args: None)])
        self.assertEqual(
                ['2'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=1&arg=2',
                    }, lambda *args: None)])
        self.assertEqual(
                ['0'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=',
                    }, lambda *args: None)])
        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])

    def test_BoolArguments(self):

        class Routes(object):

            @route('PROBE', arguments={'arg': bool})
            def probe(self, arg):
                return json.dumps(arg)

        router = Router(Routes())

        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])
        self.assertEqual(
                ['true'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=1',
                    }, lambda *args: None)])
        self.assertEqual(
                ['true'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=on',
                    }, lambda *args: None)])
        self.assertEqual(
                ['true'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=true',
                    }, lambda *args: None)])
        self.assertEqual(
                ['false'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=foo',
                    }, lambda *args: None)])
        self.assertEqual(
                ['true'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=',
                    }, lambda *args: None)])
        self.assertEqual(
                ['true'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg',
                    }, lambda *args: None)])
        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])

    def test_ListArguments(self):

        class Routes(object):

            @route('PROBE', arguments={'arg': list})
            def probe(self, arg):
                return json.dumps(arg)

        router = Router(Routes())

        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])
        self.assertEqual(
                ['["a1"]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=a1',
                    }, lambda *args: None)])
        self.assertEqual(
                ['["a1", "a2"]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=a1,a2',
                    }, lambda *args: None)])
        self.assertEqual(
                ['["a1", "a2", "a3"]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=a1&arg=a2&arg=a3',
                    }, lambda *args: None)])
        self.assertEqual(
                ['[]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'arg=',
                    }, lambda *args: None)])
        self.assertEqual(
                ['null'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])

    def test_ArgumentDefaults(self):

        class Routes(object):

            @route('PROBE', arguments={'a1': -1, 'a2': False, 'a3': [None]}, mime_type='application/json')
            def probe(self, a1, a2, a3):
                return (a1, a2, a3)

        router = Router(Routes())

        self.assertEqual(
                ['[-1, false, [null]]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])
        self.assertEqual(
                ['[1, true, ["3", "4"]]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'a1=1&a2=1&a3=3,4',
                    }, lambda *args: None)])
        self.assertEqual(
                ['[0, true, []]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    'QUERY_STRING': 'a1=&a2=&a3=',
                    }, lambda *args: None)])
        self.assertEqual(
                ['[-1, false, [null]]'],
                [i for i in router({
                    'REQUEST_METHOD': 'PROBE',
                    'PATH_INFO': '/',
                    }, lambda *args: None)])

    def test_StreamedResponse(self):

        class CommandsProcessor(object):

            @route('GET')
            def get_stream(self, response):
                return StringIO('stream')

        router = Router(CommandsProcessor())

        response = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            },
            lambda *args: None)
        self.assertEqual('stream', ''.join([i for i in response]))

    def test_EmptyResponse(self):

        class CommandsProcessor(object):

            @route('GET', [], '1', mime_type='application/octet-stream')
            def get_binary(self, response):
                pass

            @route('GET', [], '2', mime_type='application/json')
            def get_json(self, response):
                pass

            @route('GET', [], '3')
            def no_get(self, response):
                pass

        router = Router(CommandsProcessor())

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
            headers = {'status-header': 'value'}

        class CommandsProcessor(object):

            @route('GET')
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
            {'content-length': str(len(error)), 'content-type': 'application/json', 'status-header': 'value'},
            ],
            response)

    def test_ErrorInHEAD(self):

        class Status(http.Status):
            status = '001 Status'

        class CommandsProcessor(object):

            @route('HEAD')
            def get(self, response):
                raise Status('Status-Error')

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'HEAD',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        self.assertEqual('', ''.join([i for i in reply]))
        self.assertEqual([
            '001 Status',
            {'X-SN-error': '"Status-Error"'},
            ],
            response)

    def test_StatusPass(self):

        class StatusPass(http.StatusPass):
            status = '001 StatusPass'
            headers = {'statuspass-header': 'value'}
            result = 'result'

        class CommandsProcessor(object):

            @route('GET')
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
            {'content-length': str(len(error)), 'statuspass-header': 'value'},
            ],
            response)

    def test_BlobsRedirects(self):
        URL = 'http://sugarlabs.org'

        class CommandsProcessor(object):

            @route('GET')
            def get(self, response):
                return Blob(url=URL)

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
            {'content-length': '0', 'location': URL},
            ],
            response)

    def test_LastModified(self):

        class CommandsProcessor(object):

            @route('GET')
            def get(self, request, response):
                response.last_modified = 10
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
            {'last-modified': formatdate(10, localtime=False, usegmt=True), 'content-length': str(len(result))},
            ],
            response)

    def test_IfModifiedSince(self):

        class CommandsProcessor(object):

            @route('GET')
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
            {'content-length': str(len(result))},
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
            {'content-length': str(len(result))},
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
            {'content-length': str(len(result))},
            ],
            response)

    def test_Request_MultipleQueryArguments(self):
        request = Request({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'a1=v1&a2=v2&a1=v3&a3=v4&a1=v5&a3=v6',
            })
        self.assertEqual(
                {'a1': ['v1', 'v3', 'v5'], 'a2': 'v2', 'a3': ['v4', 'v6']},
                dict(request))

    def test_Register_UrlPath(self):
        self.assertEqual(
                [],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': ''}).path)
        self.assertEqual(
                [],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/'}).path)
        self.assertEqual(
                ['foo'],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': 'foo'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': 'foo/bar'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/bar/'}).path)
        self.assertEqual(
                ['foo', 'bar'],
                Request({'REQUEST_METHOD': 'GET', 'PATH_INFO': '///foo////bar////'}).path)

    def test_Request_FailOnRelativePaths(self):
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '..'})
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/..'})
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/../'})
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '../bar'})
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/../bar'})
        self.assertRaises(RuntimeError, Request, {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/foo/..'})

    def test_Request_EmptyArguments(self):
        request = Request({'QUERY_STRING': 'a&b&c', 'PATH_INFO': '/', 'REQUEST_METHOD': 'GET'})
        self.assertEqual('', request['a'])
        self.assertEqual('', request['b'])
        self.assertEqual('', request['c'])

    def test_Request_UpdateQueryOnSets(self):
        request = Request({'QUERY_STRING': 'a&b=2&c', 'PATH_INFO': '/', 'REQUEST_METHOD': 'GET'})
        self.assertEqual('a&b=2&c', request.query)

        request['a'] = 'a'
        self.assertEqual('a=a&c=&b=2', request.query)

        request['b'] = 'b'
        self.assertEqual('a=a&c=&b=b', request.query)

        request['c'] = 'c'
        self.assertEqual('a=a&c=c&b=b', request.query)

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

    def test_JsonpCallback(self):

        class CommandsProcessor(object):

            @route('GET')
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
            {'content-length': str(len(result))},
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

        class CommandsProcessor(object):

            @route('GET', [], '1')
            def cmd1(self, request):
                return Blob(name='foo', blob='blob.data')

            @route('GET', [], cmd='2')
            def cmd2(self, request):
                return Blob(filename='foo.bar', blob='blob.data')

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
                'last-modified': formatdate(os.stat('blob.data').st_mtime, localtime=False, usegmt=True),
                'content-length': str(len(result)),
                'content-type': 'application/octet-stream',
                'content-disposition': 'attachment; filename="Foo.obj"',
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
                'last-modified': formatdate(os.stat('blob.data').st_mtime, localtime=False, usegmt=True),
                'content-length': str(len(result)),
                'content-type': 'application/octet-stream',
                'content-disposition': 'attachment; filename="foo.bar"',
                }
            ],
            response)

    def test_DoNotOverrideContentLengthForHEAD(self):

        class CommandsProcessor(object):

            @route('HEAD', [])
            def head(self, request, response):
                response.content_length = 100

        router = Router(CommandsProcessor())

        response = []
        reply = router({
            'PATH_INFO': '/',
            'REQUEST_METHOD': 'HEAD',
            },
            lambda status, headers: response.extend([status, dict(headers)]))
        self.assertEqual([], [i for i in reply])
        self.assertEqual([
            '200 OK',
            {'content-length': '100'},
            ],
            response)


if __name__ == '__main__':
    tests.main()
