#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from sugar_network import db
from sugar_network.db import env, volume, Volume, Document, \
        property_command, document_command, directory_command, volume_command, \
        Request, BlobProperty, Response, CommandsProcessor, \
        CommandNotFound, to_int, to_list
from sugar_network.toolkit.http import NotFound, Forbidden


class CommandsTest(tests.Test):

    def test_VolumeCommands(self):
        calls = []

        class TestCommandsProcessor(CommandsProcessor):

            @volume_command(method='PROBE')
            def command_1(self, **kwargs):
                calls.append(('command_1', kwargs))

            @volume_command(method='PROBE', cmd='command_2')
            def command_2(self, **kwargs):
                calls.append(('command_2', kwargs))

        cp = TestCommandsProcessor()

        self.call(cp, 'PROBE')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1')
        self.call(cp, 'PROBE', cmd='command_2')

        self.assertEqual([
            ('command_1', {}),
            ('command_2', {}),
            ],
            calls)

    def test_DirectoryCommands(self):
        calls = []

        class TestCommandsProcessor(CommandsProcessor):

            @directory_command(method='PROBE')
            def command_1(self, **kwargs):
                calls.append(('command_1', kwargs))

            @directory_command(method='PROBE', cmd='command_2')
            def command_2(self, **kwargs):
                calls.append(('command_2', kwargs))

        cp = TestCommandsProcessor()

        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE')
        self.call(cp, 'PROBE', document='testdocument')
        self.call(cp, 'PROBE', document='fakedocument')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1', document='testdocument')
        self.call(cp, 'PROBE', cmd='command_2', document='testdocument')
        self.call(cp, 'PROBE', cmd='command_2', document='fakedocument')

        self.assertEqual([
            ('command_1', {}),
            ('command_1', {}),
            ('command_2', {}),
            ('command_2', {}),
            ],
            calls)

    def test_DocumentCommands(self):
        calls = []

        class TestCommandsProcessor(CommandsProcessor):

            @document_command(method='PROBE')
            def command_1(self, **kwargs):
                calls.append(('command_1', kwargs))

            @document_command(method='PROBE', cmd='command_2')
            def command_2(self, **kwargs):
                calls.append(('command_2', kwargs))

        class TestDocument(Document):
            pass

        volume = Volume(tests.tmpdir, [TestDocument])
        cp = TestCommandsProcessor(volume)

        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument')
        self.call(cp, 'PROBE', document='testdocument', guid='guid')
        self.call(cp, 'PROBE', document='fakedocument', guid='guid')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1', document='testdocument', guid='guid')
        self.call(cp, 'PROBE', cmd='command_2', document='testdocument', guid='guid')
        self.call(cp, 'PROBE', cmd='command_2', document='fakedocument', guid='guid')

        self.assertEqual([
            ('command_1', {}),
            ('command_1', {}),
            ('command_2', {}),
            ('command_2', {}),
            ],
            calls)

    def test_PropertyCommands(self):
        calls = []

        class TestCommandsProcessor(CommandsProcessor):

            @property_command(method='PROBE')
            def command_1(self, **kwargs):
                calls.append(('command_1', kwargs))

            @property_command(method='PROBE', cmd='command_2')
            def command_2(self, **kwargs):
                calls.append(('command_2', kwargs))

        class TestDocument(Document):
            pass

        volume = Volume(tests.tmpdir, [TestDocument])
        cp = TestCommandsProcessor(volume)

        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument', guid='guid')
        self.call(cp, 'PROBE', document='testdocument', guid='guid', prop='prop')
        self.call(cp, 'PROBE', document='fakedocument', guid='guid', prop='prop')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1', document='testdocument', guid='guid', prop='prop')
        self.call(cp, 'PROBE', cmd='command_2', document='testdocument', guid='guid', prop='prop')
        self.call(cp, 'PROBE', cmd='command_2', document='fakedocument', guid='guid', prop='prop')

        self.assertEqual([
            ('command_1', {}),
            ('command_1', {}),
            ('command_2', {}),
            ('command_2', {}),
            ],
            calls)

    def test_ClassDodcumentCommands(self):
        calls = []

        class TestDocument(Document):

            @document_command(method='PROBE')
            def command_1(cls, **kwargs):
                calls.append(('command_1', kwargs))

            @document_command(method='PROBE', cmd='command_2')
            def command_2(cls, **kwargs):
                calls.append(('command_2', kwargs))

        volume = Volume(tests.tmpdir, [TestDocument])
        cp = CommandsProcessor(volume)

        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument')
        self.assertRaises(NotFound, self.call, cp, 'PROBE', document='testdocument', guid='guid')
        volume['testdocument'].create(guid='guid')
        self.call(cp, 'PROBE', document='testdocument', guid='guid')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='fakedocument', guid='guid')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1', document='testdocument', guid='guid')
        self.call(cp, 'PROBE', cmd='command_2', document='testdocument', guid='guid')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_2', document='fakedocument', guid='guid')

        self.assertEqual([
            ('command_1', {}),
            ('command_2', {}),
            ],
            calls)

    def test_ClassPropertyCommands(self):
        calls = []

        class TestDocument(Document):

            @property_command(method='PROBE')
            def command_1(cls, **kwargs):
                calls.append(('command_1', kwargs))

            @property_command(method='PROBE', cmd='command_2')
            def command_2(cls, **kwargs):
                calls.append(('command_2', kwargs))

        volume = Volume(tests.tmpdir, [TestDocument])
        cp = CommandsProcessor(volume)

        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='testdocument', prop='prop')
        self.assertRaises(NotFound, self.call, cp, 'PROBE', document='testdocument', guid='guid', prop='prop')
        volume['testdocument'].create(guid='guid')
        self.call(cp, 'PROBE', document='testdocument', guid='guid', prop='prop')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', document='fakedocument', guid='guid', prop='prop')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_1', document='testdocument', guid='guid', prop='prop')
        self.call(cp, 'PROBE', cmd='command_2', document='testdocument', guid='guid', prop='prop')
        self.assertRaises(CommandNotFound, self.call, cp, 'PROBE', cmd='command_2', document='fakedocument', guid='guid', prop='prop')

        self.assertEqual([
            ('command_1', {}),
            ('command_2', {}),
            ],
            calls)

    def test_AccessLevel(self):
        calls = []

        class TestCommandsProcessor(CommandsProcessor):

            @volume_command(method='PROBE', cmd='all')
            def all(self):
                pass

            @volume_command(method='PROBE', cmd='system', access_level=env.ACCESS_SYSTEM)
            def system(self):
                pass

            @volume_command(method='PROBE', cmd='local', access_level=env.ACCESS_LOCAL)
            def local(self):
                pass

            @volume_command(method='PROBE', cmd='remote', access_level=env.ACCESS_REMOTE)
            def remote(self):
                pass

        cp = TestCommandsProcessor()

        self.call(cp, 'PROBE', cmd='all', access_level=env.ACCESS_REMOTE)
        self.call(cp, 'PROBE', cmd='all', access_level=env.ACCESS_LOCAL)
        self.call(cp, 'PROBE', cmd='all', access_level=env.ACCESS_SYSTEM)

        self.call(cp, 'PROBE', cmd='remote', access_level=env.ACCESS_REMOTE)
        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='remote', access_level=env.ACCESS_LOCAL)
        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='remote', access_level=env.ACCESS_SYSTEM)

        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='local', access_level=env.ACCESS_REMOTE)
        self.call(cp, 'PROBE', cmd='local', access_level=env.ACCESS_LOCAL)
        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='local', access_level=env.ACCESS_SYSTEM)

        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='system', access_level=env.ACCESS_REMOTE)
        self.assertRaises(Forbidden, self.call, cp, 'PROBE', cmd='system', access_level=env.ACCESS_LOCAL)
        self.call(cp, 'PROBE', cmd='system', access_level=env.ACCESS_SYSTEM)

    def test_ParentClasses(self):
        calls = []

        class Parent(object):

            @volume_command(method='PROBE')
            def probe(self):
                return 'probe'

        class TestCommandsProcessor(CommandsProcessor, Parent):
            pass

        cp = TestCommandsProcessor()
        self.assertEqual('probe', self.call(cp, 'PROBE'))

    def test_OverrideInChildClass(self):
        calls = []

        class Parent(CommandsProcessor):

            @volume_command(method='PROBE')
            def probe(self):
                return 'probe-1'

            @volume_command(method='COMMON')
            def common(self):
                return 'common'

        class Child(Parent):

            @volume_command(method='PROBE')
            def probe(self):
                return 'probe-2'

            @volume_command(method='PARTICULAR')
            def particular(self):
                return 'particular'

        cp = Child()
        self.assertEqual('probe-2', self.call(cp, 'PROBE'))
        self.assertEqual('common', self.call(cp, 'COMMON'))
        self.assertEqual('particular', self.call(cp, 'PARTICULAR'))

    def test_RequestRead(self):

        class Stream(object):

            def __init__(self, value):
                self.pos = 0
                self.value = value

            def read(self, size):
                assert self.pos + size <= len(self.value)
                result = self.value[self.pos:self.pos + size]
                self.pos += size
                return result

        request = Request()
        request.content_stream = Stream('123')
        request.content_length = len(request.content_stream.value)
        self.assertEqual('123', request.read())
        self.assertEqual('', request.read())
        self.assertEqual('', request.read(10))

        request = Request()
        request.content_stream = Stream('123')
        request.content_length = len(request.content_stream.value)
        self.assertEqual('123', request.read(10))

        request = Request()
        request.content_stream = Stream('123')
        request.content_length = len(request.content_stream.value)
        self.assertEqual('1', request.read(1))
        self.assertEqual('2', request.read(1))
        self.assertEqual('3', request.read())

    def test_Arguments(self):

        class TestCommandsProcessor(CommandsProcessor):

            @volume_command(method='PROBE', arguments={'arg_int': to_int, 'arg_list': to_list})
            def probe(self, arg_int=None, arg_list=None):
                return arg_int, arg_list

        cp = TestCommandsProcessor()

        self.assertEqual((None, None), self.call(cp, 'PROBE'))
        self.assertEqual((-1, [-2, None]), self.call(cp, 'PROBE', arg_int=-1, arg_list=[-2, None]))
        self.assertEqual((4, [' foo', ' bar  ', '  ']), self.call(cp, 'PROBE', arg_int='4', arg_list=' foo, bar  ,  '))
        self.assertEqual((None, ['foo']), self.call(cp, 'PROBE', arg_list='foo'))
        self.assertEqual((None, []), self.call(cp, 'PROBE', arg_list=''))
        self.assertEqual((None, [' ']), self.call(cp, 'PROBE', arg_list=' '))
        self.assertEqual((0, None), self.call(cp, 'PROBE', arg_int=''))
        self.assertRaises(RuntimeError, self.call, cp, 'PROBE', arg_int=' ')
        self.assertRaises(RuntimeError, self.call, cp, 'PROBE', arg_int='foo')

    def test_PassKwargs(self):

        class TestCommandsProcessor(CommandsProcessor):

            @volume_command(method='PROBE')
            def probe(self, arg, request, response, **kwargs):
                return arg, dict(request), dict(response), kwargs

        cp = TestCommandsProcessor()

        self.assertEqual(
                (None, {'method': 'PROBE'}, {}, {}),
                self.call(cp, 'PROBE'))
        self.assertEqual(
                (1, {'method': 'PROBE', 'arg': 1}, {}, {}),
                self.call(cp, 'PROBE', arg=1))
        self.assertEqual(
                (None, {'method': 'PROBE', 'foo': 'bar'}, {}, {}),
                self.call(cp, 'PROBE', foo='bar'))
        self.assertEqual(
                (-2, {'method': 'PROBE', 'foo': 'bar', 'arg': -2}, {}, {}),
                self.call(cp, 'PROBE', foo='bar', arg=-2))

    def test_PrePost(self):

        class ParentCommandsProcessor(CommandsProcessor):

            @db.volume_command_pre(method='PROBE')
            def command_pre1(self, request):
                request['probe'].append('pre1')

            @db.volume_command_pre(method='PROBE')
            def command_pre2(self, request):
                request['probe'].append('pre2')

            @db.volume_command_post(method='PROBE')
            def command_post1(self, request, response, result):
                request['probe'].append('post1')
                response['probe'].append('post1')
                return result + 1

            @db.volume_command_post(method='PROBE')
            def command_post2(self, request, response, result):
                request['probe'].append('post2')
                response['probe'].append('post2')
                return result + 1

        class TestCommandsProcessor(ParentCommandsProcessor):

            @db.volume_command_pre(method='PROBE')
            def command_pre3(self, request):
                request['probe'].append('pre3')

            @db.volume_command_pre(method='PROBE')
            def command_pre4(self, request):
                request['probe'].append('pre4')

            @db.volume_command(method='PROBE')
            def command(self, request):
                request['probe'].append('cmd')
                response['probe'].append('cmd')
                return 1

            @db.volume_command_post(method='PROBE')
            def command_post3(self, request, response, result):
                request['probe'].append('post3')
                response['probe'].append('post3')
                return result + 1

            @db.volume_command_post(method='PROBE')
            def command_post4(self, request, response, result):
                request['probe'].append('post4')
                response['probe'].append('post4')
                return result + 1

        cp = TestCommandsProcessor()

        request = db.Request(method='PROBE', probe=[])
        response = db.Response(probe=[])
        self.assertEqual(5, cp.call(request, response))
        self.assertEqual(['pre1', 'pre2', 'pre3', 'pre4', 'cmd', 'post1', 'post2', 'post3', 'post4'], request['probe'])
        self.assertEqual(['cmd', 'post1', 'post2', 'post3', 'post4'], response['probe'])

    def test_PrePostCallbackLess(self):

        class TestCommandsProcessor(CommandsProcessor):

            @db.volume_command_pre(method='PROBE')
            def command_pre(self, request):
                request['probe'].append('pre')

            def super_call(self, request, response):
                request['probe'].append('cmd')
                response['probe'].append('cmd')
                return 1

            @db.volume_command_post(method='PROBE')
            def command_post(self, request, response, result):
                request['probe'].append('post')
                response['probe'].append('post')
                return result + 1

        cp = TestCommandsProcessor()

        request = db.Request(method='PROBE', probe=[])
        response = db.Response(probe=[])
        self.assertEqual(2, cp.call(request, response))
        self.assertEqual(['pre', 'cmd', 'post'], request['probe'])
        self.assertEqual(['cmd', 'post'], response['probe'])

    def test_SubCall(self):

        class TestCommandsProcessor(CommandsProcessor):

            @db.volume_command(method='PROBE')
            def command1(self, request):
                return request.call('PROBE', cmd='command2')

            @db.volume_command(method='PROBE')
            def command2(self, request):
                return {'access_level': request.access_level, 'accept_language': request.accept_language}

        cp = TestCommandsProcessor()

        request = db.Request(method='PROBE')
        request.access_level = -1
        request.accept_language = 'foo'
        self.assertEqual({
            'access_level': -1,
            'accept_language': 'foo',
            },
            cp.call(request, db.Response()))

    def call(self, cp, method, document=None, guid=None, prop=None,
            access_level=env.ACCESS_REMOTE, **kwargs):

        class TestRequest(Request):

            content_stream = None
            content_length = 0

        request = TestRequest(**kwargs)
        request['method'] = method
        request.access_level = access_level
        if document:
            request['document'] = document
        if guid:
            request['guid'] = guid
        if prop:
            request['prop'] = prop
        if 'content_stream' in request:
            request.content_stream = request.pop('content_stream')
            request.content_length = len(request.content_stream.getvalue())

        self.response = Response()
        return cp.call(request, self.response)


if __name__ == '__main__':
    tests.main()
