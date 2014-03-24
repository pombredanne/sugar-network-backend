#!/usr/bin/env python
# sugar-lint: disable

import os
import sys
import time
import shutil
import hashlib
from cStringIO import StringIO
from email.message import Message
from email.utils import formatdate
from os.path import dirname, join, abspath, exists

src_root = abspath(dirname(__file__))

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.model.user import User
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, ACL, File
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import coroutine, http, i18n


class RoutesTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)
        this.localcast = lambda x: x

    def test_PostDefaults(self):

        class Document(db.Resource):

            @db.stored_property(default='default')
            def w_default(self, value):
                return value

            @db.stored_property()
            def wo_default(self, value):
                return value

            @db.stored_property(default='not_stored_default')
            def not_stored_default(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))

        self.assertRaises(RuntimeError, this.call, method='POST', path=['document'], content={})

        guid = this.call(method='POST', path=['document'], content={'wo_default': 'wo_default'})
        self.assertEqual('default', this.call(method='GET', path=['document', guid, 'w_default']))
        self.assertEqual('wo_default', this.call(method='GET', path=['document', guid, 'wo_default']))
        self.assertEqual('not_stored_default', this.call(method='GET', path=['document', guid, 'not_stored_default']))

    def test_Populate(self):
        self.touch(
                ('db/document/1/1/guid', '{"value": "1"}'),
                ('db/document/1/1/ctime', '{"value": 1}'),
                ('db/document/1/1/mtime', '{"value": 1}'),
                ('db/document/1/1/seqno', '{"value": 0}'),

                ('db/document/2/2/guid', '{"value": "2"}'),
                ('db/document/2/2/ctime', '{"value": 2}'),
                ('db/document/2/2/mtime', '{"value": 2}'),
                ('db/document/2/2/seqno', '{"value": 0}'),
                )

        class Document(db.Resource):
            pass

        with db.Volume(tests.tmpdir, [Document]) as volume:
            router = Router(db.Routes(volume))
            for __ in volume['document'].populate():
                pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('index/document')

        class Document(db.Resource):
            pass

        with db.Volume(tests.tmpdir, [Document]) as volume:
            router = Router(db.Routes(volume))
            for __ in volume['document'].populate():
                pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

    def test_Commands(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        volume['testdocument'].create({'guid': 'guid'})

        self.assertEqual({
            'total': 1,
            'result': [
                {'guid': 'guid', 'prop': ''},
                ],
            },
            this.call(method='GET', path=['testdocument'], reply=['guid', 'prop']))

        guid_1 = this.call(method='POST', path=['testdocument'], content={'prop': 'value_1'})
        assert guid_1
        guid_2 = this.call(method='POST', path=['testdocument'], content={'prop': 'value_2'})
        assert guid_2

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_1'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(this.call(method='GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        this.call(method='PUT', path=['testdocument', guid_1], content={'prop': 'value_3'})

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(this.call(method='GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        this.call(method='DELETE', path=['testdocument', guid_2])

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    ]),
                sorted(this.call(method='GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid_2])

        self.assertEqual(
                {'guid': guid_1, 'prop': 'value_3'},
                this.call(method='GET', path=['testdocument', guid_1], reply=['guid', 'prop']))

        self.assertEqual(
                'value_3',
                this.call(method='GET', path=['testdocument', guid_1, 'prop']))

    def test_SetBLOBs(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content='blob1')
        self.assertEqual('blob1', file(this.call(method='GET', path=['testdocument', guid, 'blob']).path).read())

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content_stream=StringIO('blob2'))
        self.assertEqual('blob2', file(this.call(method='GET', path=['testdocument', guid, 'blob']).path).read())

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content=None)
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

    def test_CreateBLOBsWithMeta(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={})

        self.assertRaises(http.BadRequest, this.call, method='PUT', path=['testdocument', guid, 'blob'],
                content={}, content_type='application/json')
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

        self.assertRaises(http.BadRequest, this.call, method='PUT', path=['testdocument', guid, 'blob'],
                content={'location': 'foo'}, content_type='application/json')
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

        self.assertRaises(http.BadRequest, this.call, method='PUT', path=['testdocument', guid, 'blob'],
                content={'location': 'url', 'digest': 'digest', 'foo': 'bar', 'content-type': 'foo/bar'},
                content_type='application/json')
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

    def test_UpdateBLOBsWithMeta(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'blob': 'blob'})
        blob = this.call(method='GET', path=['testdocument', guid, 'blob'], environ={'HTTP_HOST': 'localhost'})
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': '4',
            'x-seqno': '1',
            },
            dict(blob))
        self.assertEqual('blob', file(blob.path).read())

        self.assertRaises(http.BadRequest, this.call, method='PUT', path=['testdocument', guid, 'blob'],
                content={'foo': 'bar'}, content_type='application/json')
        blob = this.call(method='GET', path=['testdocument', guid, 'blob'], environ={'HTTP_HOST': 'localhost'})
        self.assertEqual({
            'content-type': 'application/octet-stream',
            'content-length': '4',
            'x-seqno': '1',
            },
            dict(blob))
        self.assertEqual('blob', file(blob.path).read())

    def test_RemoveBLOBs(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={'blob': 'blob'})

        self.assertEqual('blob', file(this.call(method='GET', path=['testdocument', guid, 'blob']).path).read())

        this.call(method='PUT', path=['testdocument', guid, 'blob'])
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

    def test_ReuploadBLOBs(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={'blob': 'blob1'})

        blob1 = this.call(method='GET', path=['testdocument', guid, 'blob'])
        self.assertEqual('blob1', file(blob1.path).read())

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content='blob2')
        blob2 = this.call(method='GET', path=['testdocument', guid, 'blob'])
        self.assertEqual('blob2', file(blob2.path).read())
        assert blob1.path != blob2.path
        assert not exists(blob1.path)

    def test_RemoveBLOBsOnFailedSetter(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

            @blob.setter
            def blob(self, value):
                if value:
                    raise RuntimeError()
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])

        self.assertRaises(RuntimeError, this.call, method='PUT', path=['testdocument', guid, 'blob'], content='probe')
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])
        assert not exists('blobs/%s' % hashlib.sha1('probe').hexdigest())

    def test_SetBLOBsWithMimeType(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob, mime_type='default')
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={})

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content='blob1')
        response = []
        [i for i in router({
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/testdocument/%s/blob' % guid,
            }, lambda status, headers: response.extend([status, headers]))]
        self.assertEqual('200 OK', response[0])
        self.assertEqual('default', dict(response[1]).get('content-type'))

        this.call(method='PUT', path=['testdocument', guid, 'blob'], content='blob1', content_type='foo')
        response = []
        [i for i in router({
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/testdocument/%s/blob' % guid,
            }, lambda status, headers: response.extend([status, headers]))]
        self.assertEqual('200 OK', response[0])
        self.assertEqual('foo', dict(response[1]).get('content-type'))

        this.call(method='PUT', path=['testdocument', guid], content={'blob': 'blob2'}, content_type='bar')
        response = []
        [i for i in router({
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': '/testdocument/%s/blob' % guid,
            }, lambda status, headers: response.extend([status, headers]))]
        self.assertEqual('200 OK', response[0])
        self.assertEqual('default', dict(response[1]).get('content-type'))

    def test_GetBLOBs(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        blob = 'blob'
        this.call(method='PUT', path=['testdocument', guid, 'blob'], content=blob)
        digest = str(hashlib.sha1(blob).hexdigest())
        blob_path = 'blobs/%s' % digest

        self.assertEqual('blob', file(this.call(method='GET', path=['testdocument', guid, 'blob']).path).read())

        self.assertEqual({
            'blob': 'http://localhost/blobs/%s' % digest,
            },
            this.call(method='GET', path=['testdocument', guid], reply=['blob'], environ={'HTTP_HOST': 'localhost'}))

        self.assertEqual([{
            'blob': 'http://localhost/blobs/%s' % digest,
            }],
            this.call(method='GET', path=['testdocument'], reply=['blob'], environ={'HTTP_HOST': 'localhost'})['result'])

    def test_GetBLOBsByUrls(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid1 = this.call(method='POST', path=['testdocument'], content={})

        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid1, 'blob'])
        self.assertEqual(
                {'blob': ''},
                this.call(method='GET', path=['testdocument', guid1], reply=['blob'], environ={'HTTP_HOST': '127.0.0.1'}))
        self.assertEqual(
                sorted([
                    '',
                    ]),
                sorted([i['blob'] for i in this.call(method='GET', path=['testdocument'], reply=['blob'],
                    environ={'HTTP_HOST': '127.0.0.1'})['result']]))

        blob = 'file'
        digest = hashlib.sha1(blob).hexdigest()
        guid2 = this.call(method='POST', path=['testdocument'], content={'blob': blob})
        self.assertEqual(
                'http://127.0.0.1/blobs/%s' % digest,
                this.call(method='GET', path=['testdocument', guid2], reply=['blob'], environ={'HTTP_HOST': '127.0.0.1'})['blob'])
        self.assertEqual(
                sorted([
                    '',
                    'http://127.0.0.1/blobs/%s' % digest,
                    ]),
                sorted([i['blob'] for i in this.call(method='GET', path=['testdocument'], reply=['blob'],
                    environ={'HTTP_HOST': '127.0.0.1'})['result']]))

        volume.blobs.update(digest, {
            'location': 'http://foo',
            })
        self.assertEqual(
                'http://foo',
                this.call(method='GET', path=['testdocument', guid2], reply=['blob'], environ={'HTTP_HOST': '127.0.0.1'})['blob'])
        self.assertEqual(
                sorted([
                    '',
                    'http://foo',
                    ]),
                sorted([i['blob'] for i in this.call(method='GET', path=['testdocument'], reply=['blob'],
                    environ={'HTTP_HOST': '127.0.0.1'})['result']]))

    def test_CommandsGetAbsentBlobs(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        self.assertRaises(http.NotFound, this.call, method='GET', path=['testdocument', guid, 'blob'])
        self.assertEqual(
                {'blob': ''},
                this.call(method='GET', path=['testdocument', guid], reply=['blob'], environ={'HTTP_HOST': 'localhost'}))

    def test_Command_ReplyForGET(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={'prop': 'value'})

        self.assertEqual(
                ['guid', 'prop'],
                this.call(method='GET', path=['testdocument', guid], reply=['guid', 'prop']).keys())

        self.assertEqual(
                ['guid'],
                this.call(method='GET', path=['testdocument'])['result'][0].keys())

        self.assertEqual(
                sorted(['guid', 'prop']),
                sorted(this.call(method='GET', path=['testdocument'], reply=['prop', 'guid'])['result'][0].keys()))

        self.assertEqual(
                sorted(['prop']),
                sorted(this.call(method='GET', path=['testdocument'], reply=['prop'])['result'][0].keys()))

    def test_DecodeBeforeSetting(self):

        class TestDocument(db.Resource):

            @db.indexed_property(db.Numeric, slot=1)
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop': '-1'})
        self.assertEqual(-1, this.call(method='GET', path=['testdocument', guid, 'prop']))

    def test_LocalizedSet(self):
        i18n._default_langs = ['en']

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        directory = volume['testdocument']
        guid = this.call(method='POST', path=['testdocument'], content={'localized_prop': 'value_ru'},
                environ={'HTTP_ACCEPT_LANGUAGE': 'ru'})

        self.assertEqual({'ru': 'value_ru'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_ru')[0]])

        this.call(method='PUT', path=['testdocument', guid], content={'localized_prop': 'value_en'},
                environ={'HTTP_ACCEPT_LANGUAGE': 'en'})
        self.assertEqual({'ru': 'value_ru', 'en': 'value_en'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_ru')[0]])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_en')[0]])

    def test_LocalizedGet(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        directory = volume['testdocument']

        guid = this.call(method='POST', path=['testdocument'], content={
            'localized_prop': {
                'ru': 'value_ru',
                'es': 'value_es',
                'en': 'value_en',
                },
            })

        i18n._default_langs = ['en']

        self.assertEqual(
                {'localized_prop': 'value_en'},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop']))
        self.assertEqual(
                {'localized_prop': 'value_ru'},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'ru'}))
        self.assertEqual(
                'value_ru',
                this.call(method='GET', path=['testdocument', guid, 'localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'ru,es'}))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                this.call(method='GET', path=['testdocument'], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'foo,ru,es'})['result'])

        self.assertEqual(
                {'localized_prop': 'value_ru'},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'ru-RU'}))
        self.assertEqual(
                'value_ru',
                this.call(method='GET', path=['testdocument', guid, 'localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'ru-RU,es'}))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                this.call(method='GET', path=['testdocument'], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'ru-RU,es'})['result'])

        self.assertEqual(
                {'localized_prop': 'value_es'},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'es'}))
        self.assertEqual(
                'value_es',
                this.call(method='GET', path=['testdocument', guid, 'localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'es,ru'}))
        self.assertEqual(
                [{'localized_prop': 'value_es'}],
                this.call(method='GET', path=['testdocument'], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'foo,es,ru'})['result'])

        self.assertEqual(
                {'localized_prop': 'value_en'},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'fr'}))
        self.assertEqual(
                'value_en',
                this.call(method='GET', path=['testdocument', guid, 'localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'fr,za'}))
        self.assertEqual(
                [{'localized_prop': 'value_en'}],
                this.call(method='GET', path=['testdocument'], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'foo,fr,za'})['result'])

        i18n._default_langs = ['foo']
        fallback_lang = sorted(['ru', 'es', 'en'])[0]

        self.assertEqual(
                {'localized_prop': 'value_%s' % fallback_lang},
                this.call(method='GET', path=['testdocument', guid], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'fr'}))
        self.assertEqual(
                'value_%s' % fallback_lang,
                this.call(method='GET', path=['testdocument', guid, 'localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'fr,za'}))
        self.assertEqual(
                [{'localized_prop': 'value_%s' % fallback_lang}],
                this.call(method='GET', path=['testdocument'], reply=['localized_prop'],
                    environ={'HTTP_ACCEPT_LANGUAGE': 'foo,fr,za'})['result'])

    def test_OpenByModuleName(self):
        self.touch(
                ('foo/bar.py', [
                    'from sugar_network import db',
                    'class Bar(db.Resource): pass',
                    ]),
                ('foo/__init__.py', ''),
                )
        sys.path.insert(0, '.')

        volume = db.Volume('.', ['foo.bar'])
        volume['bar'].find()
        assert exists('index/bar')
        volume.close()

    def test_on_create(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        ts = int(time.time())
        guid = this.call(method='POST', path=['testdocument'], content={})
        assert volume['testdocument'].get(guid)['ctime'] in range(ts - 1, ts + 1)
        assert volume['testdocument'].get(guid)['mtime'] in range(ts - 1, ts + 1)

    def test_on_create_Override(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

            def created(self):
                self.posts['prop'] = 'overriden'

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop': 'foo'})
        self.assertEqual('overriden', volume['testdocument'].get(guid)['prop'])

        this.call(method='PUT', path=['testdocument', guid], content={'prop': 'bar'})
        self.assertEqual('bar', volume['testdocument'].get(guid)['prop'])

    def test_on_update(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        prev_mtime = volume['testdocument'].get(guid)['mtime']

        time.sleep(1)

        this.call(method='PUT', path=['testdocument', guid], content={'prop': 'probe'})
        assert volume['testdocument'].get(guid)['mtime'] - prev_mtime >= 1

    def test_on_update_Override(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

            def updated(self):
                self.posts['prop'] = 'overriden'

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop': 'foo'})
        self.assertEqual('foo', volume['testdocument'].get(guid)['prop'])

        this.call(method='PUT', path=['testdocument', guid], content={'prop': 'bar'})
        self.assertEqual('overriden', volume['testdocument'].get(guid)['prop'])

    def __test_DoNotPassGuidsForCreate(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(db.Localized, prefix='L', default={})
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        self.assertRaises(http.Forbidden, this.call, method='POST', path=['testdocument'], content={'guid': 'foo'})
        guid = this.call(method='POST', path=['testdocument'], content={})
        assert guid

    def test_seqno(self):

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [Document1, Document2])
        router = Router(db.Routes(volume))

        self.assertEqual(0, volume.seqno.value)

        volume['document1'].create({'guid': '1'})
        self.assertEqual(1, volume['document1'].get('1')['seqno'])
        volume['document2'].create({'guid': '1'})
        self.assertEqual(2, volume['document2'].get('1')['seqno'])
        volume['document1'].create({'guid': '2'})
        self.assertEqual(3, volume['document1'].get('2')['seqno'])
        volume['document2'].create({'guid': '2'})
        self.assertEqual(4, volume['document2'].get('2')['seqno'])

        self.assertEqual(4, volume.seqno.value)
        volume.seqno.commit()
        volume = db.Volume(tests.tmpdir, [Document1, Document2])
        self.assertEqual(4, volume.seqno.value)

    def test_Events(self):
        db.index_flush_threshold.value = 0
        db.index_flush_timeout.value = 0

        class Document1(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                pass

        class Document2(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                pass

            @db.stored_property(db.Blob)
            def blob(self, value):
                return value

        self.touch(
                ('db/document1/1/1/guid', '{"value": "1"}'),
                ('db/document1/1/1/ctime', '{"value": 1}'),
                ('db/document1/1/1/mtime', '{"value": 1}'),
                ('db/document1/1/1/prop', '{"value": ""}'),
                ('db/document1/1/1/seqno', '{"value": 0}'),
                )

        events = []
        this.localcast = lambda x: events.append(x)
        this.broadcast = lambda x: events.append(x)
        volume = db.Volume(tests.tmpdir, [Document1, Document2])
        volume['document1']
        volume['document2']
        coroutine.sleep(.1)

        mtime = int(os.stat('index/document1/mtime').st_mtime)
        self.assertEqual([
            {'event': 'commit', 'resource': 'document1', 'mtime': mtime},
            ],
            events)
        del events[:]

        volume['document1'].create({'guid': 'guid1'})
        volume['document2'].create({'guid': 'guid2'})
        self.assertEqual([
            {'event': 'create', 'resource': 'document1', 'guid': 'guid1'},
            {'event': 'create', 'resource': 'document2', 'guid': 'guid2'},
            ],
            events)
        del events[:]

        volume['document1'].update('guid1', {'prop': 'foo'})
        volume['document2'].update('guid2', {'prop': 'bar'})
        self.assertEqual([
            {'event': 'update', 'resource': 'document1', 'guid': 'guid1', 'props': {'prop': 'foo'}},
            {'event': 'update', 'resource': 'document2', 'guid': 'guid2', 'props': {'prop': 'bar'}},
            ],
            events)
        del events[:]

        volume['document1'].delete('guid1')
        self.assertEqual([
            {'event': 'delete', 'resource': 'document1', 'guid': 'guid1'},
            ],
            events)
        del events[:]

        volume['document1'].commit()
        mtime1 = int(os.stat('index/document1/mtime').st_mtime)
        volume['document2'].commit()
        mtime2 = int(os.stat('index/document2/mtime').st_mtime)

        self.assertEqual([
            {'event': 'commit', 'resource': 'document1', 'mtime': mtime1},
            {'event': 'commit', 'resource': 'document2', 'mtime': mtime2},
            ],
            events)

    def test_PermissionsNoWrite(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='', acl=ACL.READ)
            def prop(self, value):
                pass

            @db.stored_property(db.Blob, acl=ACL.READ)
            def blob(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={})

        self.assertRaises(http.Forbidden, this.call, method='POST', path=['testdocument'], content={'prop': 'value'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid], content={'prop': 'value'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid], content={'blob': 'value'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid, 'prop'], content='value')
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid, 'blob'], content='value')

    def test_BlobsWritePermissions(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Blob, acl=ACL.CREATE | ACL.WRITE)
            def blob1(self, value):
                return value

            @db.stored_property(db.Blob, acl=ACL.CREATE)
            def blob2(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        this.call(method='PUT', path=['testdocument', guid], content={'blob1': 'value1', 'blob2': 'value2'})
        this.call(method='PUT', path=['testdocument', guid], content={'blob1': 'value1'})
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid], content={'blob2': 'value2_'})

        guid = this.call(method='POST', path=['testdocument'], content={})
        this.call(method='PUT', path=['testdocument', guid, 'blob1'], content='value1')
        this.call(method='PUT', path=['testdocument', guid, 'blob2'], content='value2')
        this.call(method='PUT', path=['testdocument', guid, 'blob1'], content='value1_')
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['testdocument', guid, 'blob2'], content='value2_')

    def test_properties_OverrideGet(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='1')
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2, default='2')
            def prop2(self, value):
                return -1

            @db.stored_property(db.Blob)
            def blob(self, meta):
                meta['blob'] = 'new-blob'
                return meta

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={})
        self.touch(('new-blob', 'new-blob'))
        this.call(method='PUT', path=['testdocument', guid, 'blob'], content='old-blob')

        self.assertEqual(
                'new-blob',
                this.call(method='GET', path=['testdocument', guid, 'blob'])['blob'])
        self.assertEqual(
                '1',
                this.call(method='GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(
                -1,
                this.call(method='GET', path=['testdocument', guid, 'prop2']))
        self.assertEqual(
                {'prop1': '1', 'prop2': -1},
                this.call(method='GET', path=['testdocument', guid], reply=['prop1', 'prop2']))

    def test_properties_OverrideSetter(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='1')
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return '_%s' % value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={})

        self.assertEqual('_1', this.call(method='GET', path=['testdocument', guid, 'prop']))

        this.call(method='PUT', path=['testdocument', guid, 'prop'], content='2')
        self.assertEqual('_2', this.call(method='GET', path=['testdocument', guid, 'prop']))

        this.call(method='PUT', path=['testdocument', guid], content={'prop': 3})
        self.assertEqual('_3', this.call(method='GET', path=['testdocument', guid, 'prop']))

    def test_properties_AccessToOldValuesInSetters(self):

        class TestDocument(db.Resource):

            @db.stored_property(db.Numeric)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + (self.orig('prop') or 0)

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop': 1})
        self.assertEqual(1, this.call(method='GET', path=['testdocument', guid, 'prop']))

        this.call(method='PUT', path=['testdocument', guid, 'prop'], content='2')
        self.assertEqual(3, this.call(method='GET', path=['testdocument', guid, 'prop']))

        this.call(method='PUT', path=['testdocument', guid], content={'prop': 3})
        self.assertEqual(6, this.call(method='GET', path=['testdocument', guid, 'prop']))

    def test_properties_CallSettersAtTheEnd(self):

        class TestDocument(db.Resource):

            @db.indexed_property(db.Numeric, slot=1)
            def prop1(self, value):
                return value

            @prop1.setter
            def prop1(self, value):
                return self['prop3'] + value

            @db.indexed_property(db.Numeric, slot=2)
            def prop2(self, value):
                return value

            @prop2.setter
            def prop2(self, value):
                return self['prop3'] - value

            @db.indexed_property(db.Numeric, slot=3)
            def prop3(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop1': 1, 'prop2': 2, 'prop3': 3})
        self.assertEqual(4, this.call(method='GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(1, this.call(method='GET', path=['testdocument', guid, 'prop2']))

    def test_properties_PopulateRequiredPropsInSetters(self):

        class TestDocument(db.Resource):

            @db.indexed_property(db.Numeric, slot=1)
            def prop1(self, value):
                return value

            @prop1.setter
            def prop1(self, value):
                self.post('prop2', value + 1)
                return value

            @db.indexed_property(db.Numeric, slot=2)
            def prop2(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['testdocument'], content={'prop1': 1})
        self.assertEqual(1, this.call(method='GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(2, this.call(method='GET', path=['testdocument', guid, 'prop2']))

    def test_Group(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        this.call(method='POST', path=['testdocument'], content={'prop': 1})
        this.call(method='POST', path=['testdocument'], content={'prop': 2})
        this.call(method='POST', path=['testdocument'], content={'prop': 1})

        self.assertEqual(
                sorted([{'prop': 1}, {'prop': 2}]),
                sorted(this.call(method='GET', path=['testdocument'], reply='prop', group_by='prop')['result']))

    def test_CallSetterEvenIfThereIsNoCreatePermissions(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.READ, default=0)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + 1

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))

        self.assertRaises(http.Forbidden, this.call, method='POST', path=['testdocument'], content={'prop': 1})

        guid = this.call(method='POST', path=['testdocument'], content={})
        self.assertEqual(1, this.call(method='GET', path=['testdocument', guid, 'prop']))

    def test_ReturnDefualtsForMissedProps(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='default')
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['testdocument'], content={'prop': 'set'})

        self.assertEqual(
                [{'prop': 'set'}],
                this.call(method='GET', path=['testdocument'], reply='prop')['result'])
        self.assertEqual(
                {'prop': 'set'},
                this.call(method='GET', path=['testdocument', guid], reply='prop'))
        self.assertEqual(
                'set',
                this.call(method='GET', path=['testdocument', guid, 'prop']))

        os.unlink('db/testdocument/%s/%s/prop' % (guid[:2], guid))

        self.assertEqual(
                [{'prop': 'default'}],
                this.call(method='GET', path=['testdocument'], reply='prop')['result'])
        self.assertEqual(
                {'prop': 'default'},
                this.call(method='GET', path=['testdocument', guid], reply='prop'))
        self.assertEqual(
                'default',
                this.call(method='GET', path=['testdocument', guid, 'prop']))

    def test_DefaultAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['document'], content={}, principal='user')
        self.assertEqual(
                [{'name': 'user', 'role': 2}],
                this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual(
                {'user': {'role': 2, 'order': 0}},
                volume['document'].get(guid)['author'])

        volume['user'].create({'guid': 'user', 'pubkey': '', 'name': 'User'})

        guid = this.call(method='POST', path=['document'], content={}, principal='user')
        self.assertEqual(
                [{'guid': 'user', 'name': 'User', 'role': 3}],
                this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual(
                {'user': {'name': 'User', 'role': 3, 'order': 0}},
                volume['document'].get(guid)['author'])

    def test_FindByAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'UserName1'})
        volume['user'].create({'guid': 'user2', 'pubkey': '', 'name': 'User Name2'})
        volume['user'].create({'guid': 'user3', 'pubkey': '', 'name': 'User Name 3'})

        guid1 = this.call(method='POST', path=['document'], content={}, principal='user1')
        guid2 = this.call(method='POST', path=['document'], content={}, principal='user2')
        guid3 = this.call(method='POST', path=['document'], content={}, principal='user3')

        self.assertEqual(sorted([
            {'guid': guid1},
            ]),
            this.call(method='GET', path=['document'], author='UserName1')['result'])

        self.assertEqual(sorted([
            {'guid': guid1},
            ]),
            sorted(this.call(method='GET', path=['document'], query='author:UserName')['result']))
        self.assertEqual(sorted([
            {'guid': guid1},
            {'guid': guid2},
            {'guid': guid3},
            ]),
            sorted(this.call(method='GET', path=['document'], query='author:User')['result']))
        self.assertEqual(sorted([
            {'guid': guid2},
            {'guid': guid3},
            ]),
            sorted(this.call(method='GET', path=['document'], query='author:Name')['result']))

    def test_PreserveAuthorsOrder(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'User1'})
        volume['user'].create({'guid': 'user2', 'pubkey': '', 'name': 'User2'})
        volume['user'].create({'guid': 'user3', 'pubkey': '', 'name': 'User3'})

        guid = this.call(method='POST', path=['document'], content={}, principal='user1')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user2', role=0)
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='userdel', user='user2', principal='user1')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='userdel', user='user2', principal='user1')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='userdel', user='user3', principal='user1')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            'user3': {'name': 'User3', 'role': 1, 'order': 4},
            },
            volume['document'].get(guid)['author'])

    def test_AddUser(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'User1'})
        volume['user'].create({'guid': 'user2', 'pubkey': '', 'name': 'User2'})

        guid = this.call(method='POST', path=['document'], content={}, principal='user1')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='User3', role=3)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='User4', role=4)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            {'name': 'User4', 'role': 0},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            'User4': {'role': 0, 'order': 3},
            },
            volume['document'].get(guid)['author'])

    def test_UpdateAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'User1'})
        guid = this.call(method='POST', path=['document'], content={}, principal='user1')

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='User2', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'name': 'User2', 'role': 0},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user1', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 0},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='useradd', user='User2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 2},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 2, 'order': 1},
            },
            volume['document'].get(guid)['author'])

    def test_DelUser(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

            @db.stored_property()
            def pubkey(self, value):
                return value

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))

        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'User1'})
        volume['user'].create({'guid': 'user2', 'pubkey': '', 'name': 'User2'})
        guid = this.call(method='POST', path=['document'], content={}, principal='user1')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='user2')
        this.call(method='PUT', path=['document', guid], cmd='useradd', user='User3')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        # Do not remove yourself
        self.assertRaises(RuntimeError, this.call, method='PUT', path=['document', guid], cmd='userdel', user='user1', principal='user1')
        self.assertRaises(RuntimeError, this.call, method='PUT', path=['document', guid], cmd='userdel', user='user2', principal='user2')

        this.call(method='PUT', path=['document', guid], cmd='userdel', user='user1', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            volume['document'].get(guid)['author'])

        this.call(method='PUT', path=['document', guid], cmd='userdel', user='User3', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            this.call(method='GET', path=['document', guid, 'author']))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            },
            volume['document'].get(guid)['author'])

    def test_DefaultOrder(self):

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))

        volume['document'].create({'guid': '3', 'ctime': 3})
        volume['document'].create({'guid': '2', 'ctime': 2})
        volume['document'].create({'guid': '1', 'ctime': 1})

        self.assertEqual([
            {'guid': '1'},
            {'guid': '2'},
            {'guid': '3'},
            ],
            this.call(method='GET', path=['document'])['result'])

    def test_DefaultsOnNonePostValues(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, default='default')
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['document'], content={'prop': None})
        self.assertEqual('default', this.call(method='GET', path=['document', guid, 'prop']))

    def test_InsertAggprops(self):

        class Document(db.Resource):

            @db.stored_property(default='')
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.INSERT)
            def prop3(self, value):
                return value

        events = []
        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        this.localcast = lambda x: events.append(x)
        guid = this.call(method='POST', path=['document'], content={})

        self.assertRaises(http.NotFound, this.call, method='POST', path=['document', 'foo', 'bar'], content={})
        self.assertRaises(http.NotFound, this.call, method='POST', path=['document', guid, 'bar'], content={})
        self.assertRaises(http.BadRequest, this.call, method='POST', path=['document', guid, 'prop1'], content={})

        del events[:]
        self.override(time, 'time', lambda: 0)
        self.override(toolkit, 'uuid', lambda: '0')
        self.assertEqual('0', this.call(method='POST', path=['document', guid, 'prop3'], content=0))
        self.assertEqual({
            '0': {'seqno': 2, 'value': 0},
            },
            volume['document'].get(guid)['prop3'])
        self.assertEqual([
            {'event': 'update', 'resource': 'document', 'guid': guid, 'props': {
                'mtime': 0,
                'prop3': {'0': {'seqno': 2, 'value': 0}},
                }},
            ],
            events)

        self.override(time, 'time', lambda: 1)
        self.override(toolkit, 'uuid', lambda: '1')
        self.assertEqual('1', this.call(method='POST', path=['document', guid, 'prop3'], content={'foo': 'bar'}))
        self.assertEqual({
            '0': {'seqno': 2, 'value': 0},
            '1': {'seqno': 3, 'value': {'foo': 'bar'}},
            },
            volume['document'].get(guid)['prop3'])

        self.override(time, 'time', lambda: 2)
        self.override(toolkit, 'uuid', lambda: '2')
        self.assertEqual('2', this.call(method='POST', path=['document', guid, 'prop3'], content=None))
        self.assertEqual({
            '0': {'seqno': 2, 'value': 0},
            '1': {'seqno': 3, 'value': {'foo': 'bar'}},
            '2': {'seqno': 4, 'value': None},
            },
            volume['document'].get(guid)['prop3'])

    def test_RemoveAggprops(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.INSERT)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.INSERT | ACL.REMOVE)
            def prop2(self, value):
                return value

        events = []
        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        this.localcast = lambda x: events.append(x)
        guid = this.call(method='POST', path=['document'], content={})

        agg_guid = this.call(method='POST', path=['document', guid, 'prop1'], content=2)
        del events[:]
        self.assertEqual(
                {agg_guid: {'seqno': 2, 'value': 2}},
                volume['document'].get(guid)['prop1'])
        self.assertRaises(http.Forbidden, this.call, method='DELETE', path=['document', guid, 'prop1', agg_guid])
        self.assertEqual(
                {agg_guid: {'seqno': 2, 'value': 2}},
                volume['document'].get(guid)['prop1'])
        self.assertEqual([], events)

        agg_guid = this.call(method='POST', path=['document', guid, 'prop2'], content=3)
        del events[:]
        self.assertEqual(
                {agg_guid: {'seqno': 3, 'value': 3}},
                volume['document'].get(guid)['prop2'])
        this.call(method='DELETE', path=['document', guid, 'prop2', agg_guid])
        self.assertEqual(
                {agg_guid: {'seqno': 4}},
                volume['document'].get(guid)['prop2'])
        self.assertEqual([
            {'event': 'update', 'resource': 'document', 'guid': guid, 'props': {
                'mtime': 0,
                'prop2': {agg_guid: {'seqno': 4}},
                }},
            ],
            events)

    def test_FailOnAbsentAggprops(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
            def prop(self, value):
                return value

        events = []
        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        this.localcast = lambda x: events.append(x)
        guid = this.call(method='POST', path=['document'], content={})
        del events[:]

        self.assertRaises(http.NotFound, this.call, method='DELETE', path=['document', guid, 'prop', 'absent'])
        self.assertEqual([], events)

    def test_UpdateAggprops(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property(db.Aggregated)
            def prop1(self, value):
                return value

            @db.stored_property(db.Aggregated, acl=ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
            def prop2(self, value):
                return value

        events = []
        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        this.localcast = lambda x: events.append(x)
        guid = this.call(method='POST', path=['document'], content={})

        agg_guid = this.call(method='POST', path=['document', guid, 'prop1'], content=1)
        del events[:]
        self.assertEqual(
                {agg_guid: {'seqno': 2, 'value': 1}},
                volume['document'].get(guid)['prop1'])
        self.assertRaises(http.Forbidden, this.call, method='PUT', path=['document', guid, 'prop1', agg_guid], content=2)
        self.assertEqual(
                {agg_guid: {'seqno': 2, 'value': 1}},
                volume['document'].get(guid)['prop1'])
        self.assertEqual([], events)

        agg_guid = this.call(method='POST', path=['document', guid, 'prop2'], content=2)
        del events[:]
        self.assertEqual(
                {agg_guid: {'seqno': 3, 'value': 2}},
                volume['document'].get(guid)['prop2'])
        this.call(method='PUT', path=['document', guid, 'prop2', agg_guid], content=3)
        self.assertEqual(
                {agg_guid: {'seqno': 4, 'value': 3}},
                volume['document'].get(guid)['prop2'])
        self.assertEqual([
            {'event': 'update', 'resource': 'document', 'guid': guid, 'props': {
                'mtime': 0,
                'prop2': {agg_guid: {'seqno': 4, 'value': 3}},
                }},
            ],
            events)

    def test_PostAbsentAggpropsOnUpdate(self):
        self.override(time, 'time', lambda: 0)

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
            def prop(self, value):
                return value

        events = []
        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        this.localcast = lambda x: events.append(x)
        guid = this.call(method='POST', path=['document'], content={})
        del events[:]

        this.call(method='PUT', path=['document', guid, 'prop', 'absent'], content='probe')
        self.assertEqual(
                {'absent': {'seqno': 2, 'value': 'probe'}},
                volume['document'].get(guid)['prop'])
        self.assertEqual([
            {'event': 'update', 'resource': 'document', 'guid': guid, 'props': {
                'mtime': 0,
                'prop': {'absent': {'seqno': 2, 'value': 'probe'}},
                }},
            ],
            events)

    def test_OriginalAggprops(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, acl=ACL.INSERT | ACL.REMOVE)
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [User, Document])
        router = Router(db.Routes(volume))
        volume['user'].create({'guid': 'user1', 'pubkey': '', 'name': 'User1'})
        volume['user'].create({'guid': 'user2', 'pubkey': '', 'name': 'User2'})

        guid = this.call(method='POST', path=['document'], content={}, principal=tests.UID)
        assert ACL.ORIGINAL & volume['document'][guid]['author'][tests.UID]['role']

        agg_guid1 = this.call(method='POST', path=['document', guid, 'prop'], content=1, principal=tests.UID)
        assert tests.UID2 not in volume['document'][guid]['prop'][agg_guid1]['author']
        assert ACL.ORIGINAL & volume['document'][guid]['prop'][agg_guid1]['author'][tests.UID]['role']

        agg_guid2 = this.call(method='POST', path=['document', guid, 'prop'], content=1, principal=tests.UID2)
        assert tests.UID not in volume['document'][guid]['prop'][agg_guid2]['author']
        assert not (ACL.ORIGINAL & volume['document'][guid]['prop'][agg_guid2]['author'][tests.UID2]['role'])

        this.call(method='DELETE', path=['document', guid, 'prop', agg_guid2], principal=tests.UID2)
        assert tests.UID not in volume['document'][guid]['prop'][agg_guid2]['author']
        assert not (ACL.ORIGINAL & volume['document'][guid]['prop'][agg_guid2]['author'][tests.UID2]['role'])

    def test_AggregatedBlobs(self):

        class Document(db.Resource):

            @db.stored_property(db.Aggregated, subtype=db.Blob())
            def blobs(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))
        guid = this.call(method='POST', path=['document'], content={})

        digest1 = hashlib.sha1('blob1').hexdigest()
        digest2 = hashlib.sha1('blob2').hexdigest()
        digest3 = hashlib.sha1('blob3').hexdigest()

        agg1 = this.call(method='POST', path=['document', guid, 'blobs'], content='blob1')
        self.assertEqual({
            agg1: {'seqno': 3, 'value': digest1},
            },
            volume['document'].get(guid)['blobs'])
        assert volume.blobs.get(digest1)

        agg2 = this.call(method='POST', path=['document', guid, 'blobs'], content='blob2')
        self.assertEqual({
            agg1: {'seqno': 3, 'value': digest1},
            agg2: {'seqno': 5, 'value': digest2},
            },
            volume['document'].get(guid)['blobs'])
        assert volume.blobs.get(digest2)

        this.call(method='DELETE', path=['document', guid, 'blobs', agg1])
        self.assertEqual({
            agg1: {'seqno': 7},
            agg2: {'seqno': 5, 'value': digest2},
            },
            volume['document'].get(guid)['blobs'])
        assert not volume.blobs.get(digest1).exists
        assert volume.blobs.get(digest2)

        this.call(method='DELETE', path=['document', guid, 'blobs', agg2])
        self.assertEqual({
            agg1: {'seqno': 7},
            agg2: {'seqno': 9},
            },
            volume['document'].get(guid)['blobs'])
        assert not volume.blobs.get(digest1).exists
        assert not volume.blobs.get(digest2).exists

        agg3 = this.call(method='POST', path=['document', guid, 'blobs'], content='blob3')
        self.assertEqual({
            agg1: {'seqno': 7},
            agg2: {'seqno': 9},
            agg3: {'seqno': 11, 'value': digest3},
            },
            volume['document'].get(guid)['blobs'])
        assert not volume.blobs.get(digest1).exists
        assert not volume.blobs.get(digest2).exists
        assert volume.blobs.get(digest3)

    def test_AggregatedSearch(self):

        class Document(db.Resource):

            @db.indexed_property(db.Aggregated, prefix='A', full_text=True)
            def comments(self, value):
                return value

            @db.indexed_property(prefix='B', full_text=False, default='')
            def prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))

        guid1 = this.call(method='POST', path=['document'], content={})
        this.call(method='POST', path=['document', guid1, 'comments'], content='a')
        this.call(method='POST', path=['document', guid1, 'comments'], content='b')
        this.call(method='PUT', path=['document', guid1, 'prop'], content='c')

        guid2 = this.call(method='POST', path=['document'], content={})
        this.call(method='POST', path=['document', guid2, 'comments'], content='c')
        this.call(method='POST', path=['document', guid2, 'comments'], content='a')
        this.call(method='PUT', path=['document', guid2, 'prop'], content='b')

        guid3 = this.call(method='POST', path=['document'], content={})
        this.call(method='POST', path=['document', guid3, 'comments'], content='a c')
        this.call(method='POST', path=['document', guid3, 'comments'], content='b d')
        this.call(method='PUT', path=['document', guid3, 'prop'], content='e')

        self.assertEqual(
                sorted([guid1, guid2, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='a')['result']]))
        self.assertEqual(
                sorted([guid1, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='b')['result']]))
        self.assertEqual(
                sorted([guid2, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='c')['result']]))
        self.assertEqual(
                sorted([]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='absent')['result']]))

        self.assertEqual(
                sorted([guid1, guid2, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='comments:a')['result']]))
        self.assertEqual(
                sorted([guid1, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='comments:b')['result']]))
        self.assertEqual(
                sorted([guid2, guid3]),
                sorted([i['guid'] for i in this.call(method='GET', path=['document'], query='comments:c')['result']]))

    def test_HandleDeletes(self):

        class Document(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [Document])
        router = Router(db.Routes(volume))

        guid = this.call(method='POST', path=['document'], content={})
        self.assertEqual('active', volume['document'][guid]['state'])

        events = []
        this.localcast = lambda x: events.append(x)
        this.call(method='DELETE', path=['document', guid], principal=tests.UID)

        self.assertRaises(http.NotFound, this.call, method='GET', path=['document', guid])
        self.assertEqual('deleted', volume['document'][guid]['state'])
        self.assertEqual(
                [{'event': 'delete', 'resource': 'document', 'guid': guid}],
                events)


if __name__ == '__main__':
    tests.main()
