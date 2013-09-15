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
from sugar_network.db.routes import _typecast_prop_value
from sugar_network.db.metadata import Property
from sugar_network.toolkit.router import Router, Request, Response, fallbackroute, Blob, ACL
from sugar_network.toolkit import coroutine, http


class RoutesTest(tests.Test):

    def test_PostDefaults(self):

        class Document(db.Resource):

            @db.stored_property(default='default')
            def w_default(self, value):
                return value

            @db.stored_property()
            def wo_default(self, value):
                return value

            @db.indexed_property(slot=1, default='not_stored_default')
            def not_stored_default(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [Document], lambda event: None)

        self.assertRaises(RuntimeError, self.call, 'POST', ['document'], content={})

        guid = self.call('POST', ['document'], content={'wo_default': 'wo_default'})
        self.assertEqual('default', self.call('GET', ['document', guid, 'w_default']))
        self.assertEqual('wo_default', self.call('GET', ['document', guid, 'wo_default']))
        self.assertEqual('not_stored_default', self.call('GET', ['document', guid, 'not_stored_default']))

    def test_Populate(self):
        self.touch(
                ('document/1/1/guid', '{"value": "1"}'),
                ('document/1/1/ctime', '{"value": 1}'),
                ('document/1/1/mtime', '{"value": 1}'),
                ('document/1/1/seqno', '{"value": 0}'),

                ('document/2/2/guid', '{"value": "2"}'),
                ('document/2/2/ctime', '{"value": 2}'),
                ('document/2/2/mtime', '{"value": 2}'),
                ('document/2/2/seqno', '{"value": 0}'),
                )

        class Document(db.Resource):
            pass

        with db.Volume(tests.tmpdir, [Document], lambda event: None) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('document/index')

        class Document(db.Resource):
            pass

        with db.Volume(tests.tmpdir, [Document], lambda event: None) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

    def test_Commands(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        self.volume['testdocument'].create({'guid': 'guid'})

        self.assertEqual({
            'total': 1,
            'result': [
                {'guid': 'guid', 'prop': ''},
                ],
            },
            self.call('GET', path=['testdocument'], reply=['guid', 'prop']))

        guid_1 = self.call('POST', path=['testdocument'], content={'prop': 'value_1'})
        assert guid_1
        guid_2 = self.call('POST', path=['testdocument'], content={'prop': 'value_2'})
        assert guid_2

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_1'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        self.call('PUT', path=['testdocument', guid_1], content={'prop': 'value_3'})

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        self.call('DELETE', path=['testdocument', guid_2])

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    ]),
                sorted(self.call('GET', path=['testdocument'], reply=['guid', 'prop'])['result']))

        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid_2])

        self.assertEqual(
                {'guid': guid_1, 'prop': 'value_3'},
                self.call('GET', path=['testdocument', guid_1], reply=['guid', 'prop']))

        self.assertEqual(
                'value_3',
                self.call('GET', path=['testdocument', guid_1, 'prop']))

    def test_SetBLOBs(self):

        class TestDocument(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.call('PUT', path=['testdocument', guid, 'blob'], content='blob1')
        self.assertEqual('blob1', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

        self.call('PUT', path=['testdocument', guid, 'blob'], content_stream=StringIO('blob2'))
        self.assertEqual('blob2', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

        self.call('PUT', path=['testdocument', guid, 'blob'], content=None)
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob'])

    def test_SetBLOBsByMeta(self):

        class TestDocument(db.Resource):

            @db.blob_property(mime_type='default')
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.assertRaises(RuntimeError, self.call, 'PUT', path=['testdocument', guid, 'blob'],
                content={}, content_type='application/json')
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob'])

        self.call('PUT', path=['testdocument', guid, 'blob'],
                content={'url': 'foo', 'bar': 'probe'}, content_type='application/json')
        blob = self.call('GET', path=['testdocument', guid, 'blob'])
        self.assertEqual('foo', blob['url'])

    def test_RemoveBLOBs(self):

        class TestDocument(db.Resource):

            @db.blob_property(mime_type='default')
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'blob': 'blob'})

        self.assertEqual('blob', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

        self.call('PUT', path=['testdocument', guid, 'blob'])
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob'])

    def test_RemoveTempBLOBFilesOnFails(self):

        class TestDocument(db.Resource):

            @db.blob_property(mime_type='default')
            def blob(self, value):
                return value

            @blob.setter
            def blob(self, value):
                raise RuntimeError()

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.assertRaises(RuntimeError, self.call, 'PUT', path=['testdocument', guid, 'blob'], content='probe')
        self.assertEqual(0, len(os.listdir('tmp')))

    def test_SetBLOBsWithMimeType(self):

        class TestDocument(db.Resource):

            @db.blob_property(mime_type='default')
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.call('PUT', path=['testdocument', guid, 'blob'], content='blob1')
        self.assertEqual('default', self.call('GET', path=['testdocument', guid, 'blob'])['mime_type'])
        self.assertEqual('default', self.response.content_type)

        self.call('PUT', path=['testdocument', guid, 'blob'], content='blob1', content_type='foo')
        self.assertEqual('foo', self.call('GET', path=['testdocument', guid, 'blob'])['mime_type'])
        self.assertEqual('foo', self.response.content_type)

    def test_GetBLOBs(self):

        class TestDocument(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})
        self.call('PUT', path=['testdocument', guid, 'blob'], content='blob')

        blob_path = tests.tmpdir + '/testdocument/%s/%s/blob' % (guid[:2], guid)
        blob_meta = {
                'seqno': 2,
                'blob': blob_path + '.blob',
                'blob_size': 4,
                'digest': hashlib.sha1('blob').hexdigest(),
                'mime_type': 'application/octet-stream',
                'mtime': int(os.stat(blob_path).st_mtime),
                }

        self.assertEqual('blob', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

        self.assertEqual(
                {'guid': guid, 'blob': 'http://localhost/testdocument/%s/blob' % guid},
                self.call('GET', path=['testdocument', guid], reply=['guid', 'blob'], host='localhost'))

        self.assertEqual([
            {'guid': guid, 'blob': 'http://localhost/testdocument/%s/blob' % guid},
            ],
            self.call('GET', path=['testdocument'], reply=['guid', 'blob'], host='localhost')['result'])

    def test_GetBLOBsByUrls(self):

        class TestDocument(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob'])
        self.assertEqual(
                {'blob': 'http://127.0.0.1/testdocument/%s/blob' % guid},
                self.call('GET', path=['testdocument', guid], reply=['blob'], host='127.0.0.1'))
        self.assertEqual([
            {'blob': 'http://127.0.0.1/testdocument/%s/blob' % guid},
            ],
            self.call('GET', path=['testdocument'], reply=['blob'], host='127.0.0.1')['result'])

        self.call('PUT', path=['testdocument', guid, 'blob'], content='file')
        self.assertEqual('file', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())
        self.assertEqual(
                {'blob': 'http://127.0.0.1/testdocument/%s/blob' % guid},
                self.call('GET', path=['testdocument', guid], reply=['blob'], host='127.0.0.1'))
        self.assertEqual([
            {'blob': 'http://127.0.0.1/testdocument/%s/blob' % guid},
            ],
            self.call('GET', path=['testdocument'], reply=['blob'], host='127.0.0.1')['result'])

        self.call('PUT', path=['testdocument', guid, 'blob'], content={'url': 'http://foo'},
                content_type='application/json')
        self.assertEqual('http://foo', self.call('GET', path=['testdocument', guid, 'blob'])['url'])
        self.assertEqual(
                {'blob': 'http://foo'},
                self.call('GET', path=['testdocument', guid], reply=['blob'], host='127.0.0.1'))
        self.assertEqual([
            {'blob': 'http://foo'},
            ],
            self.call('GET', path=['testdocument'], reply=['blob'], host='127.0.0.1')['result'])

    def test_CommandsGetAbsentBlobs(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', path=['testdocument'], content={'prop': 'value'})
        self.assertEqual('value', self.call('GET', path=['testdocument', guid, 'prop']))
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob'])
        self.assertEqual(
                {'blob': 'http://localhost/testdocument/%s/blob' % guid},
                self.call('GET', path=['testdocument', guid], reply=['blob'], host='localhost'))

    def test_Command_ReplyForGET(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'prop': 'value'})

        self.assertEqual(
                ['guid', 'prop'],
                self.call('GET', path=['testdocument', guid], reply=['guid', 'prop']).keys())

        self.assertEqual(
                ['guid'],
                self.call('GET', path=['testdocument'])['result'][0].keys())

        self.assertEqual(
                sorted(['guid', 'prop']),
                sorted(self.call('GET', path=['testdocument'], reply=['prop', 'guid'])['result'][0].keys()))

        self.assertEqual(
                sorted(['prop']),
                sorted(self.call('GET', path=['testdocument'], reply=['prop'])['result'][0].keys()))

    def test_DecodeBeforeSetting(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, typecast=int)
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', path=['testdocument'], content={'prop': '-1'})
        self.assertEqual(-1, self.call('GET', path=['testdocument', guid, 'prop']))

    def test_LocalizedSet(self):
        toolkit._default_lang = 'en'

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        directory = self.volume['testdocument']

        guid = directory.create({'localized_prop': 'value_raw'})
        self.assertEqual({'en': 'value_raw'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_raw')[0]])

        directory.update(guid, {'localized_prop': 'value_raw2'})
        self.assertEqual({'en': 'value_raw2'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_raw2')[0]])

        guid = self.call('POST', path=['testdocument'], accept_language=['ru'], content={'localized_prop': 'value_ru'})
        self.assertEqual({'ru': 'value_ru'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(localized_prop='value_ru')[0]])

        self.call('PUT', path=['testdocument', guid], accept_language=['en'], content={'localized_prop': 'value_en'})
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

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        directory = self.volume['testdocument']

        guid = self.call('POST', path=['testdocument'], content={
            'localized_prop': {
                'ru': 'value_ru',
                'es': 'value_es',
                'en': 'value_en',
                },
            })

        toolkit._default_lang = 'en'

        self.assertEqual(
                {'localized_prop': 'value_en'},
                self.call('GET', path=['testdocument', guid], reply=['localized_prop']))
        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', path=['testdocument', guid], accept_language=['ru'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', path=['testdocument', guid, 'localized_prop'], accept_language=['ru', 'es']))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                self.call('GET', path=['testdocument'], accept_language=['foo', 'ru', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', path=['testdocument', guid], accept_language=['ru-RU'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', path=['testdocument', guid, 'localized_prop'], accept_language=['ru-RU', 'es']))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                self.call('GET', path=['testdocument'], accept_language=['foo', 'ru-RU', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_es'},
                self.call('GET', path=['testdocument', guid], accept_language=['es'], reply=['localized_prop']))
        self.assertEqual(
                'value_es',
                self.call('GET', path=['testdocument', guid, 'localized_prop'], accept_language=['es', 'ru']))
        self.assertEqual(
                [{'localized_prop': 'value_es'}],
                self.call('GET', path=['testdocument'], accept_language=['foo', 'es', 'ru'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_en'},
                self.call('GET', path=['testdocument', guid], accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_en',
                self.call('GET', path=['testdocument', guid, 'localized_prop'], accept_language=['fr', 'za']))
        self.assertEqual(
                [{'localized_prop': 'value_en'}],
                self.call('GET', path=['testdocument'], accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

        toolkit._default_lang = 'foo'
        fallback_lang = sorted(['ru', 'es', 'en'])[0]

        self.assertEqual(
                {'localized_prop': 'value_%s' % fallback_lang},
                self.call('GET', path=['testdocument', guid], accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_%s' % fallback_lang,
                self.call('GET', path=['testdocument', guid, 'localized_prop'], accept_language=['fr', 'za']))
        self.assertEqual(
                [{'localized_prop': 'value_%s' % fallback_lang}],
                self.call('GET', path=['testdocument'], accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

    def test_OpenByModuleName(self):
        self.touch(
                ('foo/bar.py', [
                    'from sugar_network import db',
                    'class Bar(db.Resource): pass',
                    ]),
                ('foo/__init__.py', ''),
                )
        sys.path.insert(0, '.')

        volume = db.Volume('.', ['foo.bar'], lambda event: None)
        assert exists('bar/index')
        volume['bar'].find()
        volume.close()

    def test_Command_GetBlobSetByUrl(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.blob_property()
            def blob(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})
        self.call('PUT', path=['testdocument', guid, 'blob'], url='http://sugarlabs.org')

        self.assertEqual(
                'http://sugarlabs.org',
                self.call('GET', path=['testdocument', guid, 'blob'])['url'])

    def test_on_create(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        ts = int(time.time())
        guid = self.call('POST', path=['testdocument'], content={})
        assert self.volume['testdocument'].get(guid)['ctime'] in range(ts - 1, ts + 1)
        assert self.volume['testdocument'].get(guid)['mtime'] in range(ts - 1, ts + 1)

    def test_on_create_Override(self):

        class Routes(db.Routes):

            def on_create(self, request, props, event):
                props['prop'] = 'overriden'
                db.Routes.on_create(self, request, props, event)

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', ['testdocument'], content={'prop': 'foo'}, routes=Routes)
        self.assertEqual('overriden', self.volume['testdocument'].get(guid)['prop'])

        self.call('PUT', ['testdocument', guid], content={'prop': 'bar'}, routes=Routes)
        self.assertEqual('bar', self.volume['testdocument'].get(guid)['prop'])

    def test_on_update(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})
        prev_mtime = self.volume['testdocument'].get(guid)['mtime']

        time.sleep(1)

        self.call('PUT', path=['testdocument', guid], content={'prop': 'probe'})
        assert self.volume['testdocument'].get(guid)['mtime'] - prev_mtime >= 1

    def test_on_update_Override(self):

        class Routes(db.Routes):

            def on_update(self, request, props, event):
                props['prop'] = 'overriden'
                db.Routes.on_update(self, request, props, event)

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', ['testdocument'], content={'prop': 'foo'}, routes=Routes)
        self.assertEqual('foo', self.volume['testdocument'].get(guid)['prop'])

        self.call('PUT', ['testdocument', guid], content={'prop': 'bar'}, routes=Routes)
        self.assertEqual('overriden', self.volume['testdocument'].get(guid)['prop'])

    def __test_DoNotPassGuidsForCreate(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        self.assertRaises(http.Forbidden, self.call, 'POST', path=['testdocument'], content={'guid': 'foo'})
        guid = self.call('POST', path=['testdocument'], content={})
        assert guid

    def test_seqno(self):

        class Document1(db.Resource):
            pass

        class Document2(db.Resource):
            pass

        volume = db.Volume(tests.tmpdir, [Document1, Document2], lambda event: None)

        assert not exists('seqno')
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
        assert not exists('seqno')
        volume.seqno.commit()
        assert exists('seqno')
        volume = db.Volume(tests.tmpdir, [Document1, Document2], lambda event: None)
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

            @db.blob_property()
            def blob(self, value):
                return value

        self.touch(
                ('document1/1/1/guid', '{"value": "1"}'),
                ('document1/1/1/ctime', '{"value": 1}'),
                ('document1/1/1/mtime', '{"value": 1}'),
                ('document1/1/1/prop', '{"value": ""}'),
                ('document1/1/1/seqno', '{"value": 0}'),
                )

        events = []
        volume = db.Volume(tests.tmpdir, [Document1, Document2], lambda event: events.append(event))
        coroutine.sleep(.1)

        mtime = int(os.stat('document1/index/mtime').st_mtime)
        self.assertEqual([
            {'event': 'commit', 'resource': 'document1', 'mtime': mtime},
            {'event': 'populate', 'resource': 'document1', 'mtime': mtime},
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
            {'event': 'update', 'resource': 'document1', 'guid': 'guid1'},
            {'event': 'update', 'resource': 'document2', 'guid': 'guid2'},
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
        mtime1 = int(os.stat('document1/index/mtime').st_mtime)
        volume['document2'].commit()
        mtime2 = int(os.stat('document2/index/mtime').st_mtime)

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

            @db.blob_property(acl=ACL.READ)
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.assertRaises(http.Forbidden, self.call, 'POST', path=['testdocument'], content={'prop': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid], content={'prop': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid], content={'blob': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid, 'prop'], content='value')
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid, 'blob'], content='value')

    def test_BlobsWritePermissions(self):

        class TestDocument(db.Resource):

            @db.blob_property(acl=ACL.CREATE | ACL.WRITE)
            def blob1(self, value):
                return value

            @db.blob_property(acl=ACL.CREATE)
            def blob2(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', path=['testdocument'], content={})
        self.call('PUT', path=['testdocument', guid], content={'blob1': 'value1', 'blob2': 'value2'})
        self.call('PUT', path=['testdocument', guid], content={'blob1': 'value1'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid], content={'blob2': 'value2_'})

        guid = self.call('POST', path=['testdocument'], content={})
        self.call('PUT', path=['testdocument', guid, 'blob1'], content='value1')
        self.call('PUT', path=['testdocument', guid, 'blob2'], content='value2')
        self.call('PUT', path=['testdocument', guid, 'blob1'], content='value1_')
        self.assertRaises(http.Forbidden, self.call, 'PUT', path=['testdocument', guid, 'blob2'], content='value2_')

    def test_properties_OverrideGet(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='1')
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2, default='2')
            def prop2(self, value):
                return -1

            @db.blob_property()
            def blob(self, meta):
                meta['blob'] = 'new-blob'
                return meta

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})
        self.touch(('new-blob', 'new-blob'))
        self.call('PUT', path=['testdocument', guid, 'blob'], content='old-blob')

        self.assertEqual(
                'new-blob',
                self.call('GET', path=['testdocument', guid, 'blob'])['blob'])
        self.assertEqual(
                '1',
                self.call('GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(
                -1,
                self.call('GET', path=['testdocument', guid, 'prop2']))
        self.assertEqual(
                {'prop1': '1', 'prop2': -1},
                self.call('GET', path=['testdocument', guid], reply=['prop1', 'prop2']))

    def test_properties_OverrideSet(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='1')
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return '_%s' % value

            @db.blob_property()
            def blob1(self, meta):
                return meta

            @blob1.setter
            def blob1(self, value):
                return Blob({'url': file(value['blob']).read()})

            @db.blob_property()
            def blob2(self, meta):
                return meta

            @blob2.setter
            def blob2(self, value):
                with toolkit.NamedTemporaryFile(delete=False) as f:
                    f.write(' %s ' % file(value['blob']).read())
                value['blob'] = f.name
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={})

        self.assertEqual('_1', self.call('GET', path=['testdocument', guid, 'prop']))
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob1'])

        self.call('PUT', path=['testdocument', guid, 'prop'], content='2')
        self.assertEqual('_2', self.call('GET', path=['testdocument', guid, 'prop']))
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob1'])

        self.call('PUT', path=['testdocument', guid], content={'prop': 3})
        self.assertEqual('_3', self.call('GET', path=['testdocument', guid, 'prop']))
        self.assertRaises(http.NotFound, self.call, 'GET', path=['testdocument', guid, 'blob1'])

        self.call('PUT', path=['testdocument', guid, 'blob1'], content='blob_url')
        self.assertEqual('blob_url', self.call('GET', path=['testdocument', guid, 'blob1'])['url'])

        guid = self.call('POST', path=['testdocument'], content={'blob2': 'foo'})
        self.assertEqual(' foo ', file(self.call('GET', path=['testdocument', guid, 'blob2'])['blob']).read())

        self.call('PUT', path=['testdocument', guid, 'blob2'], content='bar')
        self.assertEqual(' bar ', file(self.call('GET', path=['testdocument', guid, 'blob2'])['blob']).read())

    def test_properties_CallSettersAtTheEnd(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, typecast=int)
            def prop1(self, value):
                return value

            @prop1.setter
            def prop1(self, value):
                return self['prop3'] + value

            @db.indexed_property(slot=2, typecast=int)
            def prop2(self, value):
                return value

            @prop2.setter
            def prop2(self, value):
                return self['prop3'] - value

            @db.indexed_property(slot=3, typecast=int)
            def prop3(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'prop1': 1, 'prop2': 2, 'prop3': 3})
        self.assertEqual(4, self.call('GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(1, self.call('GET', path=['testdocument', guid, 'prop2']))

    def test_properties_PopulateRequiredPropsInSetters(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, typecast=int)
            def prop1(self, value):
                return value

            @prop1.setter
            def prop1(self, value):
                self['prop2'] = value + 1
                return value

            @db.indexed_property(slot=2, typecast=int)
            def prop2(self, value):
                return value

            @db.blob_property()
            def prop3(self, value):
                return value

            @prop3.setter
            def prop3(self, value):
                self['prop1'] = -1
                self['prop2'] = -2
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'prop1': 1})
        self.assertEqual(1, self.call('GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(2, self.call('GET', path=['testdocument', guid, 'prop2']))

    def test_properties_PopulateRequiredPropsInBlobSetter(self):

        class TestDocument(db.Resource):

            @db.blob_property()
            def blob(self, value):
                return value

            @blob.setter
            def blob(self, value):
                self['prop1'] = 1
                self['prop2'] = 2
                return value

            @db.indexed_property(slot=1, typecast=int)
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2, typecast=int)
            def prop2(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'blob': ''})
        self.assertEqual(1, self.call('GET', path=['testdocument', guid, 'prop1']))
        self.assertEqual(2, self.call('GET', path=['testdocument', guid, 'prop2']))

    def __test_SubCall(self):

        class TestDocument(db.Resource):

            @db.blob_property(mime_type='application/json')
            def blob(self, value):
                return value

            @blob.setter
            def blob(self, value):
                blob = file(value['blob']).read()
                if '!' not in blob:
                    meta = self.meta('blob')
                    if meta:
                        blob = file(meta['blob']).read() + blob
                        with toolkit.NamedTemporaryFile(delete=False) as f:
                            f.write(blob)
                        value['blob'] = f.name
                    coroutine.spawn(self.post, blob)
                return value

            def post(self, value):
                self.request.call('PUT', path=['testdocument', self.guid, 'blob'], content=value + '!')

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        guid = self.call('POST', path=['testdocument'], content={'blob': '0'})
        coroutine.dispatch()
        self.assertEqual('0!', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

        self.call('PUT', path=['testdocument', guid, 'blob'], content='1')
        coroutine.dispatch()
        self.assertEqual('0!1!', file(self.call('GET', path=['testdocument', guid, 'blob'])['blob']).read())

    def test_Group(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        self.call('POST', path=['testdocument'], content={'prop': 1})
        self.call('POST', path=['testdocument'], content={'prop': 2})
        self.call('POST', path=['testdocument'], content={'prop': 1})

        self.assertEqual(
                sorted([{'prop': 1}, {'prop': 2}]),
                sorted(self.call('GET', path=['testdocument'], reply='prop', group_by='prop')['result']))

    def test_CallSetterEvenIfThereIsNoCreatePermissions(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, acl=ACL.READ, default=0)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + 1

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        self.assertRaises(http.Forbidden, self.call, 'POST', path=['testdocument'], content={'prop': 1})

        guid = self.call('POST', path=['testdocument'], content={})
        self.assertEqual(1, self.call('GET', path=['testdocument', guid, 'prop']))

    def test_ReturnDefualtsForMissedProps(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='default')
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', path=['testdocument'], content={'prop': 'set'})

        self.assertEqual(
                [{'prop': 'set'}],
                self.call('GET', path=['testdocument'], reply='prop')['result'])
        self.assertEqual(
                {'prop': 'set'},
                self.call('GET', path=['testdocument', guid], reply='prop'))
        self.assertEqual(
                'set',
                self.call('GET', path=['testdocument', guid, 'prop']))

        os.unlink('testdocument/%s/%s/prop' % (guid[:2], guid))

        self.assertEqual(
                [{'prop': 'default'}],
                self.call('GET', path=['testdocument'], reply='prop')['result'])
        self.assertEqual(
                {'prop': 'default'},
                self.call('GET', path=['testdocument', guid], reply='prop'))
        self.assertEqual(
                'default',
                self.call('GET', path=['testdocument', guid, 'prop']))

    def test_PopulateNonDefualtPropsInSetters(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1)
            def prop1(self, value):
                return value

            @db.indexed_property(slot=2, default='default')
            def prop2(self, value):
                return all

            @prop2.setter
            def prop2(self, value):
                if value != 'default':
                    self['prop1'] = value
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)

        self.assertRaises(RuntimeError, self.call, 'POST', path=['testdocument'], content={})

        guid = self.call('POST', path=['testdocument'], content={'prop2': 'value2'})
        self.assertEqual('value2', self.call('GET', path=['testdocument', guid, 'prop1']))

    def test_prop_meta(self):

        class TestDocument(db.Resource):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.blob_property()
            def blob1(self, value):
                return value

            @db.blob_property()
            def blob2(self, value):
                return value

            @blob2.setter
            def blob2(self, value):
                return {'url': 'http://new', 'foo': 'bar', 'blob_size': 100}

        self.volume = db.Volume(tests.tmpdir, [TestDocument], lambda event: None)
        guid = self.call('POST', ['testdocument'], content = {'prop': 'prop', 'blob1': 'blob', 'blob2': ''})

        assert self.call('HEAD', ['testdocument', guid, 'prop']) is None
        meta = self.volume['testdocument'].get(guid).meta('prop')
        meta.pop('value')
        self.assertEqual(meta, self.response.meta)
        self.assertEqual(formatdate(meta['mtime'], localtime=False, usegmt=True), self.response.last_modified)

        assert self.call('HEAD', ['testdocument', guid, 'blob1'], host='localhost') is None
        meta = self.volume['testdocument'].get(guid).meta('blob1')
        meta.pop('blob')
        self.assertEqual(meta, self.response.meta)
        self.assertEqual(len('blob'), self.response.content_length)
        self.assertEqual(formatdate(meta['mtime'], localtime=False, usegmt=True), self.response.last_modified)

        assert self.call('HEAD', ['testdocument', guid, 'blob2']) is None
        meta = self.volume['testdocument'].get(guid).meta('blob2')
        self.assertEqual(meta, self.response.meta)
        self.assertEqual(100, self.response.content_length)
        self.assertEqual(formatdate(meta['mtime'], localtime=False, usegmt=True), self.response.last_modified)

        assert self.call('GET', ['testdocument', guid, 'blob2']) is not None
        meta = self.volume['testdocument'].get(guid).meta('blob2')
        self.assertEqual(meta, self.response.meta)
        self.assertEqual(formatdate(meta['mtime'], localtime=False, usegmt=True), self.response.last_modified)

    def test_DefaultAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        guid = self.call('POST', ['document'], content={}, principal='user')
        self.assertEqual(
                [{'name': 'user', 'role': 2}],
                self.call('GET', ['document', guid, 'author']))
        self.assertEqual(
                {'user': {'role': 2, 'order': 0}},
                self.volume['document'].get(guid)['author'])

        self.volume['user'].create({'guid': 'user', 'color': '', 'pubkey': '', 'name': 'User'})

        guid = self.call('POST', ['document'], content={}, principal='user')
        self.assertEqual(
                [{'guid': 'user', 'name': 'User', 'role': 3}],
                self.call('GET', ['document', guid, 'author']))
        self.assertEqual(
                {'user': {'name': 'User', 'role': 3, 'order': 0}},
                self.volume['document'].get(guid)['author'])

    def test_FindByAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        self.volume['user'].create({'guid': 'user1', 'color': '', 'pubkey': '', 'name': 'UserName1'})
        self.volume['user'].create({'guid': 'user2', 'color': '', 'pubkey': '', 'name': 'User Name2'})
        self.volume['user'].create({'guid': 'user3', 'color': '', 'pubkey': '', 'name': 'User Name 3'})

        guid1 = self.call('POST', ['document'], content={}, principal='user1')
        guid2 = self.call('POST', ['document'], content={}, principal='user2')
        guid3 = self.call('POST', ['document'], content={}, principal='user3')

        self.assertEqual(sorted([
            {'guid': guid1},
            ]),
            self.call('GET', ['document'], author='UserName1')['result'])

        self.assertEqual(sorted([
            {'guid': guid1},
            ]),
            sorted(self.call('GET', ['document'], query='author:UserName')['result']))
        self.assertEqual(sorted([
            {'guid': guid1},
            {'guid': guid2},
            {'guid': guid3},
            ]),
            sorted(self.call('GET', ['document'], query='author:User')['result']))
        self.assertEqual(sorted([
            {'guid': guid2},
            {'guid': guid3},
            ]),
            sorted(self.call('GET', ['document'], query='author:Name')['result']))

    def test_PreserveAuthorsOrder(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        self.volume['user'].create({'guid': 'user1', 'color': '', 'pubkey': '', 'name': 'User1'})
        self.volume['user'].create({'guid': 'user2', 'color': '', 'pubkey': '', 'name': 'User2'})
        self.volume['user'].create({'guid': 'user3', 'color': '', 'pubkey': '', 'name': 'User3'})

        guid = self.call('POST', ['document'], content={}, principal='user1')
        self.call('PUT', ['document', guid], cmd='useradd', user='user2', role=0)
        self.call('PUT', ['document', guid], cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='userdel', user='user2', principal='user1')
        self.call('PUT', ['document', guid], cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='userdel', user='user2', principal='user1')
        self.call('PUT', ['document', guid], cmd='useradd', user='user2', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user3': {'name': 'User3', 'role': 1, 'order': 2},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='userdel', user='user3', principal='user1')
        self.call('PUT', ['document', guid], cmd='useradd', user='user3', role=0)

        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'guid': 'user3', 'name': 'User3', 'role': 1},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 3},
            'user3': {'name': 'User3', 'role': 1, 'order': 4},
            },
            self.volume['document'].get(guid)['author'])

    def test_CopyAthors(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])
        self.volume['user'].create({'guid': 'user', 'color': '', 'pubkey': '', 'name': 'User'})

        guid1 = self.call('POST', ['document'], content={}, principal='user')
        self.assertEqual({'user': {'name': 'User', 'role': 3, 'order': 0}}, self.volume['document'].get(guid1)['author'])
        author = self.call('GET', ['document', guid1, 'author'])
        self.assertEqual([{'guid': 'user', 'role': 3, 'name': 'User'}], author)

        guid2 = self.volume['document'].create({'author': author}, setters=True)
        author = self.call('GET', ['document', guid1, 'author'])
        self.assertEqual({'user': {'name': 'User', 'role': 3, 'order': 0}}, self.volume['document'].get(guid2)['author'])

    def test_AddUser(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        self.volume['user'].create({'guid': 'user1', 'color': '', 'pubkey': '', 'name': 'User1'})
        self.volume['user'].create({'guid': 'user2', 'color': '', 'pubkey': '', 'name': 'User2'})

        guid = self.call('POST', ['document'], content={}, principal='user1')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='useradd', user='user2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='useradd', user='User3', role=3)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='useradd', user='User4', role=4)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 3},
            {'name': 'User3', 'role': 2},
            {'name': 'User4', 'role': 0},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 3, 'order': 1},
            'User3': {'role': 2, 'order': 2},
            'User4': {'role': 0, 'order': 3},
            },
            self.volume['document'].get(guid)['author'])

    def test_UpdateAuthor(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        self.volume['user'].create({'guid': 'user1', 'color': '', 'pubkey': '', 'name': 'User1'})
        guid = self.call('POST', ['document'], content={}, principal='user1')

        self.call('PUT', ['document', guid], cmd='useradd', user='User2', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'name': 'User2', 'role': 0},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='useradd', user='user1', role=0)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 0},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 0, 'order': 1},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='useradd', user='User2', role=2)
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 1},
            {'name': 'User2', 'role': 2},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 1, 'order': 0},
            'User2': {'role': 2, 'order': 1},
            },
            self.volume['document'].get(guid)['author'])

    def test_DelUser(self):

        class User(db.Resource):

            @db.indexed_property(slot=1)
            def name(self, value):
                return value

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [User, Document])

        self.volume['user'].create({'guid': 'user1', 'color': '', 'pubkey': '', 'name': 'User1'})
        self.volume['user'].create({'guid': 'user2', 'color': '', 'pubkey': '', 'name': 'User2'})
        guid = self.call('POST', ['document'], content={}, principal='user1')
        self.call('PUT', ['document', guid], cmd='useradd', user='user2')
        self.call('PUT', ['document', guid], cmd='useradd', user='User3')
        self.assertEqual([
            {'guid': 'user1', 'name': 'User1', 'role': 3},
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user1': {'name': 'User1', 'role': 3, 'order': 0},
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            self.volume['document'].get(guid)['author'])

        # Do not remove yourself
        self.assertRaises(RuntimeError, self.call, 'PUT', ['document', guid], cmd='userdel', user='user1', principal='user1')
        self.assertRaises(RuntimeError, self.call, 'PUT', ['document', guid], cmd='userdel', user='user2', principal='user2')

        self.call('PUT', ['document', guid], cmd='userdel', user='user1', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            {'name': 'User3', 'role': 0},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            'User3': {'role': 0, 'order': 2},
            },
            self.volume['document'].get(guid)['author'])

        self.call('PUT', ['document', guid], cmd='userdel', user='User3', principal='user2')
        self.assertEqual([
            {'guid': 'user2', 'name': 'User2', 'role': 1},
            ],
            self.call('GET', ['document', guid, 'author']))
        self.assertEqual({
            'user2': {'name': 'User2', 'role': 1, 'order': 1},
            },
            self.volume['document'].get(guid)['author'])

    def test_typecast_prop_value(self):
        prop = Property('prop', typecast=int)
        self.assertEqual(1, _typecast_prop_value(prop.typecast, 1))
        self.assertEqual(1, _typecast_prop_value(prop.typecast, 1.1))
        self.assertEqual(1, _typecast_prop_value(prop.typecast, '1'))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '1.0')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)

        prop = Property('prop', typecast=float)
        self.assertEqual(1.0, _typecast_prop_value(prop.typecast, 1))
        self.assertEqual(1.1, _typecast_prop_value(prop.typecast, 1.1))
        self.assertEqual(1.0, _typecast_prop_value(prop.typecast, '1'))
        self.assertEqual(1.1, _typecast_prop_value(prop.typecast, '1.1'))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)

        prop = Property('prop', typecast=bool)
        self.assertEqual(False, _typecast_prop_value(prop.typecast, 0))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, 1))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, 1.1))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, '1'))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, 'false'))
        self.assertEqual(False, _typecast_prop_value(prop.typecast, ''))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)

        prop = Property('prop', typecast=[int])
        self.assertEqual((1,), _typecast_prop_value(prop.typecast, 1))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '')
        self.assertEqual((), _typecast_prop_value(prop.typecast, []))
        self.assertEqual((123,), _typecast_prop_value(prop.typecast, '123'))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, 'a')
        self.assertEqual((123, 4, 5), _typecast_prop_value(prop.typecast, ['123', 4, 5.6]))

        prop = Property('prop', typecast=[1, 2])
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, 0)
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, 'A')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '3')
        self.assertEqual(1, _typecast_prop_value(prop.typecast, 1))
        self.assertEqual(2, _typecast_prop_value(prop.typecast, 2))
        self.assertEqual(1, _typecast_prop_value(prop.typecast, '1'))

        prop = Property('prop', typecast=[str])
        self.assertEqual(('',), _typecast_prop_value(prop.typecast, ''))
        self.assertEqual(('',), _typecast_prop_value(prop.typecast, ['']))
        self.assertEqual((), _typecast_prop_value(prop.typecast, []))

        prop = Property('prop', typecast=[])
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, None)
        self.assertEqual(('',), _typecast_prop_value(prop.typecast, ''))
        self.assertEqual(('',), _typecast_prop_value(prop.typecast, ['']))
        self.assertEqual((), _typecast_prop_value(prop.typecast, []))
        self.assertEqual(('0',), _typecast_prop_value(prop.typecast, 0))
        self.assertEqual(('',), _typecast_prop_value(prop.typecast, ''))
        self.assertEqual(('foo',), _typecast_prop_value(prop.typecast, 'foo'))

        prop = Property('prop', typecast=[['A', 'B', 'C']])
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, '')
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, [''])
        self.assertEqual((), _typecast_prop_value(prop.typecast, []))
        self.assertEqual(('A', 'B', 'C'), _typecast_prop_value(prop.typecast, ['A', 'B', 'C']))
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, ['a'])
        self.assertRaises(ValueError, _typecast_prop_value, prop.typecast, ['A', 'x'])

        prop = Property('prop', typecast=bool)
        self.assertEqual(True, _typecast_prop_value(prop.typecast, True))
        self.assertEqual(False, _typecast_prop_value(prop.typecast, False))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, 'False'))
        self.assertEqual(True, _typecast_prop_value(prop.typecast, '0'))

        prop = Property('prop', typecast=[['A', 'B', 'C']])
        self.assertEqual(('A', 'B', 'C'), _typecast_prop_value(prop.typecast, ['A', 'B', 'C']))

        prop = Property('prop', typecast=lambda x: x + 1)
        self.assertEqual(1, _typecast_prop_value(prop.typecast, 0))

    def test_DefaultOrder(self):

        class Document(db.Resource):
            pass

        self.volume = db.Volume('db', [Document])

        self.volume['document'].create({'guid': '3', 'ctime': 3})
        self.volume['document'].create({'guid': '2', 'ctime': 2})
        self.volume['document'].create({'guid': '1', 'ctime': 1})

        self.assertEqual([
            {'guid': '1'},
            {'guid': '2'},
            {'guid': '3'},
            ],
            self.call('GET', ['document'])['result'])

    def test_SetDefaultPropsOnNoneValues(self):

        class Document(db.Resource):

            @db.indexed_property(slot=1, default='default')
            def prop(self, value):
                return value

        self.volume = db.Volume('db', [Document])

        guid = self.call('POST', ['document'], content={'prop': None})
        self.assertEqual('default', self.volume['document'].get(guid).meta('prop')['value'])

    def call(self, method=None, path=None,
            accept_language=None, content=None, content_stream=None, cmd=None,
            content_type=None, host=None, request=None, routes=db.Routes, principal=None,
            **kwargs):
        if request is None:
            environ = {
                    'REQUEST_METHOD': method,
                    'PATH_INFO': '/'.join([''] + path),
                    'HTTP_ACCEPT_LANGUAGE': ','.join(accept_language or []),
                    'HTTP_HOST': host,
                    'wsgi.input': content_stream,
                    }
            if content_type:
                environ['CONTENT_TYPE'] = content_type
            if content_stream is not None:
                environ['CONTENT_LENGTH'] = str(len(content_stream.getvalue()))
            request = Request(environ, cmd=cmd, content=content)
            request.update(kwargs)
        request.principal = principal
        router = Router(routes(self.volume))
        self.response = Response()
        return router._call_route(request, self.response)


if __name__ == '__main__':
    tests.main()
