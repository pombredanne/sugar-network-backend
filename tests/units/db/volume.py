#!/usr/bin/env python
# sugar-lint: disable

import os
import sys
import time
import shutil
import hashlib
from cStringIO import StringIO
from email.message import Message
from os.path import dirname, join, abspath, exists

src_root = abspath(dirname(__file__))

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.db import env
from sugar_network.db.volume import VolumeCommands
from sugar_network.toolkit import coroutine, http


class VolumeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        self.response = db.Response()

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

        class Document(db.Document):
            pass

        with db.Volume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('document/index')

        class Document(db.Document):
            pass

        with db.Volume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

    def test_Commands(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        self.volume['testdocument'].create(guid='guid')

        self.assertEqual({
            'total': 1,
            'result': [
                {'guid': 'guid', 'prop': ''},
                ],
            },
            self.call('GET', document='testdocument', reply=['guid', 'prop']))

        guid_1 = self.call('POST', document='testdocument', content={'prop': 'value_1'})
        assert guid_1
        guid_2 = self.call('POST', document='testdocument', content={'prop': 'value_2'})
        assert guid_2

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_1'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', document='testdocument', reply=['guid', 'prop'])['result']))

        self.call('PUT', document='testdocument', guid=guid_1, content={'prop': 'value_3'})

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', document='testdocument', reply=['guid', 'prop'])['result']))

        self.call('DELETE', document='testdocument', guid=guid_2)

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    ]),
                sorted(self.call('GET', document='testdocument', reply=['guid', 'prop'])['result']))

        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid_2)

        self.assertEqual(
                {'guid': guid_1, 'prop': 'value_3'},
                self.call('GET', document='testdocument', guid=guid_1, reply=['guid', 'prop']))

        self.assertEqual(
                'value_3',
                self.call('GET', document='testdocument', guid=guid_1, prop='prop'))

    def test_SetBLOBs(self):

        class TestDocument(db.Document):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})

        self.assertRaises(RuntimeError, self.call, 'PUT', document='testdocument', guid=guid, prop='blob', content={'path': '/'})

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='blob1')
        self.assertEqual('blob1', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content_stream=StringIO('blob2'))
        self.assertEqual('blob2', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content=None)
        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob')

    def test_SetBLOBsWithMimeType(self):

        class TestDocument(db.Document):

            @db.blob_property(mime_type='default')
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='blob1')
        self.assertEqual('default', self.call('GET', document='testdocument', guid=guid, prop='blob')['mime_type'])
        self.assertEqual('default', self.response.content_type)

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='blob1', content_type='foo')
        self.assertEqual('foo', self.call('GET', document='testdocument', guid=guid, prop='blob')['mime_type'])
        self.assertEqual('foo', self.response.content_type)

    def test_GetBLOBs(self):

        class TestDocument(db.Document):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})
        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='blob')

        blob_path = tests.tmpdir + '/testdocument/%s/%s/blob' % (guid[:2], guid)
        blob_meta = {
                'seqno': 2,
                'blob': blob_path + '.blob',
                'digest': hashlib.sha1('blob').hexdigest(),
                'mime_type': 'application/octet-stream',
                'mtime': int(os.stat(blob_path).st_mtime),
                }

        self.assertEqual('blob', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())

        self.assertEqual(
                {'guid': guid, 'blob': blob_meta},
                self.call('GET', document='testdocument', guid=guid, reply=['guid', 'blob']))

        self.assertEqual([
            {'guid': guid, 'blob': blob_meta},
            ],
            self.call('GET', document='testdocument', reply=['guid', 'blob'])['result'])

    def test_GetBLOBsByUrls(self):

        class TestDocument(db.Document):

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})

        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob')
        self.assertEqual(
                {'blob': 'http://localhost/testdocument/%s/blob' % guid},
                self.call('GET', document='testdocument', guid=guid, reply=['blob'], static_prefix='http://localhost'))
        self.assertEqual([
            {'blob': 'http://localhost/testdocument/%s/blob' % guid},
            ],
            self.call('GET', document='testdocument', reply=['blob'], static_prefix='http://localhost')['result'])

        self.volume['testdocument'].set_blob(guid, 'blob', 'file')
        self.assertEqual('file', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())
        self.assertEqual(
                {'blob': 'http://localhost/testdocument/%s/blob' % guid},
                self.call('GET', document='testdocument', guid=guid, reply=['blob'], static_prefix='http://localhost'))
        self.assertEqual([
            {'blob': 'http://localhost/testdocument/%s/blob' % guid},
            ],
            self.call('GET', document='testdocument', reply=['blob'], static_prefix='http://localhost')['result'])

        self.volume['testdocument'].set_blob(guid, 'blob', db.PropertyMetadata(url='http://foo'))
        self.assertEqual('http://foo', self.call('GET', document='testdocument', guid=guid, prop='blob')['url'])
        self.assertEqual(
                {'blob': 'http://foo'},
                self.call('GET', document='testdocument', guid=guid, reply=['blob'], static_prefix='http://localhost'))
        self.assertEqual([
            {'blob': 'http://foo'},
            ],
            self.call('GET', document='testdocument', reply=['blob'], static_prefix='http://localhost')['result'])

    def test_CommandsGetAbsentBlobs(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.blob_property()
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        guid = self.call('POST', document='testdocument', content={'prop': 'value'})
        self.assertEqual('value', self.call('GET', document='testdocument', guid=guid, prop='prop'))
        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob')
        self.assertEqual(
                {'blob': db.PropertyMetadata()},
                self.call('GET', document='testdocument', guid=guid, reply=['blob']))

    def test_Command_ReplyForGET(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={'prop': 'value'})

        self.assertEqual(
                ['guid', 'prop'],
                self.call('GET', document='testdocument', guid=guid, reply=['guid', 'prop']).keys())

        self.assertEqual(
                ['guid'],
                self.call('GET', document='testdocument')['result'][0].keys())

        self.assertEqual(
                sorted(['guid', 'prop']),
                sorted(self.call('GET', document='testdocument', reply=['prop', 'guid'])['result'][0].keys()))

        self.assertEqual(
                sorted(['prop']),
                sorted(self.call('GET', document='testdocument', reply=['prop'])['result'][0].keys()))

    def test_DecodeBeforeSetting(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, typecast=int)
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        guid = self.call(method='POST', document='testdocument', content={'prop': '-1'})
        self.assertEqual(-1, self.call(method='GET', document='testdocument', guid=guid, prop='prop'))

    def test_LocalizedSet(self):
        toolkit._default_lang = 'en'

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        directory = self.volume['testdocument']

        guid = directory.create({'localized_prop': 'value_raw'})
        self.assertEqual({'en': 'value_raw'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(0, 100, localized_prop='value_raw')[0]])

        directory.update(guid, {'localized_prop': 'value_raw2'})
        self.assertEqual({'en': 'value_raw2'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(0, 100, localized_prop='value_raw2')[0]])

        guid = self.call('POST', document='testdocument', accept_language=['ru'], content={'localized_prop': 'value_ru'})
        self.assertEqual({'ru': 'value_ru'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(0, 100, localized_prop='value_ru')[0]])

        self.call('PUT', document='testdocument', guid=guid, accept_language=['en'], content={'localized_prop': 'value_en'})
        self.assertEqual({'ru': 'value_ru', 'en': 'value_en'}, directory.get(guid)['localized_prop'])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(0, 100, localized_prop='value_ru')[0]])
        self.assertEqual(
                [guid],
                [i.guid for i in directory.find(0, 100, localized_prop='value_en')[0]])

    def test_LocalizedGet(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        directory = self.volume['testdocument']

        guid = self.call('POST', document='testdocument', content={
            'localized_prop': {
                'ru': 'value_ru',
                'es': 'value_es',
                'en': 'value_en',
                },
            })

        toolkit._default_lang = 'en'

        self.assertEqual(
                {'localized_prop': 'value_en'},
                self.call('GET', document='testdocument', guid=guid, reply=['localized_prop']))
        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru', 'es'], prop='localized_prop'))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'ru', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru-RU'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru-RU', 'es'], prop='localized_prop'))
        self.assertEqual(
                [{'localized_prop': 'value_ru'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'ru-RU', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_es'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['es'], reply=['localized_prop']))
        self.assertEqual(
                'value_es',
                self.call('GET', document='testdocument', guid=guid, accept_language=['es', 'ru'], prop='localized_prop'))
        self.assertEqual(
                [{'localized_prop': 'value_es'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'es', 'ru'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_en'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_en',
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr', 'za'], prop='localized_prop'))
        self.assertEqual(
                [{'localized_prop': 'value_en'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

        toolkit._default_lang = 'foo'
        fallback_lang = sorted(['ru', 'es', 'en'])[0]

        self.assertEqual(
                {'localized_prop': 'value_%s' % fallback_lang},
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_%s' % fallback_lang,
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr', 'za'], prop='localized_prop'))
        self.assertEqual(
                [{'localized_prop': 'value_%s' % fallback_lang}],
                self.call('GET', document='testdocument', accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

    def test_LazyOpen(self):

        class Document1(db.Document):
            pass

        class Document2(db.Document):
            pass

        volume = db.Volume('.', [Document1, Document2], lazy_open=True)
        assert not exists('document1/index')
        assert not exists('document2/index')
        volume['document1'].find()
        volume['document2'].find()
        assert exists('document1/index')
        assert exists('document2/index')
        volume['document1'].find()
        volume['document2'].find()
        volume.close()

        shutil.rmtree('document1')
        shutil.rmtree('document2')

        volume = db.Volume('.', [Document1, Document2], lazy_open=False)
        assert exists('document1/index')
        assert exists('document2/index')
        volume.close()

    def test_OpenByModuleName(self):
        self.touch(
                ('foo/bar.py', [
                    'from sugar_network import db',
                    'class Bar(db.Document): pass',
                    ]),
                ('foo/__init__.py', ''),
                )
        sys.path.insert(0, '.')

        volume = db.Volume('.', ['foo.bar'])
        assert exists('bar/index')
        volume['bar'].find()
        volume.close()

    def test_Command_GetBlobSetByUrl(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.blob_property()
            def blob(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})
        self.call('PUT', document='testdocument', guid=guid, prop='blob', url='http://sugarlabs.org')

        self.assertEqual(
                'http://sugarlabs.org',
                self.call('GET', document='testdocument', guid=guid, prop='blob')['url'])

    def test_before_create(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        ts = time.time()
        guid = self.call(method='POST', document='testdocument', content={})
        assert self.volume['testdocument'].get(guid)['ctime'] in range(ts - 1, ts + 1)
        assert self.volume['testdocument'].get(guid)['mtime'] in range(ts - 1, ts + 1)

    def test_before_create_Override(self):

        class Commands(VolumeCommands):

            def before_create(self, request, props):
                props['prop'] = 'overriden'
                VolumeCommands.before_create(self, request, props)

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        cp = Commands(volume)

        request = db.Request(method='POST', document='testdocument')
        request.content = {'prop': 'foo'}
        guid = cp.call(request, db.Response())
        self.assertEqual('overriden', volume['testdocument'].get(guid)['prop'])

        request = db.Request(method='PUT', document='testdocument', guid=guid)
        request.content = {'prop': 'bar'}
        cp.call(request, db.Response())
        self.assertEqual('bar', volume['testdocument'].get(guid)['prop'])

    def test_before_update(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call(method='POST', document='testdocument', content={})
        prev_mtime = self.volume['testdocument'].get(guid)['mtime']

        time.sleep(1)

        self.call(method='PUT', document='testdocument', guid=guid, content={'prop': 'probe'})
        assert self.volume['testdocument'].get(guid)['mtime'] - prev_mtime >= 1

    def test_before_update_Override(self):

        class Commands(VolumeCommands):

            def before_update(self, request, props):
                props['prop'] = 'overriden'
                VolumeCommands.before_update(self, request, props)

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        volume = db.Volume(tests.tmpdir, [TestDocument])
        cp = Commands(volume)

        request = db.Request(method='POST', document='testdocument')
        request.content = {'prop': 'foo'}
        guid = cp.call(request, db.Response())
        self.assertEqual('foo', volume['testdocument'].get(guid)['prop'])

        request = db.Request(method='PUT', document='testdocument', guid=guid)
        request.content = {'prop': 'bar'}
        cp.call(request, db.Response())
        self.assertEqual('overriden', volume['testdocument'].get(guid)['prop'])

    def test_DoNotPassGuidsForCreate(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                return value

            @db.indexed_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        self.assertRaises(http.Forbidden, self.call, method='POST', document='testdocument', content={'guid': 'foo'})
        guid = self.call(method='POST', document='testdocument', content={})
        assert guid

    def test_seqno(self):

        class Document1(db.Document):
            pass

        class Document2(db.Document):
            pass

        volume = db.Volume(tests.tmpdir, [Document1, Document2])

        assert not exists('seqno')
        self.assertEqual(0, volume.seqno.value)

        volume['document1'].create(guid='1')
        self.assertEqual(1, volume['document1'].get('1')['seqno'])
        volume['document2'].create(guid='1')
        self.assertEqual(2, volume['document2'].get('1')['seqno'])
        volume['document1'].create(guid='2')
        self.assertEqual(3, volume['document1'].get('2')['seqno'])
        volume['document2'].create(guid='2')
        self.assertEqual(4, volume['document2'].get('2')['seqno'])

        self.assertEqual(4, volume.seqno.value)
        assert not exists('seqno')
        volume.seqno.commit()
        assert exists('seqno')
        volume = db.Volume(tests.tmpdir, [Document1, Document2])
        self.assertEqual(4, volume.seqno.value)

    def test_Events(self):
        env.index_flush_threshold.value = 0
        env.index_flush_timeout.value = 0

        class Document1(db.Document):

            @db.indexed_property(slot=1, default='')
            def prop(self, value):
                pass

        class Document2(db.Document):

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
        volume = db.Volume(tests.tmpdir, [Document1, Document2])
        volume.connect(lambda event: events.append(event))

        volume.populate()
        self.assertEqual([
            {'event': 'commit', 'document': 'document1'},
            {'event': 'populate', 'document': 'document1'},
            ],
            events)
        del events[:]

        volume['document1'].create(guid='guid1')
        volume['document2'].create(guid='guid2')
        self.assertEqual([
            {'event': 'create', 'document': 'document1', 'guid': 'guid1', 'props': {
                'ctime': 0,
                'mtime': 0,
                'seqno': 0,
                'prop': '',
                'guid': 'guid1',
                }},
            {'event': 'create', 'document': 'document2', 'guid': 'guid2', 'props': {
                'ctime': 0,
                'mtime': 0,
                'seqno': 0,
                'prop': '',
                'guid': 'guid2',
                }},
            ],
            events)
        del events[:]

        volume['document1'].update('guid1', prop='foo')
        volume['document2'].update('guid2', prop='bar')
        self.assertEqual([
            {'event': 'update', 'document': 'document1', 'guid': 'guid1', 'props': {
                'prop': 'foo',
                }},
            {'event': 'update', 'document': 'document2', 'guid': 'guid2', 'props': {
                'prop': 'bar',
                }},
            ],
            events)
        del events[:]

        volume['document2'].set_blob('guid2', 'blob', StringIO('blob'))
        self.assertEqual([
            {'event': 'update', 'document': 'document2', 'guid': 'guid2', 'props': {
                'seqno': 5,
                }},
            ],
            events)
        del events[:]

        volume['document1'].delete('guid1')
        self.assertEqual([
            {'event': 'delete', 'document': 'document1', 'guid': 'guid1'},
            ],
            events)
        del events[:]

        volume['document1'].commit()
        volume['document2'].commit()

        self.assertEqual([
            {'event': 'commit', 'document': 'document1'},
            {'event': 'commit', 'document': 'document2'},
            ],
            events)

    def test_PermissionsNoWrite(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='', permissions=db.ACCESS_READ)
            def prop(self, value):
                pass

            @db.blob_property(permissions=db.ACCESS_READ)
            def blob(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})

        self.assertRaises(http.Forbidden, self.call, 'POST', document='testdocument', content={'prop': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, content={'prop': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, content={'blob': 'value'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, prop='prop', content='value')
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, prop='blob', content='value')

    def test_BlobsWritePermissions(self):

        class TestDocument(db.Document):

            @db.blob_property(permissions=db.ACCESS_CREATE | db.ACCESS_WRITE)
            def blob1(self, value):
                return value

            @db.blob_property(permissions=db.ACCESS_CREATE)
            def blob2(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        guid = self.call('POST', document='testdocument', content={})
        self.call('PUT', document='testdocument', guid=guid, content={'blob1': 'value1', 'blob2': 'value2'})
        self.call('PUT', document='testdocument', guid=guid, content={'blob1': 'value1'})
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, content={'blob2': 'value2_'})

        guid = self.call('POST', document='testdocument', content={})
        self.call('PUT', document='testdocument', guid=guid, prop='blob1', content='value1')
        self.call('PUT', document='testdocument', guid=guid, prop='blob2', content='value2')
        self.call('PUT', document='testdocument', guid=guid, prop='blob1', content='value1_')
        self.assertRaises(http.Forbidden, self.call, 'PUT', document='testdocument', guid=guid, prop='blob2', content='value2_')

    def test_properties_OverrideGet(self):

        class TestDocument(db.Document):

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

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})
        self.touch(('new-blob', 'new-blob'))
        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='old-blob')

        self.assertEqual(
                'new-blob',
                self.call('GET', document='testdocument', guid=guid, prop='blob')['blob'])
        self.assertEqual(
                '1',
                self.call('GET', document='testdocument', guid=guid, prop='prop1'))
        self.assertEqual(
                -1,
                self.call('GET', document='testdocument', guid=guid, prop='prop2'))
        self.assertEqual(
                {'prop1': '1', 'prop2': -1},
                self.call('GET', document='testdocument', guid=guid, reply=['prop1', 'prop2']))

    def test_properties_OverrideSet(self):

        class TestDocument(db.Document):

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
                return db.PropertyMetadata(url=value)

            @db.blob_property()
            def blob2(self, meta):
                return meta

            @blob2.setter
            def blob2(self, value):
                return ' %s ' % value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={})

        self.assertEqual('_1', self.call('GET', document='testdocument', guid=guid, prop='prop'))
        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob1')

        self.call('PUT', document='testdocument', guid=guid, prop='prop', content='2')
        self.assertEqual('_2', self.call('GET', document='testdocument', guid=guid, prop='prop'))
        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob1')

        self.call('PUT', document='testdocument', guid=guid, content={'prop': 3})
        self.assertEqual('_3', self.call('GET', document='testdocument', guid=guid, prop='prop'))
        self.assertRaises(http.NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob1')

        self.call('PUT', document='testdocument', guid=guid, prop='blob1', content='blob2')
        self.assertEqual('blob2', self.call('GET', document='testdocument', guid=guid, prop='blob1')['url'])

        guid = self.call('POST', document='testdocument', content={'blob2': 'foo'})
        self.assertEqual(' foo ', file(self.call('GET', document='testdocument', guid=guid, prop='blob2')['blob']).read())

        self.call('PUT', document='testdocument', guid=guid, prop='blob2', content='bar')
        self.assertEqual(' bar ', file(self.call('GET', document='testdocument', guid=guid, prop='blob2')['blob']).read())

    def test_properties_CallSettersAtTheEnd(self):

        class TestDocument(db.Document):

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

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={'prop1': 1, 'prop2': 2, 'prop3': 3})
        self.assertEqual(4, self.call('GET', document='testdocument', guid=guid, prop='prop1'))
        self.assertEqual(1, self.call('GET', document='testdocument', guid=guid, prop='prop2'))

    def test_SubCall(self):

        class TestDocument(db.Document):

            @db.blob_property(mime_type='application/json')
            def blob(self, value):
                return value

            @blob.setter
            def blob(self, value):
                if '!' not in value:
                    meta = self.meta('blob')
                    if meta:
                        value = file(meta['blob']).read() + value
                    coroutine.spawn(self.post, value)
                return value

            def post(self, value):
                self.request.call('PUT', document='testdocument', guid=self.guid, prop='blob', content=value + '!')

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        guid = self.call('POST', document='testdocument', content={'blob': '0'})
        coroutine.dispatch()
        self.assertEqual('0!', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())

        self.call('PUT', document='testdocument', guid=guid, prop='blob', content='1')
        coroutine.dispatch()
        self.assertEqual('0!1!', file(self.call('GET', document='testdocument', guid=guid, prop='blob')['blob']).read())

    def test_Group(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1)
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        self.call('POST', document='testdocument', content={'prop': 1})
        self.call('POST', document='testdocument', content={'prop': 2})
        self.call('POST', document='testdocument', content={'prop': 1})

        self.assertEqual(
                sorted([{'prop': 1}, {'prop': 2}]),
                sorted(self.call('GET', document='testdocument', reply='prop', group_by='prop')['result']))

    def test_CallSetterEvenIfThereIsNoCreatePermissions(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, permissions=db.ACCESS_READ, default=0)
            def prop(self, value):
                return value

            @prop.setter
            def prop(self, value):
                return value + 1

        self.volume = db.Volume(tests.tmpdir, [TestDocument])

        self.assertRaises(http.Forbidden, self.call, 'POST', document='testdocument', content={'prop': 1})

        guid = self.call('POST', document='testdocument', content={})
        self.assertEqual(1, self.call('GET', document='testdocument', guid=guid, prop='prop'))

    def test_ReturnDefualtsForMissedProps(self):

        class TestDocument(db.Document):

            @db.indexed_property(slot=1, default='default')
            def prop(self, value):
                return value

        self.volume = db.Volume(tests.tmpdir, [TestDocument])
        guid = self.call('POST', document='testdocument', content={'prop': 'set'})

        self.assertEqual(
                [{'prop': 'set'}],
                self.call('GET', document='testdocument', reply='prop')['result'])
        self.assertEqual(
                {'prop': 'set'},
                self.call('GET', document='testdocument', guid=guid, reply='prop'))
        self.assertEqual(
                'set',
                self.call('GET', document='testdocument', guid=guid, prop='prop'))

        os.unlink('testdocument/%s/%s/prop' % (guid[:2], guid))

        self.assertEqual(
                [{'prop': 'default'}],
                self.call('GET', document='testdocument', reply='prop')['result'])
        self.assertEqual(
                {'prop': 'default'},
                self.call('GET', document='testdocument', guid=guid, reply='prop'))
        self.assertEqual(
                'default',
                self.call('GET', document='testdocument', guid=guid, prop='prop'))

    def call(self, method, document=None, guid=None, prop=None,
            accept_language=None, content=None, content_stream=None,
            content_type=None, if_modified_since=None, static_prefix=None,
            **kwargs):

        class TestRequest(db.Request):

            content_stream = None
            content_length = 0

        request = TestRequest(**kwargs)
        request.static_prefix = static_prefix
        request.content = content
        request.content_stream = content_stream
        request.content_type = content_type
        request.accept_language = accept_language
        request.if_modified_since = if_modified_since
        request['method'] = method
        if document:
            request['document'] = document
        if guid:
            request['guid'] = guid
        if prop:
            request['prop'] = prop
        if request.content_stream is not None:
            request.content_length = len(request.content_stream.getvalue())

        self.response = db.Response()
        cp = VolumeCommands(self.volume)
        return cp.call(request, self.response)


if __name__ == '__main__':
    tests.main()
