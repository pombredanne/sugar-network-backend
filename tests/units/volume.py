#!/usr/bin/env python
# sugar-lint: disable

import os
import sys
import shutil
from cStringIO import StringIO
from email.message import Message
from os.path import dirname, join, abspath, exists

src_root = abspath(dirname(__file__))

from __init__ import tests

from active_document import env, volume, document, SingleVolume, \
        Request, Response, Document, active_property, \
        BlobProperty, NotFound, Redirect
from active_toolkit import sockets, coroutine


class VolumeTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)

        class TestDocument(Document):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

            @active_property(BlobProperty)
            def blob(self, value):
                return value

            @active_property(prefix='L', localized=True, default='')
            def localized_prop(self, value):
                return value

        self.volume = SingleVolume(tests.tmpdir, [TestDocument])

    def test_Populate(self):
        self.touch(
                ('document/1/1/guid', '{"value": "1"}'),
                ('document/1/1/ctime', '{"value": 1}'),
                ('document/1/1/mtime', '{"value": 1}'),
                ('document/1/1/layer', '{"value": ["public"]}'),
                ('document/1/1/user', '{"value": ["me"]}'),
                ('document/1/1/seqno', '{"value": 0}'),

                ('document/2/2/guid', '{"value": "2"}'),
                ('document/2/2/ctime', '{"value": 2}'),
                ('document/2/2/mtime', '{"value": 2}'),
                ('document/2/2/layer', '{"value": ["public"]}'),
                ('document/2/2/user', '{"value": ["me"]}'),
                ('document/2/2/seqno', '{"value": 0}'),
                )

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('document/index')

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

    def test_UpdatedSchemeOnReindex(self):
        self.touch(
                ('document/1/1/guid', '{"value": "1"}'),
                ('document/1/1/ctime', '{"value": 1}'),
                ('document/1/1/mtime', '{"value": 1}'),
                ('document/1/1/layer', '{"value": ["public"]}'),
                ('document/1/1/user', '{"value": ["me"]}'),
                ('document/1/1/seqno', '{"value": 0}'),
                )

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertRaises(RuntimeError, lambda: volume['document'].get('1')['prop'])

        class Document(document.Document):

            @active_property(slot=1, default='default')
            def prop(self, value):
                return value

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            for cls in volume.values():
                for __ in cls.populate():
                    pass
            self.assertEqual('default', volume['document'].get('1')['prop'])

    def test_Commands(self):
        self.volume['testdocument'].create_with_guid('guid', {'user': []})

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

        self.assertRaises(NotFound, self.call, 'GET', document='testdocument', guid=guid_2)

        self.assertEqual(
                {'guid': guid_1, 'prop': 'value_3'},
                self.call('GET', document='testdocument', guid=guid_1, reply=['guid', 'prop']))

        self.assertEqual(
                'value_3',
                self.call('GET', document='testdocument', guid=guid_1, prop='prop'))

        self.call('PUT', document='testdocument', guid=guid_1, prop='blob', content_stream=StringIO('blob-value'))

        stream = self.call('GET', document='testdocument', guid=guid_1, prop='blob')
        self.assertEqual('blob-value', ''.join([i for i in stream]))
        self.assertEqual('application/octet-stream', self.response.content_type)
        self.assertEqual(len('blob-value'), self.response.content_length)

    def test_CommandsGetBlobDirectory(self):
        guid = self.call('POST', document='testdocument', content={})

        blob_path = tests.tmpdir + '/testdocument/%s/%s/blob' % (guid[:2], guid)
        self.touch((blob_path, '{}'))
        self.touch((blob_path + '.blob/1/2/3', 'a'))
        self.touch((blob_path + '.blob/4/5', 'b'))
        self.touch((blob_path + '.blob/6', 'c'))

        stream = StringIO()
        for chunk in self.call('GET', document='testdocument', guid=guid, prop='blob'):
            stream.write(chunk)
        stream.seek(0)

        msg = Message()
        msg['content-type'] = self.response.content_type

        files = sockets.decode_multipart(stream, self.response.content_length,
                msg.get_boundary())
        self.assertEqual(
                sorted([
                    ('1/2/3', 'a'),
                    ('4/5', 'b'),
                    ('6', 'c'),
                    ]),
                sorted([(name, content.read()) for name, content in files]))

    def test_CommandsGetAbsentBlobs(self):
        guid = self.call('POST', document='testdocument', content={'prop': 'value'})
        self.assertEqual('value', self.call('GET', document='testdocument', guid=guid, prop='prop'))
        self.assertRaises(NotFound, self.call, 'GET', document='testdocument', guid=guid, prop='blob')

    def test_Command_ReplyForGET(self):
        guid = self.call('POST', document='testdocument', content={'prop': 'value'})

        self.assertEqual(
                sorted(['layer', 'ctime', 'user', 'prop', 'mtime', 'guid', 'localized_prop']),
                sorted(self.call('GET', document='testdocument', guid=guid).keys()))

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
                sorted(['guid', 'prop']),
                sorted(self.call('GET', document='testdocument', reply=['prop'])['result'][0].keys()))

    def test_Command_GetBlobBySeqno(self):
        # seqno == 1
        guid = self.call('POST', document='testdocument', content={})
        # seqno == 2
        self.call('PUT', document='testdocument', guid=guid, prop='blob', content_stream=StringIO('value'))

        blob = self.call('GET', document='testdocument', guid=guid, prop='blob')
        self.assertEqual('value', ''.join(blob))
        self.assertEqual(len('value'), self.response.content_length)

        blob = self.call('GET', document='testdocument', guid=guid, prop='blob', seqno=0)
        self.assertEqual('value', ''.join(blob))
        self.assertEqual(len('value'), self.response.content_length)

        blob = self.call('GET', document='testdocument', guid=guid, prop='blob', seqno=1)
        self.assertEqual('value', ''.join(blob))
        self.assertEqual(len('value'), self.response.content_length)

        self.assertEqual(None, self.call('GET', document='testdocument', guid=guid, prop='blob', seqno=2))
        self.assertEqual(0, self.response.content_length)

        self.assertEqual(None, self.call('GET', document='testdocument', guid=guid, prop='blob', seqno=22))
        self.assertEqual(0, self.response.content_length)

    def test_LocalizedSet(self):
        env.DEFAULT_LANG = 'en'

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
        directory = self.volume['testdocument']

        guid = self.call('POST', document='testdocument', content={
            'localized_prop': {
                'ru': 'value_ru',
                'es': 'value_es',
                'en': 'value_en',
                },
            })

        env.DEFAULT_LANG = 'en'

        self.assertEqual(
                {
                    'localized_prop': {
                        'ru': 'value_ru',
                        'es': 'value_es',
                        'en': 'value_en',
                        },
                    },
                self.call('GET', document='testdocument', guid=guid, reply=['localized_prop']))

        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru', 'es'], prop='localized_prop'))
        self.assertEqual(
                [{'guid': guid, 'localized_prop': 'value_ru'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'ru', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_ru'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru-RU'], reply=['localized_prop']))
        self.assertEqual(
                'value_ru',
                self.call('GET', document='testdocument', guid=guid, accept_language=['ru-RU', 'es'], prop='localized_prop'))
        self.assertEqual(
                [{'guid': guid, 'localized_prop': 'value_ru'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'ru-RU', 'es'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_es'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['es'], reply=['localized_prop']))
        self.assertEqual(
                'value_es',
                self.call('GET', document='testdocument', guid=guid, accept_language=['es', 'ru'], prop='localized_prop'))
        self.assertEqual(
                [{'guid': guid, 'localized_prop': 'value_es'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'es', 'ru'], reply=['localized_prop'])['result'])

        self.assertEqual(
                {'localized_prop': 'value_en'},
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_en',
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr', 'za'], prop='localized_prop'))
        self.assertEqual(
                [{'guid': guid, 'localized_prop': 'value_en'}],
                self.call('GET', document='testdocument', accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

        env.DEFAULT_LANG = 'foo'
        fallback_lang = sorted(['ru', 'es', 'en'])[0]

        self.assertEqual(
                {'localized_prop': 'value_%s' % fallback_lang},
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr'], reply=['localized_prop']))
        self.assertEqual(
                'value_%s' % fallback_lang,
                self.call('GET', document='testdocument', guid=guid, accept_language=['fr', 'za'], prop='localized_prop'))
        self.assertEqual(
                [{'guid': guid, 'localized_prop': 'value_%s' % fallback_lang}],
                self.call('GET', document='testdocument', accept_language=['foo', 'fr', 'za'], reply=['localized_prop'])['result'])

    def test_LazyOpen(self):

        class Document1(document.Document):
            pass

        class Document2(document.Document):
            pass

        volume = SingleVolume('.', [Document1, Document2], lazy_open=True)
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

        volume = SingleVolume('.', [Document1, Document2], lazy_open=False)
        assert exists('document1/index')
        assert exists('document2/index')
        volume.close()

    def test_OpenByModuleName(self):
        self.touch(
                ('foo/bar.py', [
                    'from active_document import Document',
                    'class Bar(Document): pass',
                    ]),
                ('foo/__init__.py', ''),
                )
        sys.path.insert(0, '.')

        volume = SingleVolume('.', ['foo.bar'])
        assert exists('bar/index')
        volume['bar'].find()
        volume.close()

    def test_Command_GetBlobSetByUrl(self):
        guid = self.call('POST', document='testdocument', content={})
        self.call('PUT', document='testdocument', guid=guid, prop='blob', url='http://sugarlabs.org')

        try:
            self.call('GET', document='testdocument', guid=guid, prop='blob')
            assert False
        except Redirect, redirect:
            self.assertEqual('http://sugarlabs.org', redirect.location)

    def call(self, method, document=None, guid=None, prop=None,
            principal=None, accept_language=None, **kwargs):

        class TestRequest(Request):

            content_stream = None
            content_length = 0
            principal = None

        request = TestRequest(kwargs)
        request.accept_language = accept_language
        request['method'] = method
        request.principal = principal
        if 'content' in kwargs:
            request.content = request.pop('content')
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
        cp = volume.VolumeCommands(self.volume)
        return cp.call(request, self.response)


if __name__ == '__main__':
    tests.main()
