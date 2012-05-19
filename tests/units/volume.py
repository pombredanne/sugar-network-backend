#!/usr/bin/env python
# sugar-lint: disable

import os
import shutil
from cStringIO import StringIO
from email.message import Message
from os.path import dirname, join, abspath

src_root = abspath(dirname(__file__))

from __init__ import tests

from active_document import volume, document, SingleVolume, \
        Request, Response, Document, active_property, \
        BlobProperty, NotFound, sockets


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

        self.volume = SingleVolume(tests.tmpdir, [TestDocument])

    def test_walk_classes(self):
        classes = volume._walk_classes(join(src_root, 'document_classes'))
        self.assertEqual(
                ['Resource_1', 'Resource_3'],
                sorted([i.__name__ for i in classes]))

    def test_SingleVolume_Populate(self):
        self.touch(
                ('document/1/1/.seqno', ''),
                ('document/1/1/guid', '1'),
                ('document/1/1/ctime', '1'),
                ('document/1/1/mtime', '1'),
                ('document/1/1/layers', '["public"]'),
                ('document/1/1/author', '["me"]'),

                ('document/2/2/.seqno', ''),
                ('document/2/2/guid', '2'),
                ('document/2/2/ctime', '2'),
                ('document/2/2/mtime', '2'),
                ('document/2/2/layers', '["public"]'),
                ('document/2/2/author', '["me"]'),
                )

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

        shutil.rmtree('document/index')

        class Document(document.Document):
            pass

        with SingleVolume(tests.tmpdir, [Document]) as volume:
            self.assertEqual(
                    sorted(['1', '2']),
                    sorted([i.guid for i in volume['document'].find()[0]]))

    def test_Commands(self):
        self.volume['testdocument'].create_with_guid('guid', {'author': []})

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

        self.assertEqual(
                None,
                self.call('GET', cmd='stat-blob', document='testdocument', guid=guid_1, prop='blob'))

        self.call('PUT', document='testdocument', guid=guid_1, prop='blob', content_stream=StringIO('blob-value'))

        self.assertEqual(
                len('blob-value'),
                self.call('GET', cmd='stat-blob', document='testdocument', guid=guid_1, prop='blob')['size'])

        stream = self.call('GET', document='testdocument', guid=guid_1, prop='blob')
        self.assertEqual('blob-value', ''.join([i for i in stream]))
        self.assertEqual('application/octet-stream', self.response.content_type)
        self.assertEqual(len('blob-value'), self.response.content_length)

    def test_CommandsGetBlobDirectory(self):
        guid = self.call('POST', document='testdocument', content={})

        blob_path = tests.tmpdir + '/testdocument/%s/%s/blob' % (guid[:2], guid)
        self.touch(blob_path + '.sha1')
        self.touch((blob_path + '/1/2/3', 'a'))
        self.touch((blob_path + '/4/5', 'b'))
        self.touch((blob_path + '/6', 'c'))

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

    def test_Command_ReplyForGET(self):
        guid = self.call('POST', document='testdocument', content={'prop': 'value'})

        self.assertEqual(
                sorted(['layers', 'ctime', 'author', 'prop', 'mtime', 'guid']),
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

    def call(self, method, document=None, guid=None, prop=None,
            principal=None, **kwargs):

        class TestRequest(Request):

            content_stream = None
            content_length = 0
            principal = None

        request = TestRequest(kwargs)
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
