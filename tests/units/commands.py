#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from active_document import env, volume, SingleVolume, Document, \
        active_command, directory_command, volume_command, call, \
        active_property, Request, BlobProperty, Response


calls = []


class TestDocument(Document):

    @active_property(slot=1, default='')
    def prop(self, value):
        return value

    @active_property(BlobProperty)
    def blob(self, value):
        return value

    @active_command(method='PROBE')
    def command_1(self, directory):
        calls.append(('command_1', self.guid))

    @active_command(method='PROBE', cmd='command_2')
    def command_2(self, directory):
        calls.append(('command_2', self.guid))

    @active_command(method='PROBE', cmd='command_3', permissions=env.ACCESS_AUTH)
    def command_3(self, directory):
        calls.append(('command_3', self.guid))

    @active_command(method='PROBE', cmd='command_4', permissions=env.ACCESS_AUTHOR)
    def command_4(self, directory):
        calls.append(('command_4', self.guid))

    @classmethod
    @active_command(method='PROBE')
    def command_5(cls, directory):
        calls.append('command_5')

    @classmethod
    @active_command(method='PROBE', cmd='command_6')
    def command_6(cls, directory):
        calls.append('command_6')

    @classmethod
    @active_command(method='PROBE', cmd='command_7', permissions=env.ACCESS_AUTH)
    def command_7(cls, directory):
        calls.append('command_7')


@directory_command(method='PROBE')
def command_8(directory):
    calls.append('command_8')


@directory_command(method='PROBE', cmd='command_9')
def command_9(directory):
    calls.append('command_9')


@directory_command(method='PROBE', cmd='command_10', permissions=env.ACCESS_AUTH)
def command_10(directory):
    calls.append('command_10')


@volume_command(method='PROBE')
def command_11():
    calls.append('command_11')


@volume_command(method='PROBE', cmd='command_12')
def command_12():
    calls.append('command_12')


@volume_command(method='PROBE', cmd='command_13', permissions=env.ACCESS_AUTH)
def command_13():
    calls.append('command_13')


class CommandsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        del calls[:]
        self.volume = SingleVolume(tests.tmpdir, [TestDocument])
        self.volume['testdocument'].create_with_guid('guid', {'author': ['me']})

    def call(self, cmd, document=None, guid=None, prop=None, principal=None,
            **kwargs):

        class TestRequest(Request):

            content = None
            content_stream = None
            content_length = 0
            principal = None

        request = TestRequest(kwargs)
        request.command = cmd
        request.principal = principal
        if document:
            request['document'] = document
        if guid:
            request['guid'] = guid
        if prop:
            request['prop'] = prop
        if 'content' in request:
            request.content = request.pop('content')
        if 'content_stream' in request:
            request.content_stream = request.pop('content_stream')
            request.content_length = len(request.content_stream.getvalue())

        self.response = Response()

        return call(self.volume, request, self.response)

    def test_ScanForCommands(self):
        self.call('PROBE', 'testdocument', 'guid')
        self.assertRaises(env.NotFound, self.call, ('PROBE', 'command_1'), 'testdocument', 'guid')
        self.call(('PROBE', 'command_2'), 'testdocument', 'guid')
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_3'), 'testdocument', 'guid', principal=Request.ANONYMOUS)
        self.call(('PROBE', 'command_3'), 'testdocument', 'guid', principal='me')
        self.assertRaises(env.Forbidden, self.call, ('PROBE', 'command_4'), 'testdocument', 'guid', principal='fake')
        self.call(('PROBE', 'command_4'), 'testdocument', 'guid', principal='me')

        self.call('PROBE', 'testdocument')
        self.assertRaises(env.NotFound, self.call, ('PROBE', 'command_5'), 'testdocument')
        self.call(('PROBE', 'command_6'), 'testdocument')
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_7'), 'testdocument', principal=Request.ANONYMOUS)
        self.call(('PROBE', 'command_7'), 'testdocument', principal='me')

        self.call('PROBE', 'testdocument')
        self.call(('PROBE', 'command_9'), 'testdocument')
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_10'), 'testdocument', principal=Request.ANONYMOUS)
        self.call(('PROBE', 'command_10'), 'testdocument', principal='me')

        self.call('PROBE')
        self.assertRaises(env.NotFound, self.call, ('PROBE', 'command_11'))
        self.call(('PROBE', 'command_12'))
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_13'), principal=Request.ANONYMOUS)
        self.call(('PROBE', 'command_13'), principal='me')

        self.assertEqual([
            ('command_1', 'guid'),
            ('command_2', 'guid'),
            ('command_3', 'guid'),
            ('command_4', 'guid'),

            'command_8',
            'command_6',
            'command_7',

            'command_8',
            'command_9',
            'command_10',

            'command_11',
            'command_12',
            'command_13',
            ],
            calls)

    def _test_AssertPermissions(self):

        class Document(document.Document):

            @active_property(slot=1, default='')
            def prop(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        directory.metadata['prop']._permissions = 0
        guid = directory.create({})
        doc = directory.get(guid)

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, lambda: doc['prop'])
        directory.metadata['prop']._permissions = env.ACCESS_READ
        doc['prop']

        directory.metadata['prop']._permissions = 0
        documents, total = directory.find(0, 100, reply=['guid', 'prop'])
        self.assertRaises(env.Forbidden, lambda: documents.next().prop)
        directory.metadata['prop']._permissions = env.ACCESS_READ
        documents, total = directory.find(0, 100, reply=['guid', 'prop'])
        documents.next().prop

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        directory.create({'prop': '1'})

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        self.assertRaises(env.Forbidden, directory.create, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        directory.create({'prop': '1'})

        directory.metadata['prop']._permissions = 0
        self.assertRaises(env.Forbidden, directory.update, guid, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_CREATE
        self.assertRaises(env.Forbidden, directory.update, guid, {'prop': '1'})
        directory.metadata['prop']._permissions = env.ACCESS_WRITE
        directory.update(guid, {'prop': '1'})

    def _test_UpdateInternalProps(self):

        class Document(document.Document):
            pass

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        self.assertRaises(env.Forbidden, directory.create, {'ctime': 1})
        self.assertRaises(env.Forbidden, directory.create, {'mtime': 1})

    def _test_authorize_property_Blobs(self):

        class Document(document.Document):

            @active_property(BlobProperty)
            def blob(self, value):
                return value

        directory = Directory(tests.tmpdir, Document, IndexWriter)

        guid = directory.create({})

        directory.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, directory.get_blob, guid, 'blob')
        directory.metadata['blob']._permissions = env.ACCESS_READ
        directory.get_blob(guid, 'blob')

        directory.metadata['blob']._permissions = 0
        self.assertRaises(env.Forbidden, directory.set_blob, guid, 'blob', StringIO('data'))
        directory.metadata['blob']._permissions = env.ACCESS_WRITE
        directory.set_blob(guid, 'blob', StringIO('data'))


if __name__ == '__main__':
    tests.main()
