#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from active_document import env, volume, SingleVolume, Document, \
        active_command, directory_command, volume_command, call, \
        active_property, Request, BlobProperty


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
        self.volume['testdocument'].create_with_guid('guid', {})

    def call(self, cmd, document=None, guid=None, prop=None, **kwargs):

        class TestRequest(Request):

            content = None
            content_stream = None
            content_length = 0

        request = TestRequest(kwargs)
        request['cmd'] = cmd
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

        return call(request, self.volume)

    def test_Commands(self):
        self.call('PROBE', 'testdocument', 'guid')
        self.assertRaises(env.NoCommand, self.call, ('PROBE', 'command_1'), 'testdocument', 'guid')
        self.call(('PROBE', 'command_2'), 'testdocument', 'guid')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_3'), 'testdocument', 'guid')
        env.principal.user = 'me'
        self.call(('PROBE', 'command_3'), 'testdocument', 'guid')
        env.principal.user = 'fake'
        self.assertRaises(env.Forbidden, self.call, ('PROBE', 'command_4'), 'testdocument', 'guid')
        env.principal.user = 'me'
        self.call(('PROBE', 'command_4'), 'testdocument', 'guid')

        self.call('PROBE', 'testdocument')
        self.assertRaises(env.NoCommand, self.call, ('PROBE', 'command_5'), 'testdocument')
        self.call(('PROBE', 'command_6'), 'testdocument')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_7'), 'testdocument')
        env.principal.user = 'me'
        self.call(('PROBE', 'command_7'), 'testdocument')

        self.call('PROBE', 'testdocument')
        self.call(('PROBE', 'command_9'), 'testdocument')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_10'), 'testdocument')
        env.principal.user = 'me'
        self.call(('PROBE', 'command_10'), 'testdocument')

        self.call('PROBE')
        self.assertRaises(env.NoCommand, self.call, ('PROBE', 'command_11'))
        self.call(('PROBE', 'command_12'))
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.call, ('PROBE', 'command_13'))
        env.principal.user = 'me'
        self.call(('PROBE', 'command_13'))

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

    def test_VolumeCommands(self):
        self.assertEqual({
            'total': 1,
            'result': [
                {'guid': 'guid', 'prop': ''},
                ],
            },
            self.call('GET', 'testdocument', reply=['guid', 'prop']))

        guid_1 = self.call('POST', 'testdocument', content={'prop': 'value_1'})
        assert guid_1
        guid_2 = self.call('POST', 'testdocument', content={'prop': 'value_2'})
        assert guid_2

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_1'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', 'testdocument', reply=['guid', 'prop'])['result']))

        self.call('PUT', 'testdocument', guid_1, content={'prop': 'value_3'})

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    {'guid': guid_2, 'prop': 'value_2'},
                    ]),
                sorted(self.call('GET', 'testdocument', reply=['guid', 'prop'])['result']))

        self.call('DELETE', 'testdocument', guid_2)

        self.assertEqual(
                sorted([
                    {'guid': 'guid', 'prop': ''},
                    {'guid': guid_1, 'prop': 'value_3'},
                    ]),
                sorted(self.call('GET', 'testdocument', reply=['guid', 'prop'])['result']))

        self.assertEqual(
                {'guid': guid_1, 'prop': 'value_3'},
                self.call('GET', 'testdocument', guid_1, reply=['guid', 'prop']))

        self.assertEqual(
                'value_3',
                self.call('GET', 'testdocument', guid_1, 'prop'))

        self.assertEqual(
                None,
                self.call(('GET', 'stat-blob'), 'testdocument', guid_1, 'blob'))

        self.call('PUT', 'testdocument', guid_1, 'blob', content_stream=StringIO('blob-value'))

        self.assertEqual(
                len('blob-value'),
                self.call(('GET', 'stat-blob'), 'testdocument', guid_1, 'blob')['size'])

        stream, size, mime_type = self.call('GET', 'testdocument', guid_1, 'blob')
        self.assertEqual(len('blob-value'), size)
        self.assertEqual('blob-value', ''.join([i for i in stream]))


if __name__ == '__main__':
    tests.main()
