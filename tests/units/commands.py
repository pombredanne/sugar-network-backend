#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from active_document import env, volume, SingleVolume, Document, \
        active_command, directory_command, volume_command


calls = []


class TestDocument(Document):

    @active_command()
    def command_1(self, directory):
        calls.append(('command_1', self.guid))

    @active_command(cmd='command_2')
    def command_2(self, directory):
        calls.append(('command_2', self.guid))

    @active_command(cmd='command_3', permissions=env.ACCESS_AUTH)
    def command_3(self, directory):
        calls.append(('command_3', self.guid))

    @active_command(cmd='command_4', permissions=env.ACCESS_AUTHOR)
    def command_4(self, directory):
        calls.append(('command_4', self.guid))

    @classmethod
    @active_command()
    def command_5(cls, directory):
        calls.append('command_5')

    @classmethod
    @active_command(cmd='command_6')
    def command_6(cls, directory):
        calls.append('command_6')

    @classmethod
    @active_command(cmd='command_7', permissions=env.ACCESS_AUTH)
    def command_7(cls, directory):
        calls.append('command_7')


@directory_command()
def command_8(directory):
    calls.append('command_8')


@directory_command(cmd='command_9')
def command_9(directory):
    calls.append('command_9')


@directory_command(cmd='command_10', permissions=env.ACCESS_AUTH)
def command_10(directory):
    calls.append('command_10')


@volume_command()
def command_11():
    calls.append('command_11')


@volume_command(cmd='command_12')
def command_12():
    calls.append('command_12')


@volume_command(cmd='command_13', permissions=env.ACCESS_AUTH)
def command_13():
    calls.append('command_13')


class CommandsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        del calls[:]
        self.volume = SingleVolume(tests.tmpdir, [TestDocument])
        self.volume['testdocument'].create_with_guid('guid', {})

    def test_Commands(self):
        self.volume.call('GET', 'testdocument', 'guid')
        self.assertRaises(env.NoCommand, self.volume.call, ('GET', 'command_1'), 'testdocument', 'guid')
        self.volume.call(('GET', 'command_2'), 'testdocument', 'guid')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.volume.call, ('GET', 'command_3'), 'testdocument', 'guid')
        env.principal.user = 'me'
        self.volume.call(('GET', 'command_3'), 'testdocument', 'guid')
        env.principal.user = 'fake'
        self.assertRaises(env.Forbidden, self.volume.call, ('GET', 'command_4'), 'testdocument', 'guid')
        env.principal.user = 'me'
        self.volume.call(('GET', 'command_4'), 'testdocument', 'guid')

        self.volume.call('GET', 'testdocument')
        self.assertRaises(env.NoCommand, self.volume.call, ('GET', 'command_5'), 'testdocument')
        self.volume.call(('GET', 'command_6'), 'testdocument')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.volume.call, ('GET', 'command_7'), 'testdocument')
        env.principal.user = 'me'
        self.volume.call(('GET', 'command_7'), 'testdocument')

        self.volume.call('GET', 'testdocument')
        self.volume.call(('GET', 'command_9'), 'testdocument')
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.volume.call, ('GET', 'command_10'), 'testdocument')
        env.principal.user = 'me'
        self.volume.call(('GET', 'command_10'), 'testdocument')

        self.volume.call('GET')
        self.assertRaises(env.NoCommand, self.volume.call, ('GET', 'command_11'))
        self.volume.call(('GET', 'command_12'))
        env.principal.user = None
        self.assertRaises(env.Unauthorized, self.volume.call, ('GET', 'command_13'))
        env.principal.user = 'me'
        self.volume.call(('GET', 'command_13'))

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


if __name__ == '__main__':
    tests.main()
