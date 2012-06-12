#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import Client
from sugar_network.toolkit import sugar


class IPCServerTest(tests.Test):

    def test_SetAuthor(self):
        self.override(sugar, 'uid', lambda: tests.UID)

        self.start_server()
        client = Client('~')

        user = client.User(
                nickname='me',
                fullname='M. E.',
                color='',
                machine_sn='',
                machine_uuid='',
                pubkey=tests.PUBKEY,
                ).post()

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()

        context = client.Context(guid, reply=['user', 'author'])
        self.assertEqual([user], context['user'])
        self.assertEqual(['me', 'M. E.'], context['author'])


if __name__ == '__main__':
    tests.main()
