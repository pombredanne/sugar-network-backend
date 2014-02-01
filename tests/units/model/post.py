#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.client import Connection, keyfile
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.post import Post
from sugar_network.model.release import Release
from sugar_network.toolkit import http


class PostTest(tests.Test):

    def test_SetContext(self):
        volume = self.start_master([User, Context, Release, Post])
        client = Connection(auth=http.SugarAuth(keyfile.value))

        self.assertRaises(http.NotFound, client.post, ['post'], {'type': 'comment', 'title': '', 'message': '', 'topic': 'absent'})

        context = client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        topic = client.post(['post'], {
            'context': context,
            'title': 'title',
            'message': 'message',
            'type': 'update',
            })
        comment = client.post(['post'], {
            'topic': topic,
            'title': 'title',
            'message': 'message',
            'type': 'comment',
            })
        self.assertEqual(
                context,
                client.get(['post', comment, 'context']))


if __name__ == '__main__':
    tests.main()
