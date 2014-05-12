#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
from sugar_network.client import Connection, keyfile
from sugar_network.model.context import Context
from sugar_network.model.post import Post
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http


class PostTest(tests.Test):

    def test_FindComments(self):
        directory = db.Volume('db', [Post])['post']

        directory.create({'guid': '1', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'comments': {
            '1': {'value': {'en': 'foo'}},
            }})
        directory.create({'guid': '2', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'comments': {
            '1': {'value': {'en': 'bar'}},
            }})
        directory.create({'guid': '3', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'comments': {
            '1': {'value': {'en': 'bar'}},
            '2': {'value': {'en': 'foo'}},
            }})
        directory.create({'guid': '4', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'comments': {
            '1': {'value': {'en': 'foo bar'}},
            }})

        self.assertEqual(
                ['1', '3', '4'],
                [i.guid for i in directory.find(query='foo')[0]])
        self.assertEqual(
                ['2', '3', '4'],
                [i.guid for i in directory.find(query='bar')[0]])
        self.assertEqual(
                ['1', '2', '3', '4'],
                [i.guid for i in directory.find(query='foo bar')[0]])

        self.assertEqual(
                ['1', '3', '4'],
                [i.guid for i in directory.find(query='comments:foo')[0]])


if __name__ == '__main__':
    tests.main()
