#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
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

    def test_RatingSort(self):
        directory = db.Volume('db', [Post])['post']

        directory.create({'guid': '1', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'rating': [0, 0]})
        directory.create({'guid': '2', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'rating': [1, 2]})
        directory.create({'guid': '3', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'rating': [1, 4]})
        directory.create({'guid': '4', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'rating': [10, 10]})
        directory.create({'guid': '5', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'rating': [30, 90]})

        self.assertEqual(
                ['1', '2', '3', '4', '5'],
                [i.guid for i in directory.find()[0]])
        self.assertEqual(
                ['1', '4', '2', '5', '3'],
                [i.guid for i in directory.find(order_by='rating')[0]])
        self.assertEqual(
                ['3', '5', '2', '4', '1'],
                [i.guid for i in directory.find(order_by='-rating')[0]])

    def test_FindComments(self):
        directory = db.Volume('db', [Post])['post']

        directory.create({'guid': '1', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'comments': {
            '1': {'message': 'foo'},
            }})
        directory.create({'guid': '2', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'comments': {
            '1': {'message': 'bar'},
            }})
        directory.create({'guid': '3', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'comments': {
            '1': {'message': 'bar'},
            '2': {'message': 'foo'},
            }})
        directory.create({'guid': '4', 'context': '', 'type': 'comment', 'title': '', 'message': '', 'comments': {
            '1': {'message': 'foo bar'},
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
