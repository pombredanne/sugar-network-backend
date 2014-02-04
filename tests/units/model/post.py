#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
from sugar_network.client import Connection, keyfile
from sugar_network.model.user import User
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

    def test_ShiftContextRating(self):
        volume = db.Volume('db', [Context, Post])
        this.volume = volume

        context = volume['context'].create({
            'type': 'activity',
            'title': {},
            'summary': {},
            'description': {},
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'post',
            'title': {},
            'message': {},
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 0,
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 1,
            })
        self.assertEqual([1, 1], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 2,
            })
        self.assertEqual([2, 3], volume['context'][context]['rating'])

    def test_ShiftTopicRating(self):
        volume = db.Volume('db2', [Context, Post])
        this.volume = volume

        context = volume['context'].create({
            'type': 'activity',
            'title': {},
            'summary': {},
            'description': {},
            })
        topic = volume['post'].create({
            'context': context,
            'type': 'post',
            'title': {},
            'message': {},
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])
        self.assertEqual([0, 0], volume['post'][topic]['rating'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])
        self.assertEqual([0, 0], volume['post'][topic]['rating'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 0,
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])
        self.assertEqual([0, 0], volume['post'][topic]['rating'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 1,
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])
        self.assertEqual([1, 1], volume['post'][topic]['rating'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            'vote': 2,
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])
        self.assertEqual([2, 3], volume['post'][topic]['rating'])


if __name__ == '__main__':
    tests.main()
