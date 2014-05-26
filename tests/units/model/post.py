#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db
from sugar_network.client import Connection, keyfile
from sugar_network.model.context import Context
from sugar_network.node.auth import RootAuth
from sugar_network.model.post import Post
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http


class PostTest(tests.Test):

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
            'type': 'topic',
            'title': {},
            'message': {},
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'topic',
            'title': {},
            'message': {},
            'vote': 0,
            })
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'topic',
            'title': {},
            'message': {},
            'vote': 1,
            })
        self.assertEqual([1, 1], volume['context'][context]['rating'])

        volume['post'].create({
            'context': context,
            'type': 'topic',
            'title': {},
            'message': {},
            'vote': 2,
            })
        self.assertEqual([2, 3], volume['context'][context]['rating'])

    def test_ShiftContextRatingOnDeletes(self):
        volume = self.start_master(auth=RootAuth())
        context = this.call(method='POST', path=['context'], content={'title': '', 'summary': '', 'description': '', 'type': 'activity'})

        post1 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'review', 'title': '', 'message': '', 'vote': 1})
        post2 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'review', 'title': '', 'message': '', 'vote': 2})
        self.assertEqual([2, 3], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual([1, 2], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual([0, 0], volume['context'][context]['rating'])

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
            'type': 'topic',
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

    def test_ShiftTopicRatingOnDeletes(self):
        volume = self.start_master(auth=RootAuth())
        topic = this.call(method='POST', path=['post'], content={'context': '', 'type': 'topic', 'title': '', 'message': ''})

        post1 = this.call(method='POST', path=['post'], content={'context': '', 'topic': topic, 'type': 'post', 'title': '', 'message': '', 'vote': 1})
        post2 = this.call(method='POST', path=['post'], content={'context': '', 'topic': topic, 'type': 'post', 'title': '', 'message': '', 'vote': 2})
        self.assertEqual([2, 3], volume['post'][topic]['rating'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual([1, 2], volume['post'][topic]['rating'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual([0, 0], volume['post'][topic]['rating'])

    def test_DoesTypeCorrespondToTopicValue(self):
        volume = self.start_master(auth=RootAuth())

        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'type': 'post', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'type': 'solution', 'title': '', 'message': ''})

        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'topic', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'review', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'artefact', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'question', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'issue', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'idea', 'title': '', 'message': ''})
        self.assertRaises(RuntimeError, this.call, method='POST', path=['post'],
                content={'context': '', 'topic': 'topic', 'type': 'notice', 'title': '', 'message': ''})

if __name__ == '__main__':
    tests.main()
