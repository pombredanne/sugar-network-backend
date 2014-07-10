#!/usr/bin/env python
# sugar-lint: disable

import time
import hashlib
from M2Crypto import RSA
from os.path import join

from __init__ import tests

from sugar_network import db, model
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
            'type': ['activity', 'book', 'talks', 'project'],
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

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        post1 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': '', 'vote': 1})
        post2 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': '', 'vote': 2})
        self.assertEqual([2, 3], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual([1, 2], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual([0, 0], volume['context'][context]['rating'])

    def test_DoNotShiftRatingOnZeroVotes(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        post1 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        post2 = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual([0, 0], volume['context'][context]['rating'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual([0, 0], volume['context'][context]['rating'])

    def test_ShiftTopicRating(self):
        volume = db.Volume('db2', [Context, Post])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
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

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })
        topic = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

        post1 = this.call(method='POST', path=['post'], content={'context': context, 'topic': topic, 'type': 'post', 'title': '', 'message': '', 'vote': 1})
        post2 = this.call(method='POST', path=['post'], content={'context': context, 'topic': topic, 'type': 'post', 'title': '', 'message': '', 'vote': 2})
        self.assertEqual([2, 3], volume['post'][topic]['rating'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual([1, 2], volume['post'][topic]['rating'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual([0, 0], volume['post'][topic]['rating'])

    def test_ContextExistance(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['talks'],
            'title': {},
            'summary': {},
            'description': {},
            })

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': 'absent', 'type': 'topic', 'title': '', 'message': ''})
        assert this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

        volume['context'].update(context, {'state': 'deleted'})
        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

    def test_InappropriateType(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['talks'],
            'title': {},
            'summary': {},
            'description': {},
            })

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'poll', 'title': '', 'message': ''})
        assert this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

    def test_InappropriateRelation(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['talks'],
            'title': {},
            'summary': {},
            'description': {},
            })
        topic = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'title': '', 'message': ''})
        assert this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'topic': topic, 'title': '', 'message': ''})

    def test_DefaultResolution(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        topic = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'topic', 'title': '', 'message': ''})
        self.assertEqual('', volume['post'][topic]['resolution'])

        issue = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'issue', 'title': '', 'message': ''})
        self.assertEqual('unconfirmed', volume['post'][issue]['resolution'])

        poll = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'poll', 'title': '', 'message': ''})
        self.assertEqual('open', volume['post'][poll]['resolution'])

    def test_InappropriateResolution(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })

        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'poll', 'title': '', 'message': '', 'resolution': 'open'})

        topic = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'issue', 'title': '', 'message': ''})
        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'open', 'title': '', 'message': ''})
        this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'resolved', 'title': '', 'message': ''})
        self.assertEqual('resolved', volume['post'][topic]['resolution'])

        topic = this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'poll', 'title': '', 'message': ''})
        self.assertRaises(http.BadRequest, this.call, method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'resolved', 'title': '', 'message': ''})
        this.call(method='POST', path=['post'],
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'closed', 'title': '', 'message': ''})
        self.assertEqual('closed', volume['post'][topic]['resolution'])

    def test_ForbiddenIssueResolution(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        context = this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'type': ['activity', 'book', 'talks', 'project'],
            'title': '',
            'summary': '',
            'description': '',
            })
        topic = this.call(method='POST', path=['post'], environ=auth_env(tests.UID2), content={
            'context': context,
            'type': 'issue',
            'title': '',
            'message': '',
            })

        self.assertRaises(http.Forbidden, this.call, method='POST', path=['post'], environ=auth_env(tests.UID2),
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'resolved', 'title': '', 'message': ''})
        this.call(method='POST', path=['post'], environ=auth_env(tests.UID),
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'resolved', 'title': '', 'message': ''})
        self.assertEqual('resolved', volume['post'][topic]['resolution'])

    def test_ForbiddenPollResolution(self):
        volume = self.start_master()
        volume['user'].create({'guid': tests.UID, 'name': 'user', 'pubkey': tests.PUBKEY})
        volume['user'].create({'guid': tests.UID2, 'name': 'user2', 'pubkey': tests.PUBKEY2})

        context = this.call(method='POST', path=['context'], environ=auth_env(tests.UID), content={
            'type': ['activity', 'book', 'talks', 'project'],
            'title': '',
            'summary': '',
            'description': '',
            })
        topic = this.call(method='POST', path=['post'], environ=auth_env(tests.UID2), content={
            'context': context,
            'type': 'poll',
            'title': '',
            'message': '',
            })

        self.assertRaises(http.Forbidden, this.call, method='POST', path=['post'], environ=auth_env(tests.UID),
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'closed', 'title': '', 'message': ''})
        this.call(method='POST', path=['post'], environ=auth_env(tests.UID2),
                content={'context': context, 'type': 'post', 'topic': topic, 'resolution': 'closed', 'title': '', 'message': ''})
        self.assertEqual('closed', volume['post'][topic]['resolution'])

    def test_ShiftReplies(self):
        volume = db.Volume('.', [Context, Post])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
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
        self.assertEqual(0, volume['post'][topic]['replies'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            })
        self.assertEqual(1, volume['post'][topic]['replies'])

        volume['post'].create({
            'context': context,
            'topic': topic,
            'type': 'post',
            'title': {},
            'message': {},
            })
        self.assertEqual(2, volume['post'][topic]['replies'])

    def test_ShiftRepliesOnDeletes(self):
        volume = self.start_master(auth=RootAuth())

        context = volume['context'].create({
            'type': ['activity', 'book', 'talks', 'project'],
            'title': {},
            'summary': {},
            'description': {},
            })
        topic = this.call(method='POST', path=['post'], content={'context': context, 'type': 'topic', 'title': '', 'message': ''})

        post1 = this.call(method='POST', path=['post'], content={'context': context, 'topic': topic, 'type': 'post', 'title': '', 'message': ''})
        post2 = this.call(method='POST', path=['post'], content={'context': context, 'topic': topic, 'type': 'post', 'title': '', 'message': ''})
        self.assertEqual(2, volume['post'][topic]['replies'])

        this.call(method='DELETE', path=['post', post1])
        self.assertEqual(1, volume['post'][topic]['replies'])

        this.call(method='DELETE', path=['post', post2])
        self.assertEqual(0, volume['post'][topic]['replies'])


def auth_env(uid):
    key = RSA.load_key(join(tests.root, 'data', uid))
    nonce = int(time.time() + 2)
    data = hashlib.sha1('%s:%s' % (uid, nonce)).digest()
    signature = key.sign(data).encode('hex')
    authorization = 'Sugar username="%s",nonce="%s",signature="%s"' % \
            (uid, nonce, signature)
    return {'HTTP_AUTHORIZATION': authorization}


if __name__ == '__main__':
    tests.main()
