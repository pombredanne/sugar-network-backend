#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import base64
import mimetypes

from __init__ import tests

from sugar_network import db
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node.model import User
from sugar_network.toolkit.coroutine import this


class ModelTest(tests.Test):

    def test_RatingSort(self):
        this.localcast = lambda event: None
        directory = db.Volume('db', [Post])['post']

        directory.create({'guid': '1', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [0, 0]})
        directory.create({'guid': '2', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [1, 2]})
        directory.create({'guid': '3', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [1, 4]})
        directory.create({'guid': '4', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [10, 10]})
        directory.create({'guid': '5', 'context': '', 'type': 'post', 'title': {}, 'message': {}, 'rating': [30, 90]})

        self.assertEqual(
                ['1', '2', '3', '4', '5'],
                [i.guid for i in directory.find()[0]])
        self.assertEqual(
                ['1', '4', '2', '5', '3'],
                [i.guid for i in directory.find(order_by='rating')[0]])
        self.assertEqual(
                ['3', '5', '2', '4', '1'],
                [i.guid for i in directory.find(order_by='-rating')[0]])

if __name__ == '__main__':
    tests.main()
