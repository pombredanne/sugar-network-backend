#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import Client
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.review import Review
from sugar_network.resources.artifact import Artifact


class ReviewTest(tests.Test):

    def test_SetContext(self):
        volume = self.start_master([User, Context, Review, Artifact])
        client = Client()

        context = client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        artifact = client.post(['artifact'], {
            'type': 'instance',
            'context': context,
            'title': 'title',
            'description': 'description',
            })

        review = client.post(['review'], {
            'context': context,
            'title': 'title',
            'content': 'content',
            'rating': 5,
            })
        self.assertEqual(
                context,
                client.get(['review', review, 'context']))
        self.assertEqual(
                '',
                client.get(['review', review, 'artifact']))

        review = client.post(['review'], {
            'artifact': artifact,
            'title': 'title',
            'content': 'content',
            'rating': 5,
            })
        self.assertEqual(
                context,
                client.get(['review', review, 'context']))
        self.assertEqual(
                artifact,
                client.get(['review', review, 'artifact']))


if __name__ == '__main__':
    tests.main()
