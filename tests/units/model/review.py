#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.client import Connection, keyfile
from sugar_network.model.user import User
from sugar_network.model.context import Context
from sugar_network.model.review import Review
from sugar_network.model.artifact import Artifact
from sugar_network.model.implementation import Implementation
from sugar_network.toolkit import http


class ReviewTest(tests.Test):

    def test_SetContext(self):
        volume = self.start_master([User, Context, Review, Artifact, Implementation])
        client = Connection(auth=http.SugarAuth(keyfile.value))

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
