#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.client import Client
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.review import Review
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.solution import Solution
from sugar_network.resources.comment import Comment


class CommentTest(tests.Test):

    def test_SetContext(self):
        volume = self.start_master([User, Context, Review, Feedback, Solution, Comment])
        client = Client()

        self.assertRaises(RuntimeError, client.post, ['comment'], {'message': ''})

        context = client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        review = client.post(['review'], {
            'context': context,
            'title': 'title',
            'content': 'content',
            'rating': 5,
            })
        comment = client.post(['comment'], {
            'review': review,
            'message': '',
            })
        self.assertEqual(
                context,
                client.get(['comment', comment, 'context']))

        feedback = client.post(['feedback'], {
            'context': context,
            'type': 'idea',
            'title': 'title',
            'content': 'content',
            })
        comment = client.post(['comment'], {
            'feedback': feedback,
            'message': '',
            })
        self.assertEqual(
                context,
                client.get(['comment', comment, 'context']))

        solution = client.post(['solution'], {
            'feedback': feedback,
            'content': 'content',
            })
        comment = client.post(['comment'], {
            'solution': solution,
            'message': '',
            })
        self.assertEqual(
                context,
                client.get(['comment', comment, 'context']))


if __name__ == '__main__':
    tests.main()
