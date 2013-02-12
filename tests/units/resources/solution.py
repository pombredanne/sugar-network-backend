#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.client import Client
from sugar_network.resources.user import User
from sugar_network.resources.context import Context
from sugar_network.resources.feedback import Feedback
from sugar_network.resources.solution import Solution


class SolutionTest(tests.Test):

    def test_SetContext(self):
        volume = self.start_master([User, Context, Feedback, Solution])
        client = Client()

        context = client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        feedback = client.post(['feedback'], {
            'context': context,
            'type': 'idea',
            'title': 'title',
            'content': 'content',
            })
        solution = client.post(['solution'], {
            'feedback': feedback,
            'content': '',
            })

        self.assertEqual(
                context,
                client.get(['solution', solution, 'context']))


if __name__ == '__main__':
    tests.main()
