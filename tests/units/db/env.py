#!/usr/bin/env python
# sugar-lint: disable

import copy
from os.path import exists

from __init__ import tests

from sugar_network import toolkit
from sugar_network.db import env


class EnvTest(tests.Test):

    def test_gettext(self):
        # Fallback to default lang
        toolkit._default_lang = 'default'
        self.assertEqual('foo', env.gettext({'lang': 'foo', 'default': 'bar'}, 'lang'))
        self.assertEqual('bar', env.gettext({'lang': 'foo', 'default': 'bar'}, 'fake'))

        # Exact accept_language
        self.assertEqual('', env.gettext(None, 'lang'))
        self.assertEqual('foo', env.gettext('foo', 'lang'))
        self.assertEqual('foo', env.gettext({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, 'lang'))
        self.assertEqual('foo', env.gettext({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, ['lang', 'fake']))
        self.assertEqual('bar', env.gettext({'lang': 'foo', 'fake': 'bar', 'default': 'default'}, ['fake', 'lang']))

        # Last resort
        self.assertEqual('foo', env.gettext({'1': 'foo', '2': 'bar'}, 'fake'))

        # Primed accept_language
        self.assertEqual('foo', env.gettext({'1': 'foo', '2': 'bar', 'default': 'default'}, '1-a'))

        # Primed i18n value
        self.assertEqual('bar', env.gettext({'1-a': 'foo', '1': 'bar', 'default': 'default'}, '1-b'))
        self.assertEqual('foo', env.gettext({'1-a': 'foo', '2': 'bar', 'default': 'default'}, '1-b'))

    def test_gettext_EnAsTheLastResort(self):
        toolkit._default_lang = 'en-us'
        self.assertEqual('right', env.gettext({'a': 'wrong', 'en': 'right'}, 'probe'))
        self.assertEqual('exact', env.gettext({'a': 'wrong', 'en': 'right', 'probe': 'exact'}, 'probe'))


if __name__ == '__main__':
    tests.main()
