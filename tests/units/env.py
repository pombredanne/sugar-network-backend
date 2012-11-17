#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests

from active_document import env


class EnvTest(tests.Test):

    def test_Seqno_commit(self):
        seqno = env.Seqno('seqno')

        self.assertEqual(False, seqno.commit())
        assert not exists('seqno')

        seqno.next()
        self.assertEqual(True, seqno.commit())
        assert exists('seqno')
        self.assertEqual(False, seqno.commit())
        seqno.next()

        seqno = env.Seqno('seqno')
        self.assertEqual(1, seqno.value)
        self.assertEqual(False, seqno.commit())

    def test_gettext(self):
        # Fallback to default lang
        env.DEFAULT_LANG = 'default'
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


if __name__ == '__main__':
    tests.main()
