#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests

from active_document.env import Seqno


class EnvTest(tests.Test):

    def test_Seqno_commit(self):
        seqno = Seqno('seqno')

        self.assertEqual(False, seqno.commit())
        assert not exists('seqno')

        seqno.next()
        self.assertEqual(True, seqno.commit())
        assert exists('seqno')
        self.assertEqual(False, seqno.commit())
        seqno.next()

        seqno = Seqno('seqno')
        self.assertEqual(1, seqno.value)
        self.assertEqual(False, seqno.commit())


if __name__ == '__main__':
    tests.main()
