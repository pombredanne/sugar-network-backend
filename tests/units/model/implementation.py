#!/usr/bin/env python
# sugar-lint: disable

import os

import xapian

from __init__ import tests

from sugar_network import db
from sugar_network.model import implementation
from sugar_network.model.implementation import _fmt_version, Implementation
from sugar_network.client import IPCClient
from sugar_network.toolkit import http, coroutine


class ImplementationTest(tests.Test):

    def test_fmt_version(self):
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''5''000')),
                _fmt_version('1'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0002''0000''5''000')),
                _fmt_version('1.2'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0020''0300''5''000')),
                _fmt_version('1.20.300'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0020''0300''5''000')),
                _fmt_version('1.20.300.4444'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''9999''0000''5''000')),
                _fmt_version('10001.99999.10000'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''3''000')),
                _fmt_version('1-pre'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''4''000')),
                _fmt_version('1-rc'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''5''000')),
                _fmt_version('1-'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''000')),
                _fmt_version('1-r'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''3''001')),
                _fmt_version('1-pre1'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''4''002')),
                _fmt_version('1-rc2'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''003')),
                _fmt_version('1-r3'))

        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''000')),
                _fmt_version('1-r-2-3'))
        self.assertEqual(
                xapian.sortable_serialise(eval('1''0000''0000''6''001')),
                _fmt_version('1-r1.2-3'))

    def test_WrongAuthor(self):
        self.start_online_client()
        client = IPCClient()

        self.node_volume['context'].create({
                'guid': 'context',
                'type': 'content',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                'author': {'fake': None},
                })

        impl = {'context': 'context',
                'license': 'GPLv3+',
                'version': '1',
                'stability': 'stable',
                'notes': '',
                }
        self.assertRaises(http.Forbidden, client.post, ['implementation'], impl)
        self.assertEqual(0, self.node_volume['implementation'].find()[1])

        self.node_volume['context'].update('context', {'author': {tests.UID: None}})
        guid = client.post(['implementation'], impl)
        assert self.node_volume['implementation'].exists(guid)


if __name__ == '__main__':
    tests.main()
