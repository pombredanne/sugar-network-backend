#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network.client.mounts import LocalMount
from sugar_network.resources.volume import Volume
from sugar_network.toolkit import sugar


class LocalTest(tests.Test):

    def test_HandleDeletes(self):
        cp = LocalMount(Volume('db'))

        request = ad.Request(method='POST', document='context')
        request.content = {
                'type': 'activity',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                }
        guid = cp.call(request, ad.Response())
        guid_path = 'db/context/%s/%s' % (guid[:2], guid)

        assert exists(guid_path)

        request = ad.Request(method='DELETE', document='context', guid=guid)
        cp.call(request, ad.Response())

        self.assertRaises(ad.NotFound, lambda: cp.volume['context'].get(guid).exists)
        assert not exists(guid_path)

    def test_SetUser(self):
        cp = LocalMount(Volume('db'))

        self.override(sugar, 'uid', lambda: 'uid')
        request = ad.Request(method='POST', document='context')
        request.content = {
                'type': 'activity',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                }
        guid = cp.call(request, ad.Response())

        self.assertEqual(
                ['uid'],
                cp.volume['context'].get(guid)['user'])

    def test_SetAuthor(self):
        cp = LocalMount(Volume('db'))

        self.override(sugar, 'nickname', lambda: 'nickname')
        request = ad.Request(method='POST', document='context')
        request.content = {
                'type': 'activity',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                }
        guid = cp.call(request, ad.Response())

        self.assertEqual(
                ['nickname'],
                cp.volume['context'].get(guid)['author'])


if __name__ == '__main__':
    tests.main()
