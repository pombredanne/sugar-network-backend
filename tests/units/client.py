#!/usr/bin/env python
# sugar-lint: disable

from os.path import exists

from __init__ import tests

import active_document as ad
from sugar_network.client.mounts import LocalMount
from sugar_network.resources.volume import Volume, Request
from sugar_network.toolkit import sugar


class LocalTest(tests.Test):

    def test_HandleDeletes(self):
        cp = LocalMount(Volume('db'))

        request = Request(method='POST', document='context')
        request.content = {
                'type': 'activity',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                }
        guid = cp.call(request, ad.Response())
        guid_path = 'db/context/%s/%s' % (guid[:2], guid)

        assert exists(guid_path)

        request = Request(method='DELETE', document='context', guid=guid)
        cp.call(request, ad.Response())

        self.assertRaises(ad.NotFound, lambda: cp.volume['context'].get(guid).exists)
        assert not exists(guid_path)

    def test_SetUser(self):
        cp = LocalMount(Volume('db'))

        request = Request(method='POST', document='context')
        request.principal = 'uid'
        request.content = {
                'type': 'activity',
                'title': 'title',
                'summary': 'summary',
                'description': 'description',
                }
        guid = cp.call(request, ad.Response())

        self.assertEqual(
                {'uid': 1},
                cp.volume['context'].get(guid)['authority'])


if __name__ == '__main__':
    tests.main()
