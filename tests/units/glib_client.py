#!/usr/bin/env python
# sugar-lint: disable

import gobject

from __init__ import tests

from sugar_network_server.resources.user import User
from sugar_network_server.resources.context import Context

import active_document as ad
from active_document import coroutine
from sugar_network.client import Client
from sugar_network.glib_client import Client as GlibClient
from local_document.bus import Server
from local_document.mounts import Mounts


class GlibClientTest(tests.Test):

    def test_Find(self):
        self.start_server()
        client = Client('~')
        mainloop = gobject.MainLoop()

        guid_1 = client.Context(type='activity', title='title-1', summary='summary', description='description').post()
        guid_2 = client.Context(type='activity', title='title-2', summary='summary', description='description').post()
        guid_3 = client.Context(type='activity', title='title-3', summary='summary', description='description').post()
        guid_4 = client.Context(type='activity', title='title-4', summary='summary', description='description').post()
        guid_5 = client.Context(type='activity', title='title-5', summary='summary', description='description').post()

        client = GlibClient()
        self.assertEqual([
            (guid_1, 'title-1'),
            (guid_2, 'title-2'),
            (guid_3, 'title-3'),
            (guid_4, 'title-4'),
            (guid_5, 'title-5'),
            ],
            [(i['guid'], i['title']) for i in client.find('~', 'context', 0, 2, ['guid', 'title'])])

    def test_Updates(self):
        self.start_server()

        client = Client('~')
        glib_client = GlibClient()

        guid = client.Context(type='activity', title='title-1', summary='summary', description='description').post()
        self.assertEqual(
                {'guid': guid, 'title': 'title-1'},
                glib_client.get('~', 'context', guid, reply=['guid', 'title']))

        glib_client.update('~', 'context', guid, title='title-2')
        self.assertEqual(
                {'guid': guid, 'title': 'title-2'},
                glib_client.get('~', 'context', guid, reply=['guid', 'title']))

    def test_Events(self):
        self.fork(self.restful_server)
        coroutine.sleep(1)

        self.start_server()
        client = Client('~')
        mainloop = gobject.MainLoop()

        events = []
        glib_client = GlibClient()
        glib_client.connect('connect',
                lambda sender, *args: events.append(('connect',) + args))
        glib_client.connect('keep',
                lambda sender, *args: events.append(('keep',) + args))
        glib_client.connect('keep_impl',
                lambda sender, *args: events.append(('keep_impl',) + args))
        gobject.idle_add(mainloop.quit)
        mainloop.run()
        coroutine.sleep(1)

        guid = client.Context(
                type='activity',
                title='title',
                summary='summary',
                description='description').post()
        client.Context(guid, keep=True).post()
        client.Context(guid, keep=False).post()
        client.Context(guid, keep=True).post()
        client.Context(guid, keep_impl=1).post()
        client.Context(guid, keep_impl=2).post()
        client.Context(guid, keep_impl=0).post()
        client.Context.delete(guid)

        gobject.timeout_add(1000, mainloop.quit)
        mainloop.run()

        self.assertEqual([
            ('connect', '/', True),
            ('keep_impl', guid, False),
            ('keep', guid, False),
            ('keep', guid, True),
            ('keep', guid, False),
            ('keep', guid, True),
            ('keep_impl', guid, True),
            ('keep_impl', guid, False),
            ('keep_impl', guid, False),
            ],
            events)


if __name__ == '__main__':
    tests.main()
