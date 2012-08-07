#!/usr/bin/env python
# sugar-lint: disable

import os
import sys
from os.path import abspath

arg0 = abspath(__file__)

import dbus
import gobject
from dbus.mainloop.glib import threads_init, DBusGMainLoop

from __init__ import tests

from sugar_network import DBusClient
from sugar_network.local.dbus_network import Network
from sugar_network.toolkit import dbus_thread
from active_toolkit import coroutine


gobject.threads_init()
threads_init()
DBusGMainLoop(set_as_default=True)


class DbusClientTest(tests.Test):

    def setUp(self, fork_num=0):
        tests.Test.setUp(self, fork_num)

        if fork_num:
            return

        self.fork(os.execvp, arg0, [arg0, self.id().split('.')[-1], 'fork'])
        coroutine.sleep(1)

    def test_Call(self):
        client = DBusClient(mountpoint='~')

        self.assertEqual(True, client('GET', 'mounted'))

        guid = client('POST', document='context', content={
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual(
                ['activity'],
                client('GET', document='context', guid=guid, prop='type'))
        self.assertEqual(
                'title',
                client('GET', document='context', guid=guid, prop='title'))

        client('PUT', document='context', guid=guid, content={
            'title': 'title-2',
            })
        self.assertEqual(
                'title-2',
                client('GET', document='context', guid=guid, prop='title'))

        client('DELETE', document='context', guid=guid)
        self.assertRaises(Exception, client, 'GET', document='context', guid=guid, prop='type')


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[-1] == 'fork':
        self = DbusClientTest(sys.argv[1])
        self.setUp(fork_num=1)
        self.create_mountset()
        dbus_thread.spawn_service(Network)
        dbus_thread.start(self.mounts)
    else:
        tests.main()
