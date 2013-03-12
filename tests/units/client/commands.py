#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network import db, client
from sugar_network.client import journal, injector
from sugar_network.client.commands import ClientCommands
from sugar_network.resources.volume import Volume
from sugar_network.toolkit.router import IPCRouter
from sugar_network.toolkit import coroutine

import requests


class CommandsTest(tests.Test):

    def test_Hub(self):
        volume = Volume('db')
        cp = ClientCommands(volume, offline=True)
        server = coroutine.WSGIServer(
                ('localhost', client.ipc_port.value), IPCRouter(cp))
        coroutine.spawn(server.serve_forever)
        coroutine.dispatch()

        url = 'http://localhost:%s' % client.ipc_port.value

        response = requests.request('GET', url + '/hub', allow_redirects=False)
        self.assertEqual(303, response.status_code)
        self.assertEqual('/hub/', response.headers['Location'])

        client.hub_root.value = '.'
        index_html = '<html><body>index</body></html>'
        self.touch(('index.html', index_html))

        response = requests.request('GET', url + '/hub', allow_redirects=True)
        self.assertEqual(index_html, response.content)

        response = requests.request('GET', url + '/hub/', allow_redirects=False)
        self.assertEqual(index_html, response.content)

    def test_launch(self):
        self.override(injector, 'launch', lambda *args, **kwargs: [{'args': args, 'kwargs': kwargs}])
        volume = Volume('db')
        cp = ClientCommands(volume, offline=True)

        self.assertRaises(RuntimeError, cp.launch, 'fake-document', 'app', [])

        cp.launch('context', 'app', [])
        self.assertEqual(
                {'event': 'launch', 'args': ['app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': None}},
                self.wait_for_events(cp, event='launch').wait())

    def test_launch_ResumeJobject(self):
        self.override(injector, 'launch', lambda *args, **kwargs: [{'args': args, 'kwargs': kwargs}])
        self.override(journal, 'exists', lambda *args: True)
        volume = Volume('db')
        cp = ClientCommands(volume, offline=True)

        cp.launch('context', 'app', [], object_id='object_id')
        self.assertEqual(
                {'event': 'launch', 'args': ['app', []], 'kwargs': {'color': None, 'activity_id': None, 'uri': None, 'object_id': 'object_id'}},
                self.wait_for_events(cp, event='launch').wait())


if __name__ == '__main__':
    tests.main()
