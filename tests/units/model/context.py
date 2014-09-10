#!/usr/bin/env python
# sugar-lint: disable

import hashlib
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db
from sugar_network.db import blobs
from sugar_network.client import IPCConnection, Connection
from sugar_network.model.context import Context
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request
from sugar_network.toolkit.sugar import color_svg
from sugar_network.toolkit import svg_to_png, i18n, http, coroutine, enforce


class ContextTest(tests.Test):

    def test_ContextImages(self):
        volume = self.start_master()
        conn = Connection()

        guid = conn.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        assert conn.request('GET', ['context', guid, 'artefact_icon']).content == file(volume.blobs.get('assets/missing.svg').path).read()
        assert conn.request('GET', ['context', guid, 'icon']).content == file(volume.blobs.get('assets/missing.png').path).read()
        assert conn.request('GET', ['context', guid, 'logo']).content == file(volume.blobs.get('assets/missing-logo.png').path).read()


if __name__ == '__main__':
    tests.main()
