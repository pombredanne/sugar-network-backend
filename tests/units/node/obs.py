#!/usr/bin/env python
# sugar-lint: disable

import json
from cStringIO import StringIO

from __init__ import tests

from sugar_network import node
from sugar_network.toolkit import http
from sugar_network.node import obs


class ObsTest(tests.Test):

    def setUp(self):
        tests.Test.setUp(self)
        obs._repos = {}

    def test_get_repos(self):
        self.override(http, 'Client', Client(self, [
            (('GET', ['build', 'base']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="Debian-6.0" />',
                '   <entry name="Fedora-11" />',
                '</directory>',
                ]),
            (('GET', ['build', 'base', 'Debian-6.0']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="i586" />',
                '   <entry name="x86_64" />',
                '</directory>',
                ]),
            (('GET', ['build', 'base', 'Fedora-11']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="i586" />',
                '</directory>',
                ]),
            ]))

        self.assertEqual([
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arches': ['i586', 'x86_64']},
            {'distributor_id': 'Fedora', 'name': 'Fedora-11', 'arches': ['i586']},
            ],
            obs.get_repos())

    def test_resolve(self):
        self.override(http, 'Client', Client(self, [
            (('GET', ['resolve']),
                {'allowed': (400, 404), 'params': {
                    'project': 'base',
                    'repository': 'repo',
                    'arch': 'arch',
                    'package': 'pkg1',
                    }},
                [   '<resolve>',
                    '   <binary name="pygame" url="http://pkg1.prm" />',
                    '</resolve>',
                    ],
                ),
            (('GET', ['resolve']),
                {'allowed': (400, 404), 'params': {
                    'project': 'base',
                    'repository': 'repo',
                    'arch': 'arch',
                    'package': 'pkg2',
                    }},
                [   '<resolve>',
                    '   <binary name="pygame" url="http://pkg2.prm" />',
                    '</resolve>',
                    ],
                ),
            ]))

        obs.resolve('repo', 'arch', ['pkg1', 'pkg2'])

    def test_presolve(self):
        self.override(http, 'Client', lambda *args: Client(self, [
            (('GET', ['build', 'presolve']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="OLPC-11.3.1" />',
                '</directory>',
                ]),
            (('GET', ['build', 'presolve', 'OLPC-11.3.1']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="i586" />',
                '</directory>',
                ]),
            (('GET', ['resolve']),
                {'allowed': (400, 404), 'params': {
                    'project': 'presolve',
                    'repository': 'OLPC-11.3.1',
                    'arch': 'i586',
                    'package': 'pkg1',
                    'withdeps': '1',
                    'exclude': 'sugar',
                    }},
                [   '<resolve>',
                    '   <binary name="pkg1-1" url="http://pkg1-1.prm" />',
                    '   <binary name="pkg1-2" url="http://pkg1-2.prm" />',
                    '</resolve>',
                    ],
                ),
            (('GET', ['resolve']),
                {'allowed': (400, 404), 'params': {
                    'project': 'presolve',
                    'repository': 'OLPC-11.3.1',
                    'arch': 'i586',
                    'package': 'pkg2',
                    'withdeps': '1',
                    'exclude': 'sugar',
                    }},
                [   '<resolve>',
                    '   <binary name="pkg2-1" url="http://pkg2-1.prm" />',
                    '   <binary name="pkg2-2" url="http://pkg2-2.prm" />',
                    '</resolve>',
                    ],
                ),
            ('http://pkg1-1.prm', ['1']),
            ('http://pkg1-2.prm', ['2']),
            ('http://pkg2-1.prm', ['3']),
            ('http://pkg2-2.prm', ['4']),
            ]))

        obs.presolve({
            'Debian': {'binary': [['deb']]},
            'Fedora': {'binary': [['pkg1', 'pkg2']], 'devel': [['pkg3']]},
            }, '.')

        self.assertEqual([
            {'url': 'http://pkg1-1.prm', 'name': 'pkg1-1'},
            {'url': 'http://pkg1-2.prm', 'name': 'pkg1-2'},
            ],
            json.load(file('presolve/OLPC-11.3.1/i586/pkg1')))
        self.assertEqual([
            {'url': 'http://pkg2-1.prm', 'name': 'pkg2-1'},
            {'url': 'http://pkg2-2.prm', 'name': 'pkg2-2'},
            ],
            json.load(file('presolve/OLPC-11.3.1/i586/pkg2')))
        self.assertEqual('1', file('packages/OLPC-11.3.1/i586/pkg1-1.prm').read())
        self.assertEqual('2', file('packages/OLPC-11.3.1/i586/pkg1-2.prm').read())
        self.assertEqual('3', file('packages/OLPC-11.3.1/i586/pkg2-1.prm').read())
        self.assertEqual('4', file('packages/OLPC-11.3.1/i586/pkg2-2.prm').read())


class Response(object):

    headers = {'Content-Type': 'text/xml'}
    raw = None
    status_code = 200


class Client(object):

    def __init__(self, test, calls):
        self.test = test
        self.calls = calls[:]

    def request(self, *args, **kwargs):
        assert self.calls
        args_, kwargs_, reply = self.calls.pop(0)
        self.test.assertEqual((args_, kwargs_), (args, kwargs))
        response = Response()
        response.raw = StringIO(''.join(reply))
        return response

    def download(self, path, dst):
        assert self.calls
        path_, reply = self.calls.pop(0)
        self.test.assertEqual(path_, path)
        if isinstance(dst, basestring):
            with file(dst, 'wb') as f:
                f.write(''.join(reply))
        else:
            dst.write(''.join(reply))

    def __call__(self, url):
        return self


if __name__ == '__main__':
    tests.main()
