#!/usr/bin/env python
# sugar-lint: disable

from cStringIO import StringIO

from __init__ import tests

from sugar_network.toolkit import http
from sugar_network.node import obs


class ObsTest(tests.Test):

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

    def test_get_presolve_repos(self):
        self.override(http, 'Client', Client(self, [
            (('GET', ['build', 'resolve']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="Debian-6.0" />',
                '   <entry name="Fedora-11" />',
                '</directory>',
                ]),
            (('GET', ['build', 'resolve', 'Debian-6.0']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="i586" />',
                '   <entry name="x86_64" />',
                '</directory>',
                ]),
            (('GET', ['build', 'resolve', 'Fedora-11']), {'allowed': (400, 404)}, [
                '<directory>',
                '   <entry name="i586" />',
                '</directory>',
                ]),
            ]))

        self.assertEqual([
            {'name': 'Debian-6.0', 'arch': 'i586'},
            {'name': 'Debian-6.0', 'arch': 'x86_64'},
            {'name': 'Fedora-11', 'arch': 'i586'},
            ],
            obs.get_presolve_repos())

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
        self.override(http, 'Client', Client(self, [
            (('GET', ['resolve']),
                {'allowed': (400, 404), 'params': {
                    'project': 'resolve',
                    'repository': 'repo',
                    'arch': 'arch',
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
                    'project': 'resolve',
                    'repository': 'repo',
                    'arch': 'arch',
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
            ]))

        self.assertEqual([
            {'distributor_id': 'Fedora', 'url': 'http://pkg1-1.prm', 'name': 'pkg1-1'},
            {'distributor_id': 'Fedora', 'url': 'http://pkg1-2.prm', 'name': 'pkg1-2'},
            {'distributor_id': 'Fedora', 'url': 'http://pkg2-1.prm', 'name': 'pkg2-1'},
            {'distributor_id': 'Fedora', 'url': 'http://pkg2-2.prm', 'name': 'pkg2-2'},
            ],
            obs.presolve('repo', 'arch', ['pkg1', 'pkg2']))


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

    def __call__(self, url):
        return self


if __name__ == '__main__':
    tests.main()
