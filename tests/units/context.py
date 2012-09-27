#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.node import obs
from sugar_network import IPCClient, Client
from active_toolkit import coroutine, enforce


class ContextTest(tests.Test):

    def test_Aliases(self):
        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'get_presolve_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arch': 'x86'},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arch': 'x86'},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arch': 'x86_64'},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: ['fake'])
        self.override(obs, 'presolve', lambda repo, arch, names: ['%s-%s-%s' % (repo, arch, i) for i in names])

        self.start_server()
        client = IPCClient(mountpoint='~')

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        client.put(['context', guid, 'aliases'], {
            'Gentoo': {
                '*': {
                    'binary': ['pkg1.bin', 'pkg2.bin'],
                    'devel': ['pkg3.devel'],
                    },
                },
            'Debian': {
                '*': {
                    'binary': ['pkg4.bin'],
                    'devel': ['pkg5.devel', 'pkg6.devel'],
                    },
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'success', 'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
            'Debian-6.0': {'status': 'success', 'binary': ['pkg4.bin'], 'devel': ['pkg5.devel', 'pkg6.devel']},
            'Debian-7.0': {'status': 'success', 'binary': ['pkg4.bin'], 'devel': ['pkg5.devel', 'pkg6.devel']},
            },
            client.get(['context', guid, 'packages']))
        self.assertEqual({
            'Gentoo-2.1': {'status': 'success', 'binary': ['Gentoo-2.1-x86-pkg1.bin', 'Gentoo-2.1-x86-pkg2.bin'], 'devel': ['Gentoo-2.1-x86-pkg3.devel']},
            'Debian-6.0': {'status': 'success', 'binary': ['Debian-6.0-x86-pkg4.bin'], 'devel': ['Debian-6.0-x86-pkg5.devel', 'Debian-6.0-x86-pkg6.devel']},
            'Debian-7.0': {'status': 'success', 'binary': ['Debian-7.0-x86_64-pkg4.bin'], 'devel': ['Debian-7.0-x86_64-pkg5.devel', 'Debian-7.0-x86_64-pkg6.devel']},
            },
            client.get(['context', guid, 'presolve']))

    def test_WrongAliases(self):
        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'get_presolve_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arch': 'x86'},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arch': 'x86'},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arch': 'x86_64'},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, 'resolve failed'))
        self.override(obs, 'presolve', lambda repo, arch, names: enforce(False, 'presolve failed'))

        self.start_server()
        client = IPCClient(mountpoint='~')

        guid = client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        client.put(['context', guid, 'aliases'], {
            'Gentoo': {
                '*': {
                    'binary': ['pkg1.bin', 'pkg2.bin'],
                    'devel': ['pkg3.devel'],
                    },
                },
            'Debian': {
                '*': {
                    'binary': ['pkg4.bin'],
                    'devel': ['pkg5.devel', 'pkg6.devel'],
                    },
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'resolve failed'},
            'Debian-6.0': {'status': 'resolve failed'},
            'Debian-7.0': {'status': 'resolve failed'},
            },
            client.get(['context', guid, 'packages']))
        self.assertEqual({
            'Gentoo-2.1': {'status': 'presolve failed'},
            'Debian-6.0': {'status': 'presolve failed'},
            'Debian-7.0': {'status': 'presolve failed'},
            },
            client.get(['context', guid, 'presolve']))

    def test_PackagesRoute(self):
        self.override(obs, 'get_presolve_repos', lambda: [
            {'name': 'Gentoo-2.1', 'arch': 'x86'},
            {'name': 'Debian-6.0', 'arch': 'x86_64'},
            ])

        volume = self.start_master()
        client = Client()

        client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package1',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'binary': ['package1-1', 'package1-2']},
                'Debian-6.0': {'status': 'success', 'binary': ['package1-3']},
                },
            })
        client.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package2',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'devel': ['package2-1', 'package2-2']},
                'Debian-6.0': {'status': 'success', 'devel': ['package2-3']},
                },
            })
        client.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'implement': 'package3',
            'presolve': {
                'Gentoo-2.1': {'status': 'success', 'binary': ['package3-1', 'package3-2']},
                'Debian-6.0': {'status': 'success', 'binary': ['package3-3']},
                },
            })

        self.assertEqual(
                {'total': 2, 'result': [{'arch': 'x86', 'name': 'Gentoo-2.1'}, {'arch': 'x86_64', 'name': 'Debian-6.0'}]},
                client.get(['packages']))
        self.assertEqual(
                {'total': 2, 'result': ['package1', 'package2']},
                client.get(['packages', 'Gentoo-2.1']))
        self.assertEqual(
                ['package1-1', 'package1-2'],
                client.get(['packages', 'Gentoo-2.1', 'package1']))
        self.assertEqual(
                ['package1-3'],
                client.get(['packages', 'Debian-6.0', 'package1']))
        self.assertRaises(RuntimeError, client.request, 'GET', ['packages', 'Debian-6.0', 'package2'])
        self.assertRaises(RuntimeError, client.request, 'GET', ['packages', 'Gentoo-2.1', 'package3'])


if __name__ == '__main__':
    tests.main()
