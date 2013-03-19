#!/usr/bin/env python
# sugar-lint: disable

from __init__ import tests

from sugar_network.node import obs
from sugar_network.client import IPCClient
from sugar_network.toolkit import coroutine, enforce


class MasterTest(tests.Test):

    def test_Aliases(self):
        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: ['fake'])
        self.override(obs, 'presolve', lambda *args: None)

        self.start_online_client()
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['pkg1.bin', 'pkg2.bin']],
                'devel': [['pkg3.devel']],
                },
            'Debian': {
                'binary': [['pkg4.bin']],
                'devel': [['pkg5.devel', 'pkg6.devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'success', 'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
            'Debian-6.0': {'status': 'success', 'binary': ['pkg4.bin'], 'devel': ['pkg5.devel', 'pkg6.devel']},
            'Debian-7.0': {'status': 'success', 'binary': ['pkg4.bin'], 'devel': ['pkg5.devel', 'pkg6.devel']},
            },
            ipc.get(['context', guid, 'packages']))

    def test_WrongAliases(self):
        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'distributor_id': 'Debian', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'distributor_id': 'Debian', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, 'resolve failed'))
        self.override(obs, 'presolve', lambda *args: None)

        self.start_online_client()
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['pkg1.bin', 'pkg2.bin']],
                'devel': [['pkg3.devel']],
                },
            'Debian': {
                'binary': [['pkg4.bin']],
                'devel': [['pkg5.devel', 'pkg6.devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'resolve failed'},
            'Debian-6.0': {'status': 'resolve failed'},
            'Debian-7.0': {'status': 'resolve failed'},
            },
            ipc.get(['context', guid, 'packages']))

    def test_MultipleAliases(self):

        def resolve(repo, arch, names):
            enforce(not [i for i in names if 'fake' in i], 'resolve failed')
            return ['fake']

        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            ])
        self.override(obs, 'resolve', resolve)
        self.override(obs, 'presolve', lambda *args: None)

        self.start_online_client()
        ipc = IPCClient()

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['fake.bin'], ['proper.bin'], ['not-reach.bin']],
                'devel': [['fake.devel'], ['proper.devel'], ['not-reach.devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'success', 'binary': ['proper.bin'], 'devel': ['proper.devel']},
            },
            ipc.get(['context', guid, 'packages']))

        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['proper.bin']],
                'devel': [['fake.devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'resolve failed', 'binary': ['proper.bin']},
            },
            ipc.get(['context', guid, 'packages']))

        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['fake.bin']],
                'devel': [['proper.devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'resolve failed'},
            },
            ipc.get(['context', guid, 'packages']))

    def test_InvalidateSolutions(self):
        self.override(obs, 'get_repos', lambda: [
            {'distributor_id': 'Gentoo', 'name': 'Gentoo-2.1', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: ['fake'])
        self.override(obs, 'presolve', lambda *args: None)

        self.start_online_client()
        ipc = IPCClient()

        events = []
        def read_events():
            for event in ipc.subscribe():
                if event.get('document') == 'implementation':
                    events.append(event)
        job = coroutine.spawn(read_events)

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        ipc.put(['context', guid, 'aliases'], {
            'Gentoo': {
                'binary': [['bin']],
                'devel': [['devel']],
                },
            })
        coroutine.dispatch()
        self.assertEqual({
            'Gentoo-2.1': {'status': 'success', 'binary': ['bin'], 'devel': ['devel']},
            },
            ipc.get(['context', guid, 'packages']))
        print events
        self.assertEqual(1, len(events))
        assert 'mtime' in events[0]['props']

    def test_InvalidateSolutionsOnDependenciesChanges(self):
        self.start_online_client()
        ipc = IPCClient()

        events = []
        def read_events():
            for event in ipc.subscribe():
                if event.get('document') == 'implementation':
                    events.append(event)
        job = coroutine.spawn(read_events)

        guid = ipc.post(['context'], {
            'type': 'activity',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            'dependencies': [],
            })
        self.assertEqual(0, len(events))

        ipc.put(['context', guid, 'dependencies'], ['foo'])
        self.assertEqual(1, len(events))
        assert 'mtime' in events[0]['props']
        del events[:]


if __name__ == '__main__':
    tests.main()