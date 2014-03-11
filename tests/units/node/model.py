#!/usr/bin/env python
# sugar-lint: disable

import os
import time

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection, keyfile, api
from sugar_network.model.user import User
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node import model, obs
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request, Router
from sugar_network.toolkit import spec, i18n, http, coroutine, enforce


class ModelTest(tests.Test):

    def test_IncrementReleasesSeqno(self):
        events = []
        volume = self.start_master([User, model.Context, Post])
        this.broadcast = lambda x: events.append(x)
        conn = Connection(auth=http.SugarAuth(keyfile.value))

        context = conn.post(['context'], {
            'type': 'group',
            'title': 'Activity',
            'summary': 'summary',
            'description': 'description',
            })
        self.assertEqual([
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(0, volume.releases_seqno.value)

        aggid = conn.post(['context', context, 'releases'], -1)
        self.assertEqual([
            {'event': 'release', 'seqno': 1},
            ], [i for i in events if i['event'] == 'release'])
        self.assertEqual(1, volume.releases_seqno.value)

    def test_Packages(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: {'version': '1.0'})

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', '*'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                'Debian-7.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', 'Gentoo'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            'Gentoo': {
                'seqno': 6,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', 'Debian-6.0'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            'Debian-6.0': {
                'seqno': 8,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Debian-6.0': {'status': 'success', 'packages': ['pkg1.bin', 'pkg2.bin', 'pkg3.devel'], 'version': [[1, 0], 0]},
                },
            },
            volume['context'][guid]['releases'])

    def test_UnresolvedPackages(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            ])
        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, 'resolve failed'))

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))

        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })
        conn.put(['context', guid, 'releases', '*'], {
            'binary': ['pkg1.bin', 'pkg2.bin'],
            'devel': 'pkg3.devel',
            })
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['pkg1.bin', 'pkg2.bin'], 'devel': ['pkg3.devel']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': 'resolve failed'},
                },
            },
            volume['context'][guid]['releases'])

    def test_PackageOverrides(self):
        self.override(obs, 'get_repos', lambda: [
            {'lsb_id': 'Gentoo', 'lsb_release': '2.1', 'name': 'Gentoo-2.1', 'arches': ['x86', 'x86_64']},
            {'lsb_id': 'Debian', 'lsb_release': '6.0', 'name': 'Debian-6.0', 'arches': ['x86']},
            {'lsb_id': 'Debian', 'lsb_release': '7.0', 'name': 'Debian-7.0', 'arches': ['x86_64']},
            ])

        volume = self.start_master([User, model.Context])
        conn = http.Connection(api.value, http.SugarAuth(keyfile.value))
        guid = conn.post(['context'], {
            'type': 'package',
            'title': 'title',
            'summary': 'summary',
            'description': 'description',
            })

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '1'))
        conn.put(['context', guid, 'releases', '*'], {'binary': '1'})
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '1'},
                'Debian-7.0': {'status': '1'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '2'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '2'})
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['2']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '2'},
                'Debian-7.0': {'status': '2'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '3'))
        conn.put(['context', guid, 'releases', 'Debian-6.0'], {'binary': '3'})
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 5,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['2']},
                },
            'Debian-6.0': {
                'seqno': 6,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['3']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '2'},
                },
            },
            volume['context'][guid]['releases'])

        self.override(obs, 'resolve', lambda repo, arch, names: enforce(False, '4'))
        conn.put(['context', guid, 'releases', 'Debian'], {'binary': '4'})
        self.assertEqual({
            '*': {
                'seqno': 4,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['1']},
                },
            'Debian': {
                'seqno': 7,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['4']},
                },
            'Debian-6.0': {
                'seqno': 6,
                'author': {tests.UID: {'name': tests.UID, 'order': 0, 'role': 3}},
                'value': {'binary': ['3']},
                },
            'resolves': {
                'Gentoo-2.1': {'status': '1'},
                'Debian-6.0': {'status': '3'},
                'Debian-7.0': {'status': '4'},
                },
            },
            volume['context'][guid]['releases'])

    def test_solve_SortByVersions(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 3), 'blob': '3', 'version': [[3], 0]}},
                model.solve(volume, context))

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 3), 'blob': '3', 'version': [[3], 0]}},
                model.solve(volume, context))

    def test_solve_SortByStability(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'developer', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'buggy', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': ('activity', 2), 'blob': '2', 'version': [[2], 0]}},
                model.solve(volume, context))

    def test_solve_CollectDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {}}, 'stability': 'stable',
                    'version': [[1], 0],
                    'requires': spec.parse_requires('context2; context4'),
                    'commands': {'activity': {'exec': 'command'}},
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'bundles': {'*-*': {}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'blob': '1', 'version': [[1], 0], 'command': ('activity', 'command')},
            'context2': {'blob': '2', 'version': [[2], 0]},
            'context3': {'blob': '3', 'version': [[3], 0]},
            'context4': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context1'))

    def test_solve_CommandDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {}}, 'stability': 'stable',
                    'version': [[1], 0],
                    'requires': [],
                    'commands': {
                        'activity': {'exec': 1, 'requires': spec.parse_requires('context2')},
                        'application': {'exec': 2},
                        },
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'bundles': {'*-*': {}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': [],
                    }},
                },
            })

        self.assertEqual({
            'context1': {'blob': '1', 'version': [[1], 0], 'command': ('activity', 1)},
            'context2': {'blob': '2', 'version': [[2], 0]},
            },
            model.solve(volume, 'context1', command='activity'))
        self.assertEqual({
            'context1': {'blob': '1', 'version': [[1], 0], 'command': ('application', 2)},
            },
            model.solve(volume, 'context1', command='application'))

    def test_solve_DepConditions(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep < 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '2', 'version': [[2], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '3', 'version': [[3], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '5', 'version': [[5], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep >= 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '5', 'version': [[5], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep < 5'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '4', 'version': [[4], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '3', 'version': [[3], 0]},
                },
                model.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep = 1'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
                'dep': {'blob': '1', 'version': [[1], 0]},
                },
                model.solve(volume, 'context1'))

    def test_solve_SwitchToAlternativeBranch(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '6': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=1'), 'commands': {'activity': {'exec': 6}}}},
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context2'), 'commands': {'activity': {'exec': 1}}}},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3; context4=1')}},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=2')}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'blob': '6', 'version': [[1], 0], 'command': ('activity', 6)},
            'context4': {'blob': '5', 'version': [[1], 0]},
            },
            model.solve(volume, 'context1'))

    def test_solve_CommonDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep=2',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            'dep': {'blob': '2', 'version': [[2], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<5',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            'dep': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<4',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            'dep': {'blob': '3', 'version': [[3], 0]},
            },
            model.solve(volume, 'context'))

    def test_solve_ExtraDeps(self):
        volume = db.Volume('master', [Context])
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            'dep': {'blob': '5', 'version': [[5], 0]},
            },
            model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'blob': '10', 'version': [[1], 0], 'command': ('activity', 'command')},
            'dep': {'blob': '4', 'version': [[4], 0]},
            },
            model.solve(volume, 'context'))

    def test_solve_Nothing(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep=0'),
                    }},
                },
            })
        self.assertEqual(None, model.solve(volume, 'context'))

    def test_solve_Packages(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep; package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'dep': {'blob': '2', 'version': [[1], 0]},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': [[1], 0]},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='10.04'))

    def test_solve_PackagesByLsbId(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package1'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package1', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'Ubuntu': {'value': {'binary': ['bin1', 'bin2'], 'devel': ['devel1', 'devel2']}},
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package1': {'packages': ['bin1', 'bin2', 'devel1', 'devel2'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package2'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package2', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                'Ubuntu': {'value': {'binary': ['bin']}},
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg1', 'pkg2']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package2': {'packages': ['bin'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Ubuntu', lsb_release='fake'))

    def test_solve_PackagesByCommonAlias(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package1'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package1', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '*': {'value': {'binary': ['pkg1']}},
                'Ubuntu': {'value': {'binary': ['pkg2']}},
                'resolves': {
                    'Ubuntu-10.04': {'version': [[1], 0], 'packages': ['pkg3']},
                    },
                },
            })
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context))
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake'))
        self.assertEqual({
            'context': {'blob': '1', 'command': ('activity', 'command'), 'version': [[1], 0]},
            'package1': {'packages': ['pkg1'], 'version': []},
            },
            model.solve(volume, context, lsb_id='Fake', lsb_release='fake'))

    def test_solve_NoPackages(self):
        volume = db.Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'package', 'type': ['package'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, model.solve(volume, context))


if __name__ == '__main__':
    tests.main()
