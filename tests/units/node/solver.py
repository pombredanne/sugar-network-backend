#!/usr/bin/env python
# -*- coding: utf-8 -*-
# sugar-lint: disable

import os
import time
import json
import base64
import hashlib
import mimetypes
from cStringIO import StringIO
from os.path import exists

from __init__ import tests

from sugar_network import db, toolkit
from sugar_network.client import Connection
from sugar_network.model.post import Post
from sugar_network.model.context import Context
from sugar_network.node import solver
from sugar_network.node.model import User, Volume
from sugar_network.node.routes import NodeRoutes
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import Request, Router, ACL, File
from sugar_network.toolkit import spec, i18n, http, coroutine, ranges, enforce, lsb_release


class SolverTest(tests.Test):

    def test_solve_SortByVersions(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': 3, 'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'}},
                solver.solve(volume, context))

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                },
            })
        self.assertEqual(
                {context: {'command': 3, 'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'}},
                solver.solve(volume, context))

    def test_solve_SortByStability(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        context = volume['context'].create({
            'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'developer', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 2}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'buggy', 'version': [[3], 0], 'commands': {'activity': {'exec': 3}}}},
                },
            })
        self.assertEqual(
                {context: {'command': 2, 'title': '', 'blob': 'http://localhost/blobs/2', 'version': '2', 'size': 0, 'content-type': 'mime'}},
                solver.solve(volume, context))

    def test_solve_CollectDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable',
                    'version': [[1], 0],
                    'requires': spec.parse_requires('context2; context4'),
                    'commands': {'activity': {'exec': 'command'}},
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {
                    'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'context2': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': '2', 'size': 0, 'content-type': 'mime'},
            'context3': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'},
            'context4': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': '4', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context1'))

    def test_solve_CommandDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {
                    'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable',
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
                    'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable',
                    'version': [[2], 0],
                    'commands': {'activity': {'exec': 0}},
                    'requires': [],
                    }},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            'context2': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': '2', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context1', command='activity'))
        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 2, 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context1', command='application'))

    def test_solve_DepConditions(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep < 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': '2', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': '5', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep >= 2'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': '5', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep < 5'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': '4', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep > 2; dep <= 3'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep = 1'),
                    }},
                },
            })
        self.assertEqual({
                'context1': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
                'dep': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'size': 0, 'content-type': 'mime'},
                },
                solver.solve(volume, 'context1'))

    def test_solve_SwitchToAlternativeBranch(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context1', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '6': {'value': {'bundles': {'*-*': {'blob': '6'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=1'), 'commands': {'activity': {'exec': 6}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context2'), 'commands': {'activity': {'exec': 1}}}},
                },
            })
        volume['context'].create({
            'guid': 'context2', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context3; context4=1')}},
                },
            })
        volume['context'].create({
            'guid': 'context3', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('context4=2')}},
                },
            })
        volume['context'].create({
            'guid': 'context4', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        self.assertEqual({
            'context1': {'title': '', 'blob': 'http://localhost/blobs/6', 'version': '1', 'command': 6, 'size': 0, 'content-type': 'mime'},
            'context4': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': '1', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context1'))

    def test_solve_CommonDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep=2',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': '2', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<5',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': '4', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {},
            'dependencies': 'dep<4',
            'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/3', 'version': '3', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

    def test_solve_ExtraDeps(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires(''),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep>1'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/5', 'version': '5', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep<5'),
                    }},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/10', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/4', 'version': '4', 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

    def test_solve_Nothing(self):
        volume = Volume('master', [Context])
        this.volume = volume
        this.request = Request()

        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}}}},
                '3': {'value': {'bundles': {'*-*': {'blob': '3'}}, 'stability': 'stable', 'version': [[3], 0], 'commands': {'activity': {'exec': 0}}}},
                '4': {'value': {'bundles': {'*-*': {'blob': '4'}}, 'stability': 'stable', 'version': [[4], 0], 'commands': {'activity': {'exec': 0}}}},
                '5': {'value': {'bundles': {'*-*': {'blob': '5'}}, 'stability': 'stable', 'version': [[5], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                },
            })
        self.assertEqual(None, solver.solve(volume, 'context'))

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '10': {'value': {'bundles': {'*-*': {'blob': '10'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep=0'),
                    }},
                },
            })
        self.assertEqual(None, solver.solve(volume, 'context'))

    def test_solve_Packages(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        self.touch(('master/files/packages/lsb_release/machine/package', json.dumps({
            'version': '1',
            'binary': ['pkg1', 'pkg2'],
            })))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': '1'},
            },
            solver.solve(volume, 'context', lsb_release='lsb_release', machine='machine'))

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('dep; package'),
                    }},
                },
            })
        volume['context'].create({
            'guid': 'dep', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}}}},
                },
            })
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'dep': {'title': '', 'blob': 'http://localhost/blobs/2', 'version': '1', 'size': 0, 'content-type': 'mime'},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': '1'},
            },
            solver.solve(volume, 'context', lsb_release='lsb_release', machine='machine'))

    def test_solve_PackagesWithoutMachine(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        self.touch(('master/files/packages/lsb_release/machine1/package', json.dumps({
            'version': '1',
            'binary': ['pkg1', 'pkg2'],
            })))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 'command', 'size': 0, 'content-type': 'mime'},
            'package': {'packages': ['pkg1', 'pkg2'], 'version': '1'},
            },
            solver.solve(volume, 'context', lsb_release='lsb_release'))

        self.touch(('master/files/packages/lsb_release/machine2/package', json.dumps({
            'version': '2',
            'binary': ['pkg3', 'pkg4'],
            })))
        self.assertRaises(http.BadRequest, solver.solve, volume, 'context', lsb_release='lsb_release')

    def test_solve_NoPackages(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        this.request = Request()

        context = volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 'command'}},
                    'requires': spec.parse_requires('package'),
                    }},
                },
            })
        self.assertEqual(None, solver.solve(volume, context))

    def test_solve_IgnoreAbsentContexts(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('absent'), 'commands': {'activity': {'exec': 2}}}},
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'commands': {'activity': {'exec': 1}}}},
                },
            })

        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context'))

    def test_solve_SwitchToAlternativeBranchOnNonResolvedPackages(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume
        os.makedirs('master/files/packages/lsb_release/machine')

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 0}},
                    'commands': {'activity': {'exec': 1}}}},
                '2': {'value': {'bundles': {'*-*': {'blob': '2'}}, 'stability': 'stable', 'version': [[2], 0], 'commands': {'activity': {'exec': 0}},
                    'requires': spec.parse_requires('package'), 'commands': {'activity': {'exec': 2}}}},
                },
            })

        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            },
            solver.solve(volume, 'context', lsb_release='lsb_release', machine='machine'))

    def test_solve_MultipleAssumes(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'context', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '1': {'value': {'bundles': {'*-*': {'blob': '1'}}, 'stability': 'stable', 'version': [[1], 0], 'commands': {'activity': {'exec': 1}},
                    'requires': spec.parse_requires('package=2')}},
                },
            })

        self.assertEqual(
            None,
            solver.solve(volume, 'context', assume={'package': [[[1], 0]]}))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            'package': {'version': '2'},
            },
            solver.solve(volume, 'context', assume={'package': [[[2], 0]]}))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            'package': {'version': '2'},
            },
            solver.solve(volume, 'context', assume={'package': [[[1], 0], [[2], 0]]}))
        self.assertEqual({
            'context': {'title': '', 'blob': 'http://localhost/blobs/1', 'version': '1', 'command': 1, 'size': 0, 'content-type': 'mime'},
            'package': {'version': '2'},
            },
            solver.solve(volume, 'context', assume={'package': [[[1], 0], [[2], 0], [[3], 0]]}))

    def test_solve_SortMultipleAssumes(self):
        volume = Volume('master', [Context])
        volume.blobs.get = lambda digest: File(digest=digest, meta={'content-length': '0', 'content-type': 'mime'})
        this.volume = volume

        volume['context'].create({
            'guid': 'org.laptop.Memorize', 'type': ['activity'], 'title': {}, 'summary': {}, 'description': {}, 'releases': {
                '39': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[39], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.86; sugar<0.94; libxml2; numpy')}},
                '40': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[40], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.82; sugar<0.96; libxml2; numpy')}},
                '41': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[41], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.86; sugar<0.96; libxml2; numpy')}},
                '42': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[42], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.86; sugar<0.98; libxml2; numpy')}},
                '43': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[43], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.86; sugar<0.98; libxml2; numpy')}},
                '44': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[44], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.96; sugar<0.100; libxml2; numpy')}},
                '45': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[45], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.86; sugar<0.100; libxml2; numpy')}},
                '46': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[46], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.96; sugar<0.100; libxml2; numpy')}},
                '47': {'value': {'bundles': {'*-*': {'blob': 'blob'}}, 'stability': 'stable', 'version': [[47], 0], 'commands': {'activity': {'exec': 'true'}},
                    'requires': spec.parse_requires('sugar>0.96; sugar<0.100; libxml2; numpy')}},
                },
            })
        self.touch(('master/files/packages/lsb_release/machine/libxml2', json.dumps({
            'version': '2.7.8',
            'binary': ['libxml'],
            })))
        self.touch(('master/files/packages/lsb_release/machine/numpy', json.dumps({
            'version': '1.6.1',
            'binary': ['numpy'],
            })))

        self.assertEqual({
            'org.laptop.Memorize': {'title': '', 'blob': 'http://localhost/blobs/blob', 'version': '45', 'command': 'true', 'size': 0, 'content-type': 'mime'},
            'sugar': {'version': '0.94'},
            'libxml2': {'version': '2.7.8', 'packages': ['libxml']},
            'numpy': {'version': '1.6.1', 'packages': ['numpy']},
            },
            solver.solve(volume, 'org.laptop.Memorize', lsb_release='lsb_release', machine='machine', assume={'sugar': [[[0, 84], 0], [[0, 94], 0]]}))
        self.assertEqual({
            'org.laptop.Memorize': {'title': '', 'blob': 'http://localhost/blobs/blob', 'version': '45', 'command': 'true', 'size': 0, 'content-type': 'mime'},
            'sugar': {'version': '0.94'},
            'libxml2': {'version': '2.7.8', 'packages': ['libxml']},
            'numpy': {'version': '1.6.1', 'packages': ['numpy']},
            },
            solver.solve(volume, 'org.laptop.Memorize', lsb_release='lsb_release', machine='machine', assume={'sugar': [[[0, 94], 0], [[0, 84], 0]]}))


if __name__ == '__main__':
    tests.main()
