# Copyright (C) 2012-2014 Aleksey Lim
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import json
import bisect
import logging
from os.path import join, exists

from sugar_network import db
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.packagekit import parse_machine
from sugar_network.toolkit import sat, http, i18n, spec, enforce


_MACHINE_MAP = {
        'i486': ['i386'],
        'i586': ['i486', 'i386'],
        'i686': ['i586', 'i486', 'i386'],
        'x86_64': ['i686', 'i586', 'i486', 'i386'],
        'ppc': ['ppc32'],
        'ppc64': ['ppc'],
        }

_STABILITY_RATES = {
        'insecure': 0,
        'buggy': 1,
        'developer': 2,
        'testing': 3,
        'stable': 4,
        }

_logger = logging.getLogger('node.solver')


def solve(volume, top_context, command=None, lsb_release=None, machine=None,
        stability=None, requires=None, assume=None, details=False):
    top_context = volume['context'][top_context]
    if stability is None:
        stability = ['stable']
    if isinstance(stability, basestring):
        stability = [stability]
    if lsb_release:
        machine = _resolve_machine(lsb_release, machine)
    top_cond = []
    top_requires = {}
    if isinstance(requires, basestring):
        top_requires.update(spec.parse_requires(requires))
    elif requires:
        for i in requires:
            top_requires.update(spec.parse_requires(i))
    if top_context['dependencies']:
        top_requires.update(spec.parse_requires(top_context['dependencies']))
    if top_context.guid in top_requires:
        top_cond = top_requires.pop(top_context.guid)

    varset = [None]
    context_clauses = {}
    clauses = []

    _logger.debug(
            'Solve %r lsb_release=%r machine=%r stability=%r requires=%r',
            top_context.guid, lsb_release, machine, stability, top_requires)

    def rate_release(key, release):
        return [command in release.get('commands', []),
                _STABILITY_RATES.get(release['stability']) or 0,
                release['version'],
                key,
                ]

    def add_deps(v_usage, deps):
        usage = varset[v_usage]
        for dep, cond in deps.items():
            dep_clause = [-v_usage]
            for v_release in add_clause(dep):
                release = varset[v_release]
                if spec.ensure_version(release[1]['version'], cond):
                    _logger.trace('Consider %d:%s(%s) depends on %d:%s(%s)',
                            v_usage, usage[0], usage[1]['version'],
                            v_release, dep, release[1]['version'])
                    dep_clause.append(v_release)
                else:
                    _logger.trace('Ignore %d:%s(%s) depends on %d:%s(%s)',
                            v_usage, usage[0], usage[1]['version'],
                            v_release, dep, release[1]['version'])
            clauses.append(dep_clause)

    def add_context(guid):
        context = volume['context'][guid]

        if not context.available:
            _logger.trace('No %r context', context.guid)
            return []

        clause = []
        candidates = []
        releases = context['releases']

        for key, rel in releases.items():
            if 'value' not in rel:
                continue
            rel = rel['value']
            if rel['stability'] not in stability or \
                    context.guid == top_context.guid and \
                        not spec.ensure_version(rel['version'], top_cond):
                continue
            bisect.insort(candidates, rate_release(key, rel))

        for rate in reversed(candidates):
            agg_value = releases[rate[-1]]
            rel = agg_value['value']
            # TODO Assume we have only noarch bundles
            bundle = rel['bundles']['*-*']
            blob = volume.blobs.get(bundle['blob'])
            if blob is None:
                _logger.debug('Absent blob for %r release', rel)
                continue
            release_info = {
                    'title': i18n.decode(context['title'],
                        this.request.accept_language),
                    'version': rel['version'],
                    'blob': blob,
                    'size': blob.size,
                    'content-type': blob.meta['content-type'],
                    }
            if details:
                if 'announce' in rel:
                    announce = volume['post'][rel['announce']]
                    if announce.available:
                        release_info['announce'] = i18n.decode(
                                announce['message'],
                                this.request.accept_language)
                release_info['ctime'] = agg_value['ctime']
                release_info['author'] = agg_value['author']
                db.Author.format(agg_value['author'])
            unpack_size = bundle.get('unpack_size')
            if unpack_size is not None:
                release_info['unpack_size'] = unpack_size
            requires = rel.get('requires') or {}
            if top_requires and context.guid == top_context.guid:
                requires.update(top_requires)
            if context.guid == top_context.guid and 'commands' in rel:
                cmd = rel['commands'].get(command)
                if cmd is None:
                    cmd = rel['commands'].values()[0]
                release_info['command'] = cmd['exec']
                requires.update(cmd.get('requires') or {})
            _logger.trace('Consider %d:%s(%s) context',
                    len(varset), context.guid, release_info['version'])
            v_release = len(varset)
            varset.append((context.guid, release_info))
            clause.append(v_release)
            add_deps(v_release, requires)

        return clause

    def add_package(guid, pkg):
        _logger.trace('Consider %d:%s(%s) package',
                len(varset), guid, pkg['version'])
        clause = [len(varset)]
        varset.append((guid, {
            'version': pkg['version'],
            'packages': pkg['binary'],
            }))
        return clause

    def add_assumed_package(guid):
        clause = []
        for version in reversed(sorted(assume[guid])):
            _logger.trace('Assume %d:%s(%s) package',
                    len(varset), guid, version)
            clause.append(len(varset))
            varset.append((guid, {'version': version}))
        return clause

    def add_clause(guid):
        if guid in context_clauses:
            return context_clauses[guid]
        if assume and guid in assume:
            clause = add_assumed_package(guid)
        else:
            package = None
            if lsb_release:
                package = _resolve_package(lsb_release, machine, guid)
            if package is not None:
                clause = add_package(guid, package)
            else:
                clause = add_context(guid)
        if clause:
            context_clauses[guid] = clause
        else:
            _logger.trace('No candidates for %r', guid)
        return clause

    top_clause = add_clause(top_context.guid)
    if not top_clause:
        _logger.debug('No versions for %r', top_context.guid)
        return None

    result = sat.solve(clauses + [top_clause], context_clauses)
    if not result:
        _logger.debug('Failed to solve %r', top_context.guid)
        return None
    if not top_context.guid in result:
        _logger.debug('No top versions for %r', top_context.guid)
        return None

    solution = dict([varset[i] for i in result.values()])
    for rel in solution.values():
        version = rel['version']
        if version:
            rel['version'] = spec.format_version(rel['version'])
        else:
            del rel['version']

    _logger.debug('Solution for %r: %r', top_context.guid, solution)

    return solution


def _resolve_machine(lsb_release, machine):
    path = join(this.volume.root, 'files', 'packages', lsb_release)
    supported = []
    for __, supported, __ in os.walk(path):
        break
    else:
        raise http.BadRequest('Unsupported GNU/Linux distribution')

    if not machine:
        enforce(len(supported) == 1,
                http.BadRequest, "Argument 'machine' not specified")
        return supported[0]

    machine = parse_machine(machine)
    if machine in supported:
        return machine

    enforce(machine in _MACHINE_MAP,
            http.BadRequest, 'Unknown machine architecture')
    for machine in _MACHINE_MAP[machine]:
        if machine in supported:
            return machine

    raise http.BadRequest('Unsupported machine architecture')


def _resolve_package(lsb_release, machine, name):
    path = join(this.volume.root, 'files', 'packages',
            lsb_release, machine, name)
    if not exists(path):
        return None
    try:
        with file(path) as f:
            return json.load(f)
    except Exception:
        _logger.exception('Failed to resolve %s/%s/%s',
                lsb_release, machine, name)
        return None
