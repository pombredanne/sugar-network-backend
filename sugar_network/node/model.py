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

import bisect
import logging

from sugar_network import db, toolkit
from sugar_network.model import Release, context as base_context
from sugar_network.node import obs
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import spec, sat, http, coroutine, enforce


_logger = logging.getLogger('node.model')
_presolve_queue = None


class _Release(Release):

    _package_cast = db.Dict(db.List())

    def typecast(self, value):
        if not this.resource.exists or 'package' not in this.resource['type']:
            return Release.typecast(self, value)

        value = self._package_cast.typecast(value)
        enforce(value.get('binary'), http.BadRequest, 'No binary aliases')

        distro = this.request.key
        if distro == '*':
            lsb_id = None
            lsb_release = None
        elif '-' in this.request.key:
            lsb_id, lsb_release = distro.split('-', 1)
        else:
            lsb_id = distro
            lsb_release = None
        releases = this.resource.record.get('releases')
        resolves = releases['value'].setdefault('resolves', {})
        to_presolve = []

        for repo in obs.get_repos():
            if lsb_id and lsb_id != repo['lsb_id'] or \
                    lsb_release and lsb_release != repo['lsb_release']:
                continue
            # Make sure there are no alias overrides
            if not lsb_id and repo['lsb_id'] in releases['value'] or \
                    not lsb_release and repo['name'] in releases['value']:
                continue
            pkgs = sum([value.get(i, []) for i in ('binary', 'devel')], [])
            version = None
            try:
                for arch in repo['arches']:
                    version = obs.resolve(repo['name'], arch, pkgs)['version']
            except Exception, error:
                _logger.warning('Failed to resolve %r on %s',
                        pkgs, repo['name'])
                resolve = {'status': str(error)}
            else:
                to_presolve.append((repo['name'], pkgs))
                resolve = {
                        'version': spec.parse_version(version),
                        'packages': pkgs,
                        'status': 'success',
                        }
            resolves.setdefault(repo['name'], {}).update(resolve)

        if to_presolve and _presolve_queue is not None:
            _presolve_queue.put(to_presolve)
        if resolves:
            this.resource.record.set('releases', **releases)

        return value

    def teardown(self, value):
        if 'package' not in this.resource['type']:
            return Release.typecast(self, value)
        # TODO Delete presolved files


class Context(base_context.Context):

    @db.stored_property(db.Aggregated, subtype=_Release(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
    def releases(self, value):
        return value

    @releases.setter
    def releases(self, value):
        if value or this.request.method != 'POST':
            self.invalidate_solutions()
        return value


def diff(volume, in_seq, out_seq=None, exclude_seq=None, layer=None,
        ignore_documents=None, **kwargs):
    if out_seq is None:
        out_seq = toolkit.Sequence([])
    is_the_only_seq = not out_seq
    if layer:
        if isinstance(layer, basestring):
            layer = [layer]
        layer.append('common')
    try:
        for resource, directory in volume.items():
            if ignore_documents and resource in ignore_documents:
                continue
            coroutine.dispatch()
            directory.commit()
            yield {'resource': resource}
            for guid, patch in directory.diff(in_seq, exclude_seq,
                    layer=layer if resource == 'context' else None):
                adiff = {}
                adiff_seq = toolkit.Sequence()
                for prop, meta, seqno in patch:
                    adiff[prop] = meta
                    adiff_seq.include(seqno, seqno)
                if adiff:
                    yield {'guid': guid, 'diff': adiff}
                out_seq.include(adiff_seq)
        if is_the_only_seq:
            # There is only one diff, so, we can stretch it to remove all holes
            out_seq.stretch()
    except StopIteration:
        pass

    yield {'commit': out_seq}


def merge(volume, records):
    directory = None
    commit_seq = toolkit.Sequence()
    merged_seq = toolkit.Sequence()
    synced = False

    for record in records:
        resource_ = record.get('resource')
        if resource_:
            directory = volume[resource_]
            continue

        if 'guid' in record:
            seqno, merged = directory.merge(**record)
            synced = synced or merged
            if seqno is not None:
                merged_seq.include(seqno, seqno)
            continue

        commit = record.get('commit')
        if commit is not None:
            commit_seq.include(commit)
            continue

    if synced:
        this.broadcast({'event': 'sync'})

    return commit_seq, merged_seq


def solve(volume, top_context, lsb_id=None, lsb_release=None,
        stability=None, requires=None):
    top_context = volume['context'][top_context]
    top_stability = stability or ['stable']
    if isinstance(top_stability, basestring):
        top_stability = [top_stability]
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

    lsb_distro = '-'.join([lsb_id, lsb_release]) if lsb_release else None
    varset = [None]
    context_clauses = {}
    clauses = []

    _logger.debug('Solve %r lsb_id=%r lsb_release=%r stability=%r requires=%r',
            top_context.guid, lsb_id, lsb_release, top_stability, top_requires)

    def rate_release(digest, release):
        return [_STABILITY_RATES.get(release['stability']) or 0,
                release['version'],
                digest,
                ]

    def add_deps(context, v_usage, deps):
        if top_requires and context.guid == top_context.guid:
            deps.update(top_requires)
        for dep, cond in deps.items():
            dep_clause = [-v_usage]
            for v_release in add_context(dep):
                if spec.ensure(varset[v_release][1]['version'], cond):
                    dep_clause.append(v_release)
            clauses.append(dep_clause)

    def add_context(context):
        if context in context_clauses:
            return context_clauses[context]
        context = volume['context'][context]
        releases = context['releases']
        clause = []

        if 'package' in context['type']:
            pkg_lst = None
            pkg_ver = []
            pkg = releases.get('resolves', {}).get(lsb_distro)
            if pkg:
                pkg_ver = pkg['version']
                pkg_lst = pkg['packages']
            else:
                alias = releases.get(lsb_id) or releases.get('*')
                if alias:
                    alias = alias['value']
                    pkg_lst = alias.get('binary', []) + alias.get('devel', [])
            if pkg_lst:
                clause.append(len(varset))
                varset.append((
                    context.guid,
                    {'version': pkg_ver, 'packages': pkg_lst},
                    ))
        else:
            candidates = []
            for digest, release in releases.items():
                if 'value' not in release:
                    continue
                release = release['value']
                if release['stability'] not in top_stability or \
                        context.guid == top_context.guid and \
                            not spec.ensure(release['version'], top_cond):
                    continue
                bisect.insort(candidates, rate_release(digest, release))
            for release in reversed(candidates):
                digest = release[-1]
                release = releases[digest]['value']
                release_info = {'version': release['version'], 'blob': digest}
                if context.guid == top_context.guid:
                    release_info['command'] = release['command']
                v_release = len(varset)
                varset.append((context.guid, release_info))
                clause.append(v_release)
                add_deps(context, v_release, release.get('requires') or {})

        if clause:
            context_clauses[context.guid] = clause
        else:
            _logger.trace('No candidates for %r', context.guid)
        return clause

    top_clause = add_context(top_context.guid)
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

    _logger.debug('Solution for %r: %r', top_context.guid, solution)

    return solution


def presolve(presolve_path):
    global _presolve_queue
    _presolve_queue = coroutine.Queue()

    for repo_name, pkgs in _presolve_queue:
        obs.presolve(repo_name, pkgs, presolve_path)


_STABILITY_RATES = {
        'insecure': 0,
        'buggy': 1,
        'developer': 2,
        'testing': 3,
        'stable': 4,
        }
