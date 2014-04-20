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
import hashlib
import logging
from os.path import join

from sugar_network import db, toolkit
from sugar_network.model import Release, context as _context, user as _user
from sugar_network.node import obs
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import spec, sat, http, coroutine, i18n, enforce


_logger = logging.getLogger('node.model')
_presolve_queue = None


class User(_user.User):

    def created(self):
        self.posts['guid'] = str(hashlib.sha1(self['pubkey']).hexdigest())


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
            return Release.teardown(self, value)
        # TODO Delete presolved files


class Context(_context.Context):

    @db.stored_property(db.Aggregated, subtype=_Release(),
            acl=ACL.READ | ACL.INSERT | ACL.REMOVE | ACL.REPLACE)
    def releases(self, value):
        return value

    def created(self):
        _context.Context.created(self)
        self._invalidate_solutions()

    def updated(self):
        _context.Context.updated(self)
        self._invalidate_solutions()

    def _invalidate_solutions(self):
        if self['releases'] and \
                [i for i in ('state', 'releases', 'dependencies')
                    if i in self.posts and self.posts[i] != self.orig(i)]:
            this.broadcast({
                'event': 'release',
                'seqno': this.volume.release_seqno.next(),
                })


class Volume(db.Volume):

    def __init__(self, root, resources, **kwargs):
        db.Volume.__init__(self, root, resources, **kwargs)
        self.release_seqno = toolkit.Seqno(join(root, 'var', 'seqno-release'))

    def close(self):
        db.Volume.close(self)
        self.release_seqno.commit()


def solve(volume, top_context, command=None, lsb_id=None, lsb_release=None,
        stability=None, requires=None):
    top_context = volume['context'][top_context]
    if stability is None:
        stability = ['stable']
    if isinstance(stability, basestring):
        stability = [stability]
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
            top_context.guid, lsb_id, lsb_release, stability, top_requires)

    def rate_release(digest, release):
        return [command in release.get('commands', []),
                _STABILITY_RATES.get(release['stability']) or 0,
                release['version'],
                digest,
                ]

    def add_deps(v_usage, deps):
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
        enforce(context.available, http.NotFound, 'Context not found')
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
                if release['stability'] not in stability or \
                        context.guid == top_context.guid and \
                            not spec.ensure(release['version'], top_cond):
                    continue
                bisect.insort(candidates, rate_release(digest, release))
            for release in reversed(candidates):
                digest = release[-1]
                release = releases[digest]['value']
                release_info = {
                        'title': i18n.decode(context['title'],
                            this.request.accept_language),
                        'version': release['version'],
                        'blob': digest,
                        }
                blob = volume.blobs.get(digest)
                if blob is not None:
                    release_info['size'] = blob.size
                    release_info['content-type'] = blob.meta['content-type']
                unpack_size = release['bundles']['*-*'].get('unpack_size')
                if unpack_size is not None:
                    release_info['unpack_size'] = unpack_size
                requires = release.get('requires') or {}
                if top_requires and context.guid == top_context.guid:
                    requires.update(top_requires)
                if context.guid == top_context.guid and 'commands' in release:
                    cmd = release['commands'].get(command)
                    if cmd is None:
                        cmd_name, cmd = release['commands'].items()[0]
                    else:
                        cmd_name = command
                    release_info['command'] = (cmd_name, cmd['exec'])
                    requires.update(cmd.get('requires') or {})
                v_release = len(varset)
                varset.append((context.guid, release_info))
                clause.append(v_release)
                add_deps(v_release, requires)

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
