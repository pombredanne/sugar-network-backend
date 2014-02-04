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

import logging

from sugar_network import db, toolkit
from sugar_network.model import Release, context
from sugar_network.node import obs
from sugar_network.toolkit.router import ACL
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine, enforce


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
        statuses = releases['value'].setdefault('status', {})
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
            try:
                for arch in repo['arches']:
                    obs.resolve(repo['name'], arch, pkgs)
            except Exception, error:
                _logger.warning('Failed to resolve %r on %s',
                        pkgs, repo['name'])
                status = str(error)
            else:
                to_presolve.append((repo['name'], pkgs))
                status = 'success'
            statuses[repo['name']] = status

        if to_presolve and _presolve_queue is not None:
            _presolve_queue.put(to_presolve)
        if statuses:
            this.resource.record.set('releases', **releases)

        return value

    def teardown(self, value):
        if 'package' not in this.resource['type']:
            return Release.typecast(self, value)
        # TODO Delete presolved files


class Context(context.Context):

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
            resource = resource_
            directory = volume[resource_]
            continue

        if 'guid' in record:
            guid = record['guid']
            existed = directory.exists(guid)
            if existed:
                layer = directory.get(guid)['layer']
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


def presolve(presolve_path):
    global _presolve_queue
    _presolve_queue = coroutine.Queue()

    for repo_name, pkgs in _presolve_queue:
        obs.presolve(repo_name, pkgs, presolve_path)
