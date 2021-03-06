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

# pylint: disable-msg=W0611

import os
import re
import json
import shutil
import logging
from os.path import join, exists

from sugar_network import db, toolkit
from sugar_network.model import FrontRoutes
from sugar_network.node import model, solver
from sugar_network.toolkit.router import ACL, File, route
from sugar_network.toolkit.router import fallbackroute, preroute, postroute
from sugar_network.toolkit.spec import parse_version
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine, ranges, enforce


_GROUPED_DIFF_LIMIT = 1024
_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')

_logger = logging.getLogger('node.routes')


class NodeRoutes(db.Routes, FrontRoutes):

    def __init__(self, guid, auth=None, stats=None, **kwargs):
        db.Routes.__init__(self, **kwargs)
        FrontRoutes.__init__(self)
        self._guid = guid
        self._auth = auth
        self._stats = stats
        self._batch_dir = join(this.volume.root, 'batch')
        self._repos = []

        if not exists(self._batch_dir):
            os.makedirs(self._batch_dir)
        self.reload()

    @property
    def guid(self):
        return self._guid

    @preroute
    def preroute(self, op):
        request = this.request

        if request.principal:
            this.principal = request.principal
        elif op.acl & ACL.AUTH:
            this.principal = self._auth.logon(request)
        else:
            this.principal = None

        if op.acl & ACL.AUTHOR and not this.principal.cap_author_override:
            if request.resource == 'user':
                allowed = (this.principal == request.guid)
            else:
                allowed = this.principal in this.resource['author']
            enforce(allowed, http.Forbidden, 'Authors only')

        if op.acl & ACL.AGG_AUTHOR and not this.principal.cap_author_override:
            if this.resource.metadata[request.prop].acl & ACL.AUTHOR:
                allowed = this.principal in this.resource['author']
            elif request.key:
                value = this.resource[request.prop].get(request.key)
                allowed = value is None or this.principal in value['author']
            else:
                allowed = True
            enforce(allowed, http.Forbidden, 'Authors only')

        if op.acl & ACL.ADMIN:
            enforce(this.principal.cap_admin, http.Forbidden, 'Admins only')

    @postroute
    def postroute(self, result, exception):
        request = this.request
        if request.guid:
            pull = request.headers['pull']
            if pull is not None:
                this.response.content_type = 'application/octet-stream'
                result = model.diff_resource(pull)
        if exception is None and self._stats is not None:
            self._stats.count(request)
        this.response.headers['seqno'] = this.volume.seqno.value
        return result

    @route('GET', cmd='logon', acl=ACL.AUTH)
    def logon(self):
        pass

    @route('GET', cmd='whoami', mime_type='application/json', acl=ACL.AUTH)
    def whoami(self):
        data = {'route': 'direct',
                'find_limit': self.find_limit,
                }
        user = this.volume['user'][this.principal]
        if user.available:
            data['guid'] = user.guid
            data['name'] = user.repr('name')
            data['avatar'] = user.repr('avatar')
        return data

    @route('GET', cmd='status', mime_type='application/json')
    def status(self):
        return {'guid': self.guid,
                'seqno': {
                    'db': this.volume.seqno.value,
                    'releases': this.volume.release_seqno.value,
                    },
                'os': self._repos,
                # TODO
                'sugar': [
                    '0.82',
                    '0.84',
                    '0.86',
                    '0.88',
                    '0.94',
                    '0.96',
                    '0.98',
                    '0.100',
                    ],
                }

    @route('POST', ['user'], mime_type='application/json')
    def register(self):
        # To avoid authentication while registering new user
        self.create()

    @fallbackroute('GET', ['packages'])
    def route_packages(self):
        path = this.request.path
        if path and path[-1] == 'updates':
            result = []
            last_modified = 0
            for blob in this.volume.blobs.diff(
                    [[this.request.if_modified_since + 1, None]],
                    join(*path[:-1]), recursive=False):
                if '.' in blob.name:
                    continue
                result.append(blob.name)
                last_modified = max(last_modified, blob.mtime)
            this.response.content_type = 'application/json'
            if last_modified:
                this.response.last_modified = last_modified
            return result

        blob = this.volume.blobs.get(join(*path))
        if isinstance(blob, File):
            return blob
        else:
            this.response.content_type = 'application/json'
            return [i.name for i in blob if '.' not in i.name]

    @route('POST', ['context'], cmd='submit',
            arguments={'initial': False},
            mime_type='application/json', acl=ACL.AUTH)
    def submit_release(self, initial):
        blob = this.volume.blobs.post(
                this.request.content, this.request.content_type)
        try:
            context, release = model.load_bundle(blob, initial=initial)
        except Exception:
            this.volume.blobs.delete(blob.digest)
            raise
        this.call(method='POST', path=['context', context, 'releases'],
                content_type='application/json', content=release)
        return blob.digest

    @route('GET', ['context', None], cmd='solve',
            arguments={
                'requires': list,
                'stability': list,
                'assume': list,
                'details': bool,
                },
            mime_type='application/json')
    def solve(self, assume):
        assume_ = this.request['assume'] = {}
        for item in assume or []:
            enforce('-' in item, http.BadRequest,
                    "'assume' should be formed as '<CONTEXT>-<VERSION>")
            context, version = item.split('-', 1)
            assume_.setdefault(context, []).append(parse_version(version))
        solution = solver.solve(this.volume, this.request.guid, **this.request)
        enforce(solution is not None, 'Failed to solve')
        return solution

    @route('GET', ['context', None], cmd='clone',
            arguments={
                'requires': list,
                'stability': list,
                'assume': list,
                'details': bool,
                },
            )
    def clone(self, assume):
        solution = self.solve(assume)
        return this.volume.blobs.get(solution[this.request.guid]['blob'])

    @route('GET', [None, None], cmd='diff')
    def diff_resource(self):
        return model.diff_resource(this.request.headers['ranges'])

    @route('GET', [None], cmd='diff', mime_type='application/json')
    def grouped_diff(self, key):
        request = this.request
        enforce(request.resource != 'user', http.BadRequest,
                'Not allowed for User resource')

        if not key:
            key = 'guid'
        in_r = request.headers['ranges'] or [[1, None]]
        diff = {}

        for doc in this.volume[request.resource].diff(in_r):
            out_r = diff.get(doc[key])
            if out_r is None:
                if len(diff) >= _GROUPED_DIFF_LIMIT:
                    break
                out_r = diff[doc[key]] = []
            ranges.include(out_r, doc['seqno'], doc['seqno'])
            doc.diff(in_r, out_r)

        return diff

    @route('POST', cmd='apply', acl=ACL.AUTH)
    def batched_post(self):
        with toolkit.NamedTemporaryFile(dir=self._batch_dir,
                prefix=this.principal, delete=False) as batch:
            try:
                shutil.copyfileobj(this.request.content, batch)
            except Exception:
                os.unlink(batch.name)
                raise
        with file(batch.name + '.meta', 'w') as f:
            json.dump({'principal': this.principal.dump()}, f)
        coroutine.spawn(model.apply_batch, batch.name)

    @route('GET', cmd='stats', arguments={
                'start': int, 'end': int, 'limit': int, 'event': list},
            mime_type='application/json')
    def stats(self, start, end, limit, event):
        enforce(self._stats is not None, 'Statistics disabled')
        return self._stats.get(start, end, limit, event)

    def create(self):
        if this.principal and this.principal.cap_create_with_guid:
            guid = this.request.content.get('guid')
            enforce(not guid or _GUID_RE.match(guid), http.BadRequest,
                    'Malformed GUID')
        else:
            enforce('guid' not in this.request.content, http.BadRequest,
                    'GUID should not be specified')
        return db.Routes.create(self)

    def reload(self):
        try:
            sugars = this.volume['context']['sugar']['releases']
        except Exception:
            pass
        else:
            if 'resolves' in sugars:
                self._repos = sugars['resolves']['value'].keys()
                self._repos.sort()


this.principal = None
