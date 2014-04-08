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
from os.path import join

from sugar_network import db
from sugar_network.model import FrontRoutes, load_bundle
from sugar_network.node import model
# pylint: disable-msg=W0611
from sugar_network.toolkit.router import route, postroute, ACL, File
from sugar_network.toolkit.router import Request, fallbackroute, preroute
from sugar_network.toolkit.spec import parse_requires, parse_version
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine, enforce


_logger = logging.getLogger('node.routes')


class NodeRoutes(db.Routes, FrontRoutes):

    def __init__(self, guid, auth=None, **kwargs):
        db.Routes.__init__(self, **kwargs)
        FrontRoutes.__init__(self)
        self._guid = guid
        self._auth = auth

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
        if op.acl & ACL.AUTHOR and request.guid:
            if not this.principal:
                this.principal = self._auth.logon(request)
            allowed = this.principal.admin
            if not allowed:
                if request.resource == 'user':
                    allowed = (this.principal == request.guid)
                else:
                    doc = self.volume[request.resource].get(request.guid)
                    allowed = this.principal in doc['author']
            enforce(allowed, http.Forbidden, 'Authors only')
        if op.acl & ACL.SUPERUSER:
            if not this.principal:
                this.principal = self._auth.logon(request)
            enforce(this.principal.admin, http.Forbidden, 'Superusers only')

    @route('GET', cmd='whoami', mime_type='application/json')
    def whoami(self):
        roles = []
        if this.principal and this.principal.admin:
            roles.append('root')
        return {'roles': roles,
                'guid': this.principal,
                'route': 'direct',
                }

    @route('GET', cmd='status', mime_type='application/json')
    def status(self):
        return {'guid': self.guid,
                'seqno': {
                    'db': self.volume.seqno.value,
                    'releases': self.volume.releases_seqno.value,
                    },
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
            for blob in self.volume.blobs.diff(
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

        blob = self.volume.blobs.get(join(*path))
        if isinstance(blob, File):
            return blob
        else:
            this.response.content_type = 'application/json'
            return [i.name for i in blob if '.' not in i.name]

    @route('POST', ['context'], cmd='submit',
            arguments={'initial': False},
            mime_type='application/json', acl=ACL.AUTH)
    def submit_release(self, initial):
        blob = self.volume.blobs.post(
                this.request.content_stream, this.request.content_type)
        try:
            context, release = load_bundle(blob, initial=initial)
        except Exception:
            self.volume.blobs.delete(blob.digest)
            raise
        this.call(method='POST', path=['context', context, 'releases'],
                content_type='application/json', content=release)
        return blob.digest

    @route('GET', ['context', None], cmd='solve',
            arguments={'requires': list, 'stability': list},
            mime_type='application/json')
    def solve(self):
        solution = model.solve(self.volume, this.request.guid, **this.request)
        enforce(solution is not None, 'Failed to solve')
        return solution

    @route('GET', ['context', None], cmd='resolve',
            arguments={'requires': list, 'stability': list})
    def resolve(self):
        solution = self.solve()
        return self.volume.blobs.get(solution[this.request.guid]['blob'])


this.principal = None
