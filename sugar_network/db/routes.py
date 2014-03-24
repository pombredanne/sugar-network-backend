# Copyright (C) 2011-2014 Aleksey Lim
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

import re
import logging
from contextlib import contextmanager

from sugar_network import toolkit
from sugar_network.db.metadata import Aggregated
from sugar_network.toolkit.router import ACL, File, route, fallbackroute
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, parcel, enforce


_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')

_logger = logging.getLogger('db.routes')


class Routes(object):

    def __init__(self, volume, find_limit=None):
        self.volume = volume
        self._find_limit = find_limit
        this.volume = self.volume

    @route('POST', [None], acl=ACL.AUTH, mime_type='application/json')
    def create(self, request):
        with self._post(request, ACL.CREATE) as doc:
            doc.created()
            if request.principal:
                authors = doc.posts['author'] = {}
                self._useradd(authors, request.principal, ACL.ORIGINAL)
            self.volume[request.resource].create(doc.posts)
            return doc['guid']

    @route('GET', [None],
            arguments={'offset': int, 'limit': int, 'reply': ('guid',)},
            mime_type='application/json')
    def find(self, request, reply, limit):
        self._preget(request)
        if self._find_limit:
            if limit <= 0:
                request['limit'] = self._find_limit
            elif limit > self._find_limit:
                _logger.warning('The find limit is restricted to %s',
                        self._find_limit)
                request['limit'] = self._find_limit
        documents, total = self.volume[request.resource].find(
                not_state='deleted', **request)
        result = [self._postget(request, i, reply) for i in documents]
        return {'total': total, 'result': result}

    @route('GET', [None, None], cmd='exists', mime_type='application/json')
    def exists(self, request):
        return self.volume[request.resource][request.guid].exists

    @route('PUT', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def update(self, request):
        with self._post(request, ACL.WRITE) as doc:
            if not doc.posts:
                return
            doc.updated()
            self.volume[request.resource].update(doc.guid, doc.posts)

    @route('PUT', [None, None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def update_prop(self, request):
        if request.content is None:
            value = request.content_stream
        else:
            value = request.content
        request.content = {request.prop: value}
        self.update(request)

    @route('DELETE', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def delete(self, request):
        # Node data should not be deleted immediately
        # to make master-slave synchronization possible
        directory = self.volume[request.resource]
        doc = directory[request.guid]
        enforce(doc.exists, http.NotFound, 'Resource not found')
        doc.posts['state'] = 'deleted'
        doc.updated()
        directory.update(doc.guid, doc.posts, 'delete')

    @route('GET', [None, None], arguments={'reply': list},
            mime_type='application/json')
    def get(self, request, reply):
        if not reply:
            reply = []
            for prop in self.volume[request.resource].metadata.values():
                if prop.acl & ACL.READ and not (prop.acl & ACL.LOCAL) and \
                        not isinstance(prop, Aggregated):
                    reply.append(prop.name)
        self._preget(request)
        doc = self.volume[request.resource].get(request.guid)
        enforce(doc.exists and doc['state'] != 'deleted', http.NotFound,
                'Resource not found')
        return self._postget(request, doc, reply)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self, request, response):
        directory = self.volume[request.resource]
        directory.metadata[request.prop].assert_access(ACL.READ)
        value = directory[request.guid].repr(request.prop)
        enforce(value is not File.AWAY, http.NotFound, 'No blob')
        return value

    @route('HEAD', [None, None, None])
    def get_prop_meta(self, request, response):
        return self.get_prop(request, response)

    @route('POST', [None, None, None],
            acl=ACL.AUTH, mime_type='application/json')
    def insert_to_aggprop(self, request):
        return self._aggpost(request, ACL.INSERT)

    @route('PUT', [None, None, None, None],
            acl=ACL.AUTH, mime_type='application/json')
    def update_aggprop(self, request):
        self._aggpost(request, ACL.REPLACE, request.key)

    @route('DELETE', [None, None, None, None],
            acl=ACL.AUTH, mime_type='application/json')
    def remove_from_aggprop(self, request):
        self._aggpost(request, ACL.REMOVE, request.key)

    @route('GET', [None, None, None, None], mime_type='application/json')
    def get_aggprop(self):
        doc = self.volume[this.request.resource][this.request.guid]
        prop = doc.metadata[this.request.prop]
        prop.assert_access(ACL.READ)
        enforce(isinstance(prop, Aggregated), http.BadRequest,
                'Property is not aggregated')
        agg_value = doc[prop.name].get(this.request.key)
        enforce(agg_value is not None, http.NotFound,
                'Aggregated item not found')
        value = prop.subreprcast(agg_value['value'])
        enforce(value is not File.AWAY, http.NotFound, 'No blob')
        return value

    @route('PUT', [None, None], cmd='useradd',
            arguments={'role': 0}, acl=ACL.AUTH | ACL.AUTHOR)
    def useradd(self, request, user, role):
        enforce(user, "Argument 'user' is not specified")
        directory = self.volume[request.resource]
        authors = directory.get(request.guid)['author']
        self._useradd(authors, user, role)
        directory.update(request.guid, {'author': authors})

    @route('PUT', [None, None], cmd='userdel', acl=ACL.AUTH | ACL.AUTHOR)
    def userdel(self, request, user):
        enforce(user, "Argument 'user' is not specified")
        enforce(user != request.principal, 'Cannot remove yourself')
        directory = self.volume[request.resource]
        authors = directory.get(request.guid)['author']
        enforce(user in authors, 'No such user')
        del authors[user]
        directory.update(request.guid, {'author': authors})

    @route('GET', [None, None], cmd='clone')
    def clone(self, request):
        clone = self.volume.clone(request.resource, request.guid)
        return parcel.encode([('push', None, clone)])

    @fallbackroute('GET', ['blobs'])
    def blobs(self):
        return this.volume.blobs.get(this.request.guid)

    def on_aggprop_update(self, request, prop, value):
        pass

    @contextmanager
    def _post(self, request, access):
        content = request.content
        enforce(isinstance(content, dict), http.BadRequest, 'Invalid value')

        if access == ACL.CREATE:
            if 'guid' in content:
                # TODO Temporal security hole, see TODO
                guid = content['guid']
                enforce(_GUID_RE.match(guid) is not None,
                        http.BadRequest, 'Malformed %s GUID', guid)
            else:
                guid = toolkit.uuid()
            doc = self.volume[request.resource][guid]
            enforce(not doc.exists, 'Resource already exists')
            doc.posts['guid'] = guid
            for name, prop in doc.metadata.items():
                if name not in content and prop.default is not None:
                    doc.posts[name] = prop.default
        else:
            doc = self.volume[request.resource][request.guid]
            enforce(doc.exists, 'Resource not found')
        this.resource = doc

        def teardown(new, old):
            for name, value in new.items():
                if old.get(name) != value:
                    doc.metadata[name].teardown(value)

        try:
            for name, value in content.items():
                prop = doc.metadata[name]
                prop.assert_access(access, doc.orig(name))
                if value is None:
                    doc.posts[name] = prop.default
                    continue
                try:
                    doc.posts[name] = prop.typecast(value)
                except Exception, error:
                    error = 'Value %r for %r property is invalid: %s' % \
                            (value, prop.name, error)
                    toolkit.exception(error)
                    raise http.BadRequest(error)
            yield doc
        except Exception:
            teardown(doc.posts, doc.origs)
            raise
        else:
            teardown(doc.origs, doc.posts)

    def _preget(self, request):
        reply = request.get('reply')
        if not reply:
            request['reply'] = ('guid',)
        else:
            directory = self.volume[request.resource]
            for prop in reply:
                directory.metadata[prop].assert_access(ACL.READ)

    def _postget(self, request, doc, props):
        result = {}
        for name in props:
            value = doc.repr(name)
            if isinstance(value, File):
                value = value.url
            result[name] = value
        return result

    def _useradd(self, authors, user, role):
        props = {}
        user_doc = self.volume['user'][user]
        if user_doc.exists:
            props['name'] = user_doc['name']
            role |= ACL.INSYSTEM
        else:
            role &= ~ACL.INSYSTEM
        props['role'] = role & (ACL.INSYSTEM | ACL.ORIGINAL)

        if user in authors:
            authors[user].update(props)
        else:
            if authors:
                top = max(authors.values(), key=lambda x: x['order'])
                props['order'] = top['order'] + 1
            else:
                props['order'] = 0
            authors[user] = props

    def _aggpost(self, request, acl, aggid=None):
        doc = this.resource = self.volume[request.resource][request.guid]
        prop = doc.metadata[request.prop]
        enforce(isinstance(prop, Aggregated), http.BadRequest,
                'Property is not aggregated')
        prop.assert_access(acl)

        if aggid and aggid in doc[request.prop]:
            aggvalue = doc[request.prop][aggid]
            self.on_aggprop_update(request, prop, aggvalue)
            prop.subteardown(aggvalue['value'])
        else:
            enforce(acl != ACL.REMOVE, http.NotFound, 'No aggregated item')
            self.on_aggprop_update(request, prop, None)

        aggvalue = {}
        if acl != ACL.REMOVE:
            value = prop.subtypecast(
                    request.content_stream if request.content is None
                    else request.content)
            if type(value) is tuple:
                aggid_, value = value
                enforce(not aggid or aggid == aggid_, http.BadRequest,
                        'Wrong aggregated id')
                aggid = aggid_
            elif not aggid:
                aggid = toolkit.uuid()
            aggvalue['value'] = value

        if request.principal:
            authors = aggvalue['author'] = {}
            role = ACL.ORIGINAL if request.principal in doc['author'] else 0
            self._useradd(authors, request.principal, role)
        doc.posts[request.prop] = {aggid: aggvalue}
        doc.updated()
        self.volume[request.resource].update(request.guid, doc.posts)

        return aggid
