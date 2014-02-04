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
import time
import json
import logging
from contextlib import contextmanager

from sugar_network import toolkit
from sugar_network.db import files
from sugar_network.db.metadata import Aggregated
from sugar_network.toolkit.router import ACL, route, preroute, fallbackroute
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, enforce


_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')

_logger = logging.getLogger('db.routes')


class Routes(object):

    def __init__(self, volume, find_limit=None):
        self.volume = volume
        self._find_limit = find_limit
        this.volume = self.volume

    @preroute
    def __preroute__(self, op, request, response):
        this.request = request
        this.response = response

    @route('POST', [None], acl=ACL.AUTH, mime_type='application/json')
    def create(self, request):
        with self._post(request, ACL.CREATE) as doc:
            self.on_create(request, doc.props)
            self.volume[request.resource].create(doc.props)
            self.after_post(doc)
            return doc['guid']

    @route('GET', [None],
            arguments={
                'offset': int,
                'limit': int,
                'layer': [],
                'reply': ('guid',),
                },
            mime_type='application/json')
    def find(self, request, reply, limit, layer):
        self._preget(request)
        if self._find_limit:
            if limit <= 0:
                request['limit'] = self._find_limit
            elif limit > self._find_limit:
                _logger.warning('The find limit is restricted to %s',
                        self._find_limit)
                request['limit'] = self._find_limit
        if 'deleted' in layer:
            _logger.warning('Requesting "deleted" layer, will ignore')
            layer.remove('deleted')
        documents, total = self.volume[request.resource].find(
                not_layer='deleted', **request)
        result = [self._postget(request, i, reply) for i in documents]
        return {'total': total, 'result': result}

    @route('GET', [None, None], cmd='exists', mime_type='application/json')
    def exists(self, request):
        directory = self.volume[request.resource]
        return directory.exists(request.guid)

    @route('PUT', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def update(self, request):
        with self._post(request, ACL.WRITE) as doc:
            if not doc.props:
                return
            self.on_update(request, doc.props)
            self.volume[request.resource].update(doc.guid, doc.props)
            self.after_post(doc)

    @route('GET', [None, None], cmd='diff', mime_type='application/json')
    def diff(self, request):
        result = {}
        res = self.volume[request.resource][request.guid]
        for prop, meta, __ in res.diff(toolkit.Sequence([[0, None]])):
            result[prop] = meta
        return result

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
        request.content = {'layer': 'deleted'}
        self.update(request)

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
        enforce('deleted' not in doc['layer'], http.NotFound, 'Deleted')
        return self._postget(request, doc, reply)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self, request, response):
        directory = self.volume[request.resource]
        doc = directory.get(request.guid)

        prop = directory.metadata[request.prop]
        prop.assert_access(ACL.READ)

        meta = doc.meta(prop.name) or {}
        if 'value' in meta:
            value = _get_prop(doc, prop, meta.pop('value'))
            enforce(value is not toolkit.File.AWAY, http.NotFound, 'No blob')
        else:
            value = prop.default

        response.meta = meta
        response.last_modified = meta.get('mtime')
        if isinstance(value, toolkit.File):
            response.content_length = value.get('size') or 0
        else:
            response.content_length = len(json.dumps(value))

        return value

    @route('HEAD', [None, None, None])
    def get_prop_meta(self, request, response):
        self.get_prop(request, response)

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

    @fallbackroute('GET', ['blobs'])
    def blobs(self, request):
        return files.get(request.guid)

    def on_create(self, request, props):
        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts

        if request.principal:
            authors = props['author'] = {}
            self._useradd(authors, request.principal, ACL.ORIGINAL)

    def on_update(self, request, props):
        props['mtime'] = int(time.time())

    def on_aggprop_update(self, request, prop, value):
        pass

    def after_post(self, doc):
        pass

    @contextmanager
    def _post(self, request, access):
        content = request.content
        enforce(isinstance(content, dict), http.BadRequest, 'Invalid value')
        directory = self.volume[request.resource]

        if access == ACL.CREATE:
            doc = directory.resource_class(None, None)
            if 'guid' in content:
                # TODO Temporal security hole, see TODO
                guid = content['guid']
                enforce(not directory.exists(guid),
                        http.BadRequest, '%s already exists', guid)
                enforce(_GUID_RE.match(guid) is not None,
                        http.BadRequest, 'Malformed %s GUID', guid)
            else:
                doc.props['guid'] = toolkit.uuid()
            for name, prop in directory.metadata.items():
                if name not in content and prop.default is not None:
                    doc.props[name] = prop.default
            orig = None
            this.resource = doc
        else:
            doc = directory.get(request.guid)
            orig = directory.get(request.guid)
            this.resource = orig

        def teardown(new):
            if orig is None:
                return
            for name, orig_value in orig.props.items():
                if doc[name] == orig_value:
                    continue
                prop = directory.metadata[name]
                prop.teardown(doc[name] if new else orig_value)

        try:
            for name, value in content.items():
                prop = directory.metadata[name]
                prop.assert_access(access, orig[name] if orig else None)
                try:
                    doc.props[name] = prop.typecast(value)
                except Exception, error:
                    error = 'Value %r for %r property is invalid: %s' % \
                            (value, prop.name, error)
                    toolkit.exception(error)
                    raise http.BadRequest(error)
            yield doc
        except Exception:
            teardown(True)
            raise
        else:
            teardown(False)

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
            prop = doc.metadata[name]
            result[name] = _get_prop(doc, prop, doc.get(name))
        return result

    def _useradd(self, authors, user, role):
        props = {}

        users = self.volume['user']
        if users.exists(user):
            props['name'] = users.get(user)['name']
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
        props = {request.prop: {aggid: aggvalue}}
        self.on_update(request, props)
        self.volume[request.resource].update(request.guid, props)

        return aggid


def _get_prop(doc, prop, value):
    value = prop.reprcast(value)
    if prop.on_get is not None:
        value = prop.on_get(doc, value)
    return value
