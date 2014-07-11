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

# pylint: disable-msg=W0611

import logging
from contextlib import contextmanager

from sugar_network import toolkit
from sugar_network.db.metadata import Aggregated, Author
from sugar_network.toolkit.router import ACL, File
from sugar_network.toolkit.router import route, fallbackroute, preroute
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, ranges, enforce


_logger = logging.getLogger('db.routes')


class Routes(object):

    def __init__(self, find_limit=None):
        this.add_property('resource', _get_resource)
        self._find_limit = find_limit

    @preroute
    def __preroute__(self, op):
        this.reset_property('resource')

    @route('POST', [None], acl=ACL.AUTH, mime_type='application/json')
    def create(self):
        with self._post(ACL.CREATE) as doc:
            doc.routed_creating()
            if this.principal:
                authors = doc.posts['author'] = {}
                self._useradd(authors, this.principal, Author.ORIGINAL)
            this.volume[this.request.resource].create(doc.posts)
            this.request.guid = doc.guid
            doc.routed_created()
            return doc.guid

    @route('PUT', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def update(self):
        with self._post(ACL.WRITE) as doc:
            if not doc.posts:
                return
            doc.routed_updating()
            this.volume[this.request.resource].update(doc.guid, doc.posts)
            doc.routed_updated()

    @route('PUT', [None, None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def update_prop(self):
        request = this.request
        request.content = {request.prop: request.content}
        self.update()

    @route('DELETE', [None, None], acl=ACL.AUTH | ACL.AUTHOR)
    def delete(self):
        # Node data should not be deleted immediately
        # to make master-slave synchronization possible
        directory = this.volume[this.request.resource]
        doc = directory[this.request.guid]
        enforce(doc.available, http.NotFound, 'Resource not found')
        doc.posts['state'] = 'deleted'
        doc.routed_updating()
        directory.update(doc.guid, doc.posts, 'delete')
        doc.routed_updated()

    @route('GET', [None],
            arguments={'offset': int, 'limit': int, 'reply': ('guid',)},
            mime_type='application/json')
    def find(self, reply, limit):
        self._preget()
        request = this.request
        if not limit:
            request['limit'] = self._find_limit
        elif self._find_limit and limit > self._find_limit:
            _logger.warning('The find limit is restricted to %s',
                    self._find_limit)
            request['limit'] = self._find_limit
        documents, total = this.volume[request.resource].find(
                not_state='deleted', **request)
        result = [self._postget(i, reply) for i in documents]
        return {'total': total, 'result': result}

    @route('GET', [None, None], cmd='exists', mime_type='application/json')
    def exists(self):
        return this.volume[this.request.resource][this.request.guid].available

    @route('GET', [None, None], arguments={'reply': list},
            mime_type='application/json')
    def get(self, reply):
        if not reply:
            reply = []
            for prop in this.volume[this.request.resource].metadata.values():
                if prop.acl & ACL.READ and not isinstance(prop, Aggregated):
                    reply.append(prop.name)
        self._preget()
        doc = this.volume[this.request.resource].get(this.request.guid)
        enforce(doc.available, http.NotFound, 'Resource not found')
        return self._postget(doc, reply)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self):
        request = this.request
        directory = this.volume[request.resource]
        directory.metadata[request.prop].assert_access(ACL.READ)
        return directory[request.guid].repr(request.prop)

    @route('HEAD', [None, None, None])
    def get_prop_meta(self):
        return self.get_prop()

    @route('POST', [None, None, None],
            acl=ACL.AUTH | ACL.AGG_AUTHOR, mime_type='application/json')
    def insert_to_aggprop(self):
        return self._aggpost(ACL.INSERT)

    @route('PUT', [None, None, None, None],
            acl=ACL.AUTH | ACL.AGG_AUTHOR, mime_type='application/json')
    def update_aggprop(self):
        self._aggpost(ACL.REPLACE)

    @route('DELETE', [None, None, None, None],
            acl=ACL.AUTH | ACL.AGG_AUTHOR, mime_type='application/json')
    def remove_from_aggprop(self):
        self._aggpost(ACL.REMOVE)

    @route('GET', [None, None, None, None], mime_type='application/json')
    def get_aggprop(self):
        doc = this.volume[this.request.resource][this.request.guid]
        prop = doc.metadata[this.request.prop]
        prop.assert_access(ACL.READ)
        enforce(isinstance(prop, Aggregated), http.BadRequest,
                'Property is not aggregated')
        agg_value = doc[prop.name].get(this.request.key)
        enforce(agg_value is not None, http.NotFound,
                'Aggregated item not found')
        return prop.subreprcast(agg_value['value'])

    @route('PUT', [None, None], cmd='useradd',
            arguments={'role': 0}, acl=ACL.AUTH | ACL.AUTHOR)
    def useradd(self, user, role):
        request = this.request
        enforce(user, "Argument 'user' is not specified")
        directory = this.volume[request.resource]
        authors = directory.get(request.guid)['author']
        self._useradd(authors, user, role)
        directory.update(request.guid, {'author': authors})

    @route('PUT', [None, None], cmd='userdel', acl=ACL.AUTH | ACL.AUTHOR)
    def userdel(self, user):
        request = this.request
        enforce(user, "Argument 'user' is not specified")
        enforce(user != this.principal, 'Cannot remove yourself')
        directory = this.volume[request.resource]
        authors = directory.get(request.guid)['author']
        enforce(user in authors, 'No such user')
        del authors[user]
        directory.update(request.guid, {'author': authors})

    @route('GET', ['blobs', None])
    def blobs(self, thumb):
        blob = this.volume.blobs.get(this.request.guid, thumb=thumb)
        enforce(blob is not None, http.NotFound, 'No such blob')
        if thumb is '' and 'x-thumbs' in blob.meta:
            thumb = blob.meta['x-thumbs'].split()[0]
            blob = this.volume.blobs.get(this.request.guid, thumb=thumb)
        return blob

    @fallbackroute('GET', ['assets'])
    def assets(self):
        return this.volume.blobs.get(this.request.path)

    @contextmanager
    def _post(self, access):
        content = this.request.content
        enforce(isinstance(content, dict), http.BadRequest, 'Invalid value')

        if access == ACL.CREATE:
            guid = content.get('guid') or toolkit.uuid()
            doc = this.volume[this.request.resource][guid]
            enforce(not doc.exists, 'Resource already exists')
            doc.posts['guid'] = guid
            for name, prop in doc.metadata.items():
                if name not in content and prop.default is not None:
                    doc.posts[name] = prop.default
        else:
            enforce('guid' not in content, http.BadRequest,
                    'GUID in cannot be changed')
            doc = this.volume[this.request.resource][this.request.guid]
            enforce(doc.available, 'Resource not found')

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
                    _logger.exception(error)
                    raise http.BadRequest(error)
            yield doc
        except Exception:
            teardown(doc.posts, doc.origs)
            raise
        else:
            teardown(doc.origs, doc.posts)

    def _preget(self):
        reply = this.request.get('reply')
        if not reply:
            this.request['reply'] = ('guid',)
        else:
            directory = this.volume[this.request.resource]
            for prop in reply:
                directory.metadata[prop].assert_access(ACL.READ)

    def _postget(self, doc, props):
        result = {}
        for name in props:
            result[name] = doc.repr(name)
        return result

    def _useradd(self, authors, user, role):
        props = {'role': role & Author.ORIGINAL}
        if user in authors:
            authors[user].update(props)
        else:
            authors[user] = props

    def _aggpost(self, acl):
        request = this.request
        doc = this.volume[request.resource][request.guid]
        prop = doc.metadata[request.prop]
        enforce(isinstance(prop, Aggregated), http.BadRequest,
                'Property is not aggregated')
        prop.assert_access(acl)

        aggid = request.key
        if aggid and aggid in doc[request.prop]:
            aggvalue = doc[request.prop][aggid]
            prop.subteardown(aggvalue['value'])
        else:
            enforce(acl != ACL.REMOVE, http.NotFound, 'No aggregated item')

        aggvalue = {}
        if acl != ACL.REMOVE:
            value = prop.subtypecast(request.content)
            if type(value) is tuple:
                aggid_, value = value
                enforce(not aggid or aggid == aggid_, http.BadRequest,
                        'Wrong aggregated id')
                aggid = aggid_
            elif not aggid:
                aggid = toolkit.uuid()
            aggvalue['value'] = value

        if this.principal:
            authors = aggvalue['author'] = {}
            role = Author.ORIGINAL if this.principal in doc['author'] else 0
            self._useradd(authors, this.principal, role)
        doc.posts[request.prop] = {aggid: aggvalue}
        doc.routed_updating()
        this.volume[request.resource].update(request.guid, doc.posts)
        doc.routed_updated()

        return aggid


def _get_resource():
    request = this.request
    return this.volume[request.resource][request.guid]
