# Copyright (C) 2011-2013 Aleksey Lim
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
import re
import sys
import time
import json
import types
import hashlib
import logging
from contextlib import contextmanager
from os.path import exists

from sugar_network import toolkit
from sugar_network.db.metadata import BlobProperty, StoredProperty, LIST_TYPES
from sugar_network.toolkit.router import Blob, ACL, route
from sugar_network.toolkit import http, enforce


_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')

_logger = logging.getLogger('db.routes')


class Routes(object):

    def __init__(self, volume):
        self.volume = volume

    @route('POST', [None],
            acl=ACL.AUTH, mime_type='application/json')
    def create(self, request):
        with self._post(request, ACL.CREATE) as (directory, doc):
            event = {}
            self.on_create(request, doc.props, event)
            if 'guid' not in doc.props:
                doc.props['guid'] = toolkit.uuid()
            doc.guid = doc.props['guid']
            directory.create(doc.props, event)
            return doc.guid

    @route('GET', [None],
            arguments={'offset': int, 'limit': int, 'reply': ('guid',)},
            mime_type='application/json')
    def find(self, request, reply):
        self._preget(request)
        documents, total = self.volume[request.resource].find(**request)
        result = [self._get_props(i, request, reply) for i in documents]
        return {'total': total, 'result': result}

    @route('GET', [None, None], cmd='exists',
            mime_type='application/json')
    def exists(self, request):
        directory = self.volume[request.resource]
        return directory.exists(request.guid)

    @route('PUT', [None, None],
            acl=ACL.AUTH | ACL.AUTHOR)
    def update(self, request):
        with self._post(request, ACL.WRITE) as (directory, doc):
            if not doc.props:
                return
            event = {}
            self.on_update(request, doc.props, event)
            directory.update(doc.guid, doc.props, event)

    @route('PUT', [None, None, None],
            acl=ACL.AUTH | ACL.AUTHOR)
    def update_prop(self, request, url=None):
        if url:
            value = Blob({'url': url})
        elif request.content is None:
            value = request.content_stream
        else:
            value = request.content
        request.content = {request.prop: value}
        self.update(request)

    @route('DELETE', [None, None],
            acl=ACL.AUTH | ACL.AUTHOR)
    def delete(self, request):
        self.volume[request.resource].delete(request.guid)

    @route('GET', [None, None], arguments={'reply': list},
            mime_type='application/json')
    def get(self, request, reply):
        if not reply:
            reply = []
            for prop in self.volume[request.resource].metadata.values():
                if isinstance(prop, StoredProperty) and \
                        prop.acl & ACL.READ and not (prop.acl & ACL.LOCAL):
                    reply.append(prop.name)
        self._preget(request)
        doc = self.volume[request.resource].get(request.guid)
        return self._get_props(doc, request, reply)

    @route('GET', [None, None, None], mime_type='application/json')
    def get_prop(self, request, response):
        return self._prop_meta(request, response)

    @route('HEAD', [None, None, None])
    def get_prop_meta(self, request, response):
        self._prop_meta(request, response)

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

    def on_create(self, request, props, event):
        if 'guid' in props:
            # TODO Temporal security hole, see TODO
            guid = props['guid']
            enforce(not self.volume[request.resource].exists(guid),
                    '%s already exists', guid)
            enforce(_GUID_RE.match(guid) is not None,
                    'Malformed %s GUID', guid)

        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts

        if request.principal:
            authors = props['author'] = {}
            self._useradd(authors, request.principal, ACL.ORIGINAL)

    def on_update(self, request, props, event):
        props['mtime'] = int(time.time())

    def after_post(self, doc):
        pass

    @contextmanager
    def _post(self, request, access):
        content = request.content or {}
        enforce(isinstance(content, dict), 'Invalid value')

        directory = self.volume[request.resource]
        if request.guid:
            doc = directory.get(request.guid)
        else:
            doc = directory.document_class(None, {})
        doc.request = request
        blobs = []

        for name, value in content.items():
            prop = directory.metadata[name]
            if isinstance(prop, BlobProperty):
                prop.assert_access(ACL.CREATE if
                        access == ACL.WRITE and doc.meta(name) is None
                        else access)
                if value is None:
                    value = {'blob': None}
                elif isinstance(value, basestring) or hasattr(value, 'read'):
                    value = _read_blob(request, prop, value)
                    blobs.append(value['blob'])
                elif isinstance(value, dict):
                    enforce('url' in value or 'blob' in value, 'No bundle')
                else:
                    raise RuntimeError('Incorrect BLOB value')
            else:
                prop.assert_access(access)
                if prop.localized and isinstance(value, basestring):
                    value = {request.accept_language[0]: value}
                try:
                    value = _typecast_prop_value(prop.typecast, value)
                except Exception, error:
                    error = 'Value %r for %r property is invalid: %s' % \
                            (value, prop.name, error)
                    toolkit.exception(error)
                    raise RuntimeError(error)
            doc[name] = value

        if access == ACL.CREATE:
            for name, prop in directory.metadata.items():
                if not isinstance(prop, BlobProperty) and \
                        content.get(name) is None and \
                        (prop.default is not None or prop.on_set is not None):
                    doc[name] = prop.default

        try:
            for name, value in doc.props.items():
                prop = directory.metadata[name]
                if prop.on_set is not None:
                    doc.props[name] = prop.on_set(doc, value)
            yield directory, doc
        finally:
            for path in blobs:
                if exists(path):
                    os.unlink(path)

        self.after_post(doc)

    def _prop_meta(self, request, response):
        directory = self.volume[request.resource]
        prop = directory.metadata[request.prop]
        doc = directory.get(request.guid)
        doc.request = request

        prop.assert_access(ACL.READ)

        if isinstance(prop, StoredProperty):
            meta = doc.meta(prop.name) or {}
            if 'value' in meta:
                del meta['value']
            value = doc.get(prop.name, request.accept_language)
            value = prop.on_get(doc, value)
            response.content_length = len(json.dumps(value))
        else:
            value = prop.on_get(doc, doc.meta(prop.name))
            enforce(value is not None and ('blob' in value or 'url' in value),
                    http.NotFound, 'BLOB does not exist')
            if 'blob' in value:
                meta = value.copy()
                meta.pop('blob')
            else:
                meta = value

        response.meta = meta
        response.last_modified = meta.get('mtime')
        response.content_length = meta.get('blob_size') or 0

        return value

    def _preget(self, request):
        reply = request.get('reply')
        if not reply:
            request['reply'] = ('guid',)
        else:
            directory = self.volume[request.resource]
            for prop in reply:
                directory.metadata[prop].assert_access(ACL.READ)

    def _get_props(self, doc, request, props):
        result = {}
        metadata = doc.metadata
        doc.request = request
        for name in props:
            prop = metadata[name]
            value = prop.on_get(doc, doc.get(name, request.accept_language))
            if value is None:
                value = prop.default
            elif isinstance(value, Blob):
                value = value.get('url')
                if value is None:
                    value = '/'.join(['', metadata.name, doc.guid, name])
                if value.startswith('/'):
                    value = request.static_prefix + value
            result[name] = value
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


def _read_blob(request, prop, value):
    digest = hashlib.sha1()
    dst = toolkit.NamedTemporaryFile(delete=False)

    try:
        if isinstance(value, basestring):
            digest.update(value)
            dst.write(value)
        else:
            size = request.content_length or sys.maxint
            while size > 0:
                chunk = value.read(min(size, toolkit.BUFFER_SIZE))
                if not chunk:
                    break
                dst.write(chunk)
                size -= len(chunk)
                digest.update(chunk)
    except Exception:
        os.unlink(dst.name)
        raise
    finally:
        dst.close()

    return {'blob': dst.name,
            'digest': digest.hexdigest(),
            'mime_type': request.content_type or prop.mime_type,
            }


def _typecast_prop_value(typecast, value):
    if typecast is None:
        return value
    enforce(value is not None, ValueError, 'Property value cannot be None')

    def cast(typecast, value):
        if isinstance(typecast, types.FunctionType):
            return typecast(value)
        elif typecast is unicode:
            return value.encode('utf-8')
        elif typecast is str:
            return str(value)
        elif typecast is int:
            return int(value)
        elif typecast is float:
            return float(value)
        elif typecast is bool:
            return bool(value)
        elif typecast is dict:
            return dict(value)
        else:
            raise ValueError('Unknown typecast')

    if type(typecast) in LIST_TYPES:
        if typecast:
            first = iter(typecast).next()
        else:
            first = None
        if first is not None and type(first) is not type and \
                type(first) not in LIST_TYPES:
            value = cast(type(first), value)
            enforce(value in typecast, ValueError,
                    "Value %r is not in '%s' list",
                    value, ', '.join([str(i) for i in typecast]))
        else:
            enforce(len(typecast) <= 1, ValueError,
                    'List values should contain values of the same type')
            if type(value) not in LIST_TYPES:
                value = (value,)
            typecast, = typecast or [str]
            value = tuple([_typecast_prop_value(typecast, i) for i in value])
    else:
        value = cast(typecast, value)

    return value
