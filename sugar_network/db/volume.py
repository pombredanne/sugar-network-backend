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
import time
import logging
from contextlib import contextmanager
from os.path import exists, join, abspath

from sugar_network.db import env
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.db.commands import CommandsProcessor, directory_command
from sugar_network.db.commands import document_command, property_command
from sugar_network.db.commands import to_int, to_list
from sugar_network.db.metadata import BlobProperty, StoredProperty
from sugar_network.db.metadata import PropertyMetadata
from sugar_network.toolkit import coroutine, util, exception, enforce


_logger = logging.getLogger('db.volume')


class Volume(dict):

    _flush_pool = []

    def __init__(self, root, documents, index_class=None, lazy_open=False):
        Volume._flush_pool.append(self)

        if index_class is None:
            index_class = IndexWriter

        self._root = abspath(root)
        _logger.info('Opening %r volume', self._root)

        if not exists(root):
            os.makedirs(root)
        self._index_class = index_class
        self._subscriptions = {}
        self._to_open = {}
        self.seqno = util.Seqno(join(self._root, 'seqno'))

        for document in documents:
            if isinstance(document, basestring):
                name = document.split('.')[-1]
            else:
                name = document.__name__.lower()
            if lazy_open:
                self._to_open[name] = document
            else:
                self[name] = self._open(name, document)

    @property
    def root(self):
        return self._root

    def close(self):
        """Close operations with the server."""
        _logger.info('Closing documents in %r', self._root)

        while self:
            __, cls = self.popitem()
            cls.close()

    def connect(self, callback, condition=None):
        self._subscriptions[callback] = condition or {}

    def disconnect(self, callback):
        if callback in self._subscriptions:
            del self._subscriptions[callback]

    def populate(self):
        for cls in self.values():
            for __ in cls.populate():
                coroutine.dispatch()

    def notify(self, event):
        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) != value:
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    exception(_logger, 'Failed to dispatch %r', event)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name):
        directory = self.get(name)
        if directory is None:
            enforce(name in self._to_open, env.BadRequest,
                    'Unknown %r document', name)
            directory = self[name] = self._open(name, self._to_open.pop(name))
        return directory

    def _open(self, name, document):
        if isinstance(document, basestring):
            mod = __import__(document, fromlist=[name])
            cls = getattr(mod, name.capitalize())
        else:
            cls = document
        directory = Directory(join(self._root, name), cls, self._index_class,
                self.notify, self.seqno)
        return directory


class VolumeCommands(CommandsProcessor):

    def __init__(self, volume):
        CommandsProcessor.__init__(self, volume)
        self.volume = volume

    @directory_command(method='POST',
            permissions=env.ACCESS_AUTH, mime_type='application/json')
    def create(self, request):
        with self._post(request, env.ACCESS_CREATE) as (directory, doc):
            enforce('guid' not in doc.props, env.Forbidden,
                    "Property 'guid' cannot be set manually")
            self.before_create(request, doc.props)
            if 'guid' not in doc.props:
                doc.props['guid'] = env.uuid()
            doc.guid = doc.props['guid']
            directory.create(doc.props)
            return doc.guid

    @directory_command(method='GET',
            arguments={'offset': to_int, 'limit': to_int, 'reply': to_list},
            mime_type='application/json')
    def find(self, document, reply, request):
        if not reply:
            reply = ['guid']
        self._preget(request)
        documents, total = self.volume[document].find(**request)
        result = [self._get_props(i, request, reply) for i in documents]
        return {'total': total, 'result': result}

    @document_command(method='GET', cmd='exists',
            mime_type='application/json')
    def exists(self, document, guid):
        directory = self.volume[document]
        return directory.exists(guid)

    @document_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update(self, request):
        with self._post(request, env.ACCESS_WRITE) as (directory, doc):
            modified = bool(doc.props)
            self.before_update(request, doc.props)
            if modified:
                directory.update(doc.guid, doc.props)

    @property_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update_prop(self, request, prop, url=None):
        if url:
            value = PropertyMetadata(url=url)
        elif request.content is None:
            value = request.content_stream
        else:
            value = request.content
        request.content = {prop: value}
        self.update(request)

    @document_command(method='DELETE',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def delete(self, document, guid):
        directory = self.volume[document]
        directory.delete(guid)

    @document_command(method='GET', arguments={'reply': to_list},
            mime_type='application/json')
    def get(self, document, guid, reply, request):
        if not reply:
            reply = []
            for prop in self.volume[document].metadata.values():
                if prop.permissions & env.ACCESS_READ and \
                        not (prop.permissions & env.ACCESS_LOCAL):
                    reply.append(prop.name)
        self._preget(request)
        doc = self.volume[document].get(guid)
        return self._get_props(doc, request, reply)

    @property_command(method='GET', mime_type='application/json')
    def get_prop(self, document, guid, prop, request, response, part=None):
        directory = self.volume[document]
        prop = directory.metadata[prop]
        doc = directory.get(guid)
        doc.request = request

        prop.assert_access(env.ACCESS_READ)

        if isinstance(prop, StoredProperty):
            value = doc.get(prop.name, request.accept_language)
            value = prop.on_get(doc, value)
            if value is None:
                value = prop.default
            return value
        else:
            meta = prop.on_get(doc, doc.meta(prop.name))
            enforce(meta is not None and ('blob' in meta or 'url' in meta),
                    env.NotFound, 'BLOB does not exist')
            return meta

    def before_create(self, request, props):
        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts

    def before_update(self, request, props):
        props['mtime'] = int(time.time())

    def after_post(self, doc):
        pass

    @contextmanager
    def _post(self, request, access):
        enforce(isinstance(request.content, dict), 'Invalid value')

        directory = self.volume[request['document']]
        if 'guid' in request:
            doc = directory.get(request['guid'])
        else:
            doc = directory.document_class(None, {})
        doc.request = request
        blobs = []

        for name, value in request.content.items():
            prop = directory.metadata[name]
            if isinstance(prop, BlobProperty) and access == env.ACCESS_WRITE:
                if doc.meta(name) is None:
                    prop.assert_access(env.ACCESS_CREATE)
                else:
                    prop.assert_access(env.ACCESS_WRITE)
            else:
                prop.assert_access(access)
            if isinstance(prop, BlobProperty):
                enforce(PropertyMetadata.is_blob(value), 'Invalid BLOB value')
                blobs.append((name, value))
            else:
                if prop.localized and isinstance(value, basestring):
                    value = {request.accept_language[0]: value}
                try:
                    doc.props[name] = prop.decode(value)
                except Exception, error:
                    error = 'Value %r for %r property is invalid: %s' % \
                            (value, prop.name, error)
                    exception(error)
                    raise RuntimeError(error)

        if access == env.ACCESS_CREATE:
            for name, prop in directory.metadata.items():
                if not isinstance(prop, BlobProperty) and \
                        name not in request.content and \
                        (prop.default is not None or prop.on_set is not None):
                    doc.props[name] = prop.default

        for name, value in doc.props.items():
            prop = directory.metadata[name]
            if not isinstance(prop, BlobProperty) and prop.on_set is not None:
                doc.props[name] = prop.on_set(doc, value)

        yield directory, doc

        for name, value in blobs:
            prop = directory.metadata[name]
            if prop.on_set is not None:
                value = prop.on_set(doc, value)
            directory.set_blob(doc.guid, name, value,
                    mime_type=request.content_type)

        self.after_post(doc)

    def _preget(self, request):
        metadata = self.volume[request['document']].metadata
        reply = request.setdefault('reply', [])
        if reply:
            for prop in reply:
                metadata[prop].assert_access(env.ACCESS_READ)
        else:
            reply.append('guid')

    def _get_props(self, doc, request, props):
        result = {}
        metadata = doc.metadata
        doc.request = request
        for name in props:
            prop = metadata[name]
            value = prop.on_get(doc, doc.get(name, request.accept_language))
            if value is None:
                value = prop.default
            elif request.static_prefix and isinstance(value, PropertyMetadata):
                value = value.get('url')
                if value is None:
                    value = '/'.join(['', metadata.name, doc.guid, name])
                if value.startswith('/'):
                    value = request.static_prefix + value
            result[name] = value
        return result
