# Copyright (C) 2011-2012 Aleksey Lim
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
from os.path import exists, join, abspath, isdir

from active_document import env
from active_document.directory import Directory
from active_document.index import IndexWriter
from active_document.commands import document_command, directory_command
from active_document.commands import CommandsProcessor, property_command
from active_document.commands import to_int, to_list
from active_document.metadata import BlobProperty, BrowsableProperty
from active_document.metadata import PropertyMeta
from active_toolkit import coroutine, util, sockets, enforce


_logger = logging.getLogger('active_document.volume')


class _Volume(dict):

    def __init__(self, root, documents, index_class, lazy_open):
        self._root = abspath(root)
        _logger.info('Opening %r volume', self._root)

        if not exists(root):
            os.makedirs(root)
        self._index_class = index_class
        self._subscriptions = {}
        self._to_open = {}
        self.seqno = env.Seqno(join(self._root, 'seqno'))

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
                if event.get(key) not in ('*', value):
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, 'Failed to dispatch %r', event)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name):
        directory = self.get(name)
        if directory is None:
            enforce(name in self._to_open, 'Unknown %r document', name)
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


class SingleVolume(_Volume):

    def __init__(self, root, document_classes, lazy_open=False):
        enforce(env.index_write_queue.value > 0,
                'The active_document.index_write_queue.value should be > 0')
        _Volume.__init__(self, root, document_classes, IndexWriter, lazy_open)


class VolumeCommands(CommandsProcessor):

    def __init__(self, volume):
        CommandsProcessor.__init__(self, volume)
        self.volume = volume
        self._lang = [env.default_lang()]

    @directory_command(method='POST',
            permissions=env.ACCESS_AUTH, mime_type='application/json')
    def create(self, request):
        with self._post(request, env.ACCESS_CREATE) as (directory, doc):
            enforce('guid' not in doc.props, env.Forbidden,
                    "Property 'guid' cannot be set manually")
            self.before_create(request, doc.props)
            doc.guid = directory.create(doc.props)
            return doc.guid

    @directory_command(method='GET',
            arguments={'offset': to_int, 'limit': to_int, 'reply': to_list},
            mime_type='application/json')
    def find(self, document, request):
        reply = request.setdefault('reply', ['guid'])
        if 'guid' not in reply:
            reply.append('guid')
        self._preget(request)
        documents, total = self.volume[document].find(**request)
        result = [self._get_props(i, request) for i in documents]
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
            self.before_update(request, doc.props)
            directory.update(doc.guid, doc.props)

    @property_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update_prop(self, request, prop, url=None):
        if url:
            value = PropertyMeta(url=url)
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
    def get(self, document, guid, request):
        self._preget(request)
        doc = self.volume[document].get(guid)
        return self._get_props(doc, request)

    @property_command(method='GET', mime_type='application/json')
    def get_prop(self, document, guid, prop, request, response, part=None):
        directory = self.volume[document]
        prop = directory.metadata[prop]
        doc = directory.get(guid)
        doc.request = request
        meta = doc.meta(prop.name)

        if meta is not None:
            if request.if_modified_since:
                if meta['mtime'] <= request.if_modified_since:
                    raise env.NotModified()
            response.last_modified = meta['mtime']

        prop.assert_access(env.ACCESS_READ)

        if not isinstance(prop, BlobProperty):
            value = doc.get(prop.name, request.accept_language or self._lang)
            return prop.on_get(doc, value)

        meta = prop.on_get(doc, meta)
        enforce(meta is not None, env.NotFound, 'BLOB does not exist')

        url = meta.url(part)
        if url is not None:
            if not isinstance(url, basestring):
                return url
            raise env.Redirect(url)

        enforce('path' in meta, env.NotFound, 'BLOB does not exist')

        path = meta['path']
        if isdir(path):
            dir_info, dir_reader = sockets.encode_directory(path)
            response.content_length = dir_info.content_length
            response.content_type = dir_info.content_type
            return dir_reader
        else:
            response.content_length = os.stat(path).st_size
            response.content_type = prop.mime_type
            return _file_reader(path)

    def before_create(self, request, props):
        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts

    def before_update(self, request, props):
        props['mtime'] = int(time.time())

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
            prop.assert_access(access)
            value = prop.on_set(doc, value)
            if isinstance(prop, BlobProperty):
                enforce(PropertyMeta.is_blob(value), 'Invalid BLOB value')
                blobs.append((name, value))
            else:
                if prop.localized and isinstance(value, basestring):
                    value = {(request.accept_language or self._lang)[0]: value}
                doc.props[name] = value

        yield directory, doc

        for name, value in blobs:
            directory.set_blob(doc.guid, name, value)

    def _preget(self, request):
        metadata = self.volume[request['document']].metadata
        reply = request.setdefault('reply', [])
        if reply:
            for prop in reply:
                metadata[prop].assert_access(env.ACCESS_READ)
        else:
            for prop in metadata.values():
                if isinstance(prop, BrowsableProperty) and \
                        prop.permissions & env.ACCESS_READ:
                    reply.append(prop.name)

    def _get_props(self, doc, request):
        result = {}
        lang = request.accept_language or self._lang
        metadata = doc.metadata
        doc.request = request
        for prop in request['reply']:
            result[prop] = metadata[prop].on_get(doc, doc.get(prop, lang))
        return result


def _file_reader(path):
    with file(path, 'rb') as f:
        while True:
            chunk = f.read(sockets.BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
