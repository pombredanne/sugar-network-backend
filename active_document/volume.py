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
import json
import time
import logging
from cStringIO import StringIO
from functools import partial
from os.path import exists, join, abspath, isdir

from active_document import env
from active_document.directory import Directory
from active_document.index import IndexWriter
from active_document.commands import document_command, directory_command
from active_document.commands import CommandsProcessor, property_command
from active_document.metadata import BlobProperty
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

    def _notification_cb(self, event, document):
        if event['event'] == 'update' and 'props' in event and \
                'deleted' in event['props'].get('layer', []):
            event['event'] = 'delete'
            del event['props']
        event['document'] = document

        for callback, condition in self._subscriptions.items():
            for key, value in condition.items():
                if event.get(key) not in ('*', value):
                    break
            else:
                try:
                    callback(event)
                except Exception:
                    util.exception(_logger, 'Failed to dispatch %r', event)

    def _open(self, name, document):
        if isinstance(document, basestring):
            mod = __import__(document, fromlist=[name])
            cls = getattr(mod, name.capitalize())
        else:
            cls = document
        directory = Directory(join(self._root, name), cls, self._index_class,
                partial(self._notification_cb, document=name), self.seqno)
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

    @directory_command(method='POST',
            permissions=env.ACCESS_AUTH)
    def create(self, document, request):
        directory = self.volume[document]
        props = request.content
        enforce('guid' not in props, env.Forbidden,
                'Property "guid" cannot be set manually')
        for name, value in props.items():
            prop = directory.metadata[name]
            prop.assert_access(env.ACCESS_CREATE)
            props[name] = self._prepost(request, prop, value)
        self.before_create(request, props)
        return directory.create(props)

    @directory_command(method='GET')
    def find(self, document, request, offset=None, limit=None, query=None,
            reply=None, order_by=None, group_by=None, **kwargs):
        directory = self.volume[document]
        offset = _to_int('offset', offset)
        limit = _to_int('limit', limit)
        reply = _to_list(reply) or []
        reply.append('guid')

        for i in reply:
            directory.metadata[i].assert_access(env.ACCESS_READ)

        # TODO until implementing layers support
        layer = kwargs.get('layer', ['public'])
        if isinstance(layer, basestring):
            layer = [layer]
        if 'deleted' in layer:
            _logger.warning('Requesting "deleted" layer')
            layer.remove('deleted')

        documents, total = directory.find(offset=offset, limit=limit,
                query=query, reply=reply, order_by=order_by, group_by=group_by,
                **kwargs)
        result = [i.properties(reply, request.accept_language)
                for i in documents]

        return {'total': total.value, 'result': result}

    @document_command(method='GET', cmd='exists')
    def exists(self, document, guid):
        directory = self.volume[document]
        return directory.exists(guid)

    @document_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update(self, document, guid, request):
        directory = self.volume[document]
        props = request.content
        for name, value in props.items():
            prop = directory.metadata[name]
            prop.assert_access(env.ACCESS_WRITE)
            props[name] = self._prepost(request, prop, value)
        self.before_update(request, props)
        directory.update(guid, props)

    @property_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update_prop(self, document, guid, prop, request, url=None):
        directory = self.volume[document]

        prop = directory.metadata[prop]
        prop.assert_access(env.ACCESS_WRITE)

        if not isinstance(prop, BlobProperty):
            props = {prop.name: self._prepost(request, prop, request.content)}
            self.before_update(request, props)
            directory.update(guid, props)
            return

        if url is not None:
            directory.set_blob(guid, prop.name, url)
        elif request.content is not None:
            # TODO Avoid double JSON processins
            content_stream = StringIO()
            json.dump(request.content, content_stream)
            content_stream.seek(0)
            directory.set_blob(guid, prop.name, content_stream)
        else:
            directory.set_blob(guid, prop.name, request.content_stream,
                    request.content_length)

    @document_command(method='DELETE',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def delete(self, document, guid):
        directory = self.volume[document]
        directory.delete(guid)

    @document_command(method='GET')
    def get(self, document, guid, request, reply=None):
        directory = self.volume[document]
        doc = directory.get(guid)

        reply = _to_list(reply)
        if reply:
            for i in reply:
                directory.metadata[i].assert_access(env.ACCESS_READ)

        enforce('deleted' not in doc['layer'], env.NotFound,
                'Document is not found')

        return doc.properties(reply, request.accept_language)

    @property_command(method='GET')
    def get_prop(self, document, guid, prop, request, response, seqno=None):
        directory = self.volume[document]
        doc = directory.get(guid)

        directory.metadata[prop].assert_access(env.ACCESS_READ)

        if not isinstance(directory.metadata[prop], BlobProperty):
            return doc.get(prop, request.accept_language)

        meta = doc.meta(prop)
        enforce(meta is not None, env.NotFound, 'BLOB does not exist')

        if 'url' in meta:
            raise env.Redirect(meta['url'])

        seqno = _to_int('seqno', seqno)
        if seqno is not None and seqno >= meta['seqno']:
            response.content_length = 0
            response.content_type = directory.metadata[prop].mime_type
            return None

        path = meta['path']
        if isdir(path):
            dir_info, dir_reader = sockets.encode_directory(path)
            response.content_length = dir_info.content_length
            response.content_type = dir_info.content_type
            return dir_reader
        else:
            response.content_length = os.stat(path).st_size
            response.content_type = directory.metadata[prop].mime_type
            return _file_reader(path)

    def before_create(self, request, props):
        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts
        # TODO until implementing layers support
        props['layer'] = ['public']

    def before_update(self, request, props):
        props['mtime'] = int(time.time())

    def _prepost(self, request, prop, value):
        if prop.localized and request.accept_language:
            return {request.accept_language[0]: value}
        else:
            return value


def _to_int(name, value):
    if isinstance(value, basestring):
        enforce(value.isdigit(),
                'Argument %r should be an integer value', name)
        value = int(value)
    return value


def _to_list(value):
    if isinstance(value, basestring):
        value = value.split(',')
    return value


def _file_reader(path):
    with file(path, 'rb') as f:
        while True:
            chunk = f.read(sockets.BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
