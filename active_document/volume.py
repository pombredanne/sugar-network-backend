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
from os.path import exists, join, abspath, isdir

from active_document import env
from active_document.directory import Directory
from active_document.index import IndexWriter
from active_document.commands import document_command, directory_command
from active_document.commands import CommandsProcessor, property_command
from active_document.commands import to_int, to_list
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
            permissions=env.ACCESS_AUTH)
    def create(self, document, request):
        directory = self.volume[document]
        props = request.content
        blobs = {}

        enforce('guid' not in props, env.Forbidden,
                'Property "guid" cannot be set manually')

        for name, value in props.items():
            prop = directory.metadata[name]
            prop.assert_access(env.ACCESS_CREATE)
            if isinstance(prop, BlobProperty):
                blobs[name] = props.pop(name)
            else:
                props[name] = self._prepost(request, prop, value)

        self.before_create(request, props)
        guid = directory.create(props)

        for name, value in blobs.items():
            directory.set_blob(guid, name, value)

        return guid

    @directory_command(method='GET',
            arguments={'offset': to_int, 'limit': to_int, 'reply': to_list})
    def find(self, document, reply, request):
        if reply is None:
            reply = request['reply'] = ['guid']
        elif 'guid' not in reply:
            reply.append('guid')

        directory = self.volume[document]
        for i in reply:
            directory.metadata[i].assert_access(env.ACCESS_READ)

        documents, total = directory.find(**request)
        result = [i.properties(reply, request.accept_language or self._lang)
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
        blobs = {}

        for name, value in props.items():
            prop = directory.metadata[name]
            prop.assert_access(env.ACCESS_WRITE)
            if isinstance(prop, BlobProperty):
                blobs[name] = props.pop(name)
            else:
                props[name] = self._prepost(request, prop, value)

        self.before_update(request, props)
        directory.update(guid, props)

        for name, value in blobs.items():
            directory.set_blob(guid, name, value)

    @property_command(method='PUT',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def update_prop(self, document, guid, prop, request, url=None):
        directory = self.volume[document]

        prop = directory.metadata[prop]
        prop.assert_access(env.ACCESS_WRITE)

        if not isinstance(prop, BlobProperty):
            request.content = {prop.name: request.content}
            return self.update(document, guid, request)

        if url is not None:
            directory.set_blob(guid, prop.name, url=url)
        elif request.content is not None:
            directory.set_blob(guid, prop.name, request.content)
        else:
            directory.set_blob(guid, prop.name, request.content_stream,
                    request.content_length)

    @document_command(method='DELETE',
            permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
    def delete(self, document, guid):
        directory = self.volume[document]
        directory.delete(guid)

    @document_command(method='GET', arguments={'reply': to_list})
    def get(self, document, guid, request, reply=None):
        directory = self.volume[document]
        doc = directory.get(guid)

        for i in reply or []:
            directory.metadata[i].assert_access(env.ACCESS_READ)

        return doc.properties(reply, request.accept_language or self._lang)

    @property_command(method='GET', arguments={'seqno': to_int})
    def get_prop(self, document, guid, prop, request, response, seqno=None,
            part=None):
        directory = self.volume[document]
        prop = directory.metadata[prop]
        doc = directory.get(guid)

        prop.assert_access(env.ACCESS_READ)

        if not isinstance(prop, BlobProperty):
            return doc.get(prop.name, request.accept_language or self._lang)

        meta = doc.meta(prop.name)
        enforce(meta is not None, env.NotFound, 'BLOB does not exist')

        url = meta.url(part)
        if url is not None:
            if not isinstance(url, basestring):
                response.content_type = 'application/json'
                return url
            raise env.Redirect(url)

        if seqno is not None and seqno >= meta['seqno']:
            response.content_length = 0
            response.content_type = prop.mime_type
            return None

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

    def _prepost(self, request, prop, value):
        if prop.localized and isinstance(value, basestring):
            return {(request.accept_language or self._lang)[0]: value}
        else:
            return value


def _file_reader(path):
    with file(path, 'rb') as f:
        while True:
            chunk = f.read(sockets.BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
