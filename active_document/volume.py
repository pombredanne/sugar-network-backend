# Copyright (C) 2011-2012, Aleksey Lim
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
import imp
import urllib2
import inspect
import logging
from os.path import exists, basename, join
from gettext import gettext as _

from active_document import env, gthread
from active_document.document import Document
from active_document.directory import Directory
from active_document.index import IndexWriter
from active_document.commands import document_command, directory_command
from active_document.metadata import BlobProperty
from active_document.util import enforce


_PAGE_SIZE = 1024 * 10

_logger = logging.getLogger('active_document.volume')


class _Volume(dict):

    def __init__(self, root, document_classes, index_class, extra_props):
        self._signal = None

        self._root = root
        if not exists(root):
            os.makedirs(root)

        _logger.info(_('Opening documents in %r'), root)

        if type(document_classes) not in (tuple, list):
            document_classes = _walk_classes(document_classes)

        if extra_props is None:
            extra_props = {}

        for cls in document_classes:
            name = cls.__name__.lower()
            directory = Directory(join(root, name), cls, index_class,
                    extra_props.get(name),
                    lambda event: self._notification_cb(name, event))
            self[name] = directory

    def close(self):
        """Close operations with the server."""
        _logger.info(_('Closing documents in %r'), self._root)

        while self:
            __, cls = self.popitem()
            cls.close()

    def connect(self, callback):
        # TODO Replace by regular events handler
        enforce(self._signal is None)
        self._signal = callback

    def _notification_cb(self, document, event):
        if env.only_commits_notification.value and event['event'] != 'commit':
            return

        event['document'] = document
        if event['event'] == 'update':
            if 'deleted' in event['props'].get('layers', []):
                event['event'] = 'delete'
                del event['props']
        if 'props' in event:
            del event['props']

        signal = self._signal
        if signal is not None:
            signal(event)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name):
        enforce(name in self, _('Unknow %r document'), name)
        return self.get(name)


class SingleVolume(_Volume):

    def __init__(self, root, document_classes, extra_props=None):
        enforce(env.index_write_queue.value > 0,
                _('The active_document.index_write_queue.value should be > 0'))

        _Volume.__init__(self, root, document_classes, IndexWriter,
                extra_props)

        for cls in self.values():
            for __ in cls.populate():
                gthread.dispatch()


@directory_command(method='POST',
        permissions=env.ACCESS_AUTH)
def _create(directory, request):
    props = request.content
    for i in props.keys():
        directory.metadata[i].assert_access(env.ACCESS_CREATE)
    return directory.create(props)


@directory_command(method='GET')
def _find(directory, offset=None, limit=None, query=None, reply=None,
        order_by=None, **kwargs):
    offset = _to_int('offset', offset)
    limit = _to_int('limit', limit)
    reply = _to_list(reply) or ['guid']

    for i in reply:
        directory.metadata[i].assert_access(env.ACCESS_READ)

    # TODO until implementing layers support
    kwargs['layers'] = 'public'

    documents, total = directory.find(offset=offset, limit=limit,
            query=query, reply=reply, order_by=order_by, **kwargs)
    result = [i.properties(reply or ['guid']) for i in documents]

    return {'total': total.value, 'result': result}


@document_command(method='PUT',
        permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
def _update(directory, document, request, prop=None, url=None):
    if prop is None:
        props = request.content
        for i in props.keys():
            directory.metadata[i].assert_access(env.ACCESS_WRITE)
        directory.update(document.guid, props)
        return

    directory.metadata[prop].assert_access(env.ACCESS_WRITE)

    if not isinstance(directory.metadata[prop], BlobProperty):
        directory.update(document.guid, {prop: request.content})
        return

    if url is not None:
        _logger.info(_('Download BLOB for %r from %r'), prop, url)
        stream = urllib2.urlopen(url)
        try:
            directory.set_blob(document.guid, prop, stream)
        finally:
            stream.close()
    else:
        directory.set_blob(document.guid, prop, request.content_stream,
                request.content_length)


@document_command(method='DELETE',
        permissions=env.ACCESS_AUTH | env.ACCESS_AUTHOR)
def _delete(directory, document, prop=None):
    enforce(prop is None, _('Properties cannot be deleted'))
    # TODO until implementing layers support
    directory.update(document.guid, {'layers': ['deleted']})


@document_command(method='GET')
def _get(directory, document, response, prop=None, reply=None):
    if not prop:
        reply = _to_list(reply)
        if reply:
            for i in reply:
                directory.metadata[i].assert_access(env.ACCESS_READ)
        enforce('deleted' not in document['layers'], env.NotFound,
                _('Document is not found'))
        return document.properties(reply)

    directory.metadata[prop].assert_access(env.ACCESS_READ)

    if not isinstance(directory.metadata[prop], BlobProperty):
        return document[prop]

    stream = directory.get_blob(document.guid, prop)

    response.content_length = 0
    if stream is not None:
        stat = directory.stat_blob(document.guid, prop)
        if stat and stat['size']:
            response.content_length = stat['size']
    response.content_type = directory.metadata[prop].mime_type

    def send(stream):
        while True:
            chunk = stream.read(_PAGE_SIZE)
            if not chunk:
                break
            yield chunk

    if stream is not None:
        return send(stream)


@document_command(method='GET', cmd='stat-blob')
def _stat_blob(directory, document, prop=None):
    enforce(prop is not None, _('No BLOB property specified'))
    directory.metadata[prop].assert_access(env.ACCESS_READ)
    stat = directory.stat_blob(document.guid, prop)
    if not stat:
        return None
    return {'size': stat['size'], 'sha1sum': stat['sha1sum']}


def _to_int(name, value):
    if type(value) in (str, unicode):
        enforce(value.isdigit(),
                _('Argument %r should be an integer value'), name)
        value = int(value)
    return value


def _to_list(value):
    if type(value) in (str, unicode):
        value = value.split(',')
    return value


def _walk_classes(path):
    classes = set()

    for filename in os.listdir(path):
        if filename == '__init__.py' or not filename.endswith('.py'):
            continue

        mod_name = basename(filename)[:-3]
        fp, pathname, description = imp.find_module(mod_name, [path])
        try:
            mod = imp.load_module(mod_name, fp, pathname, description)
        finally:
            if fp:
                fp.close()

        for __, cls in inspect.getmembers(mod):
            if inspect.isclass(cls) and issubclass(cls, Document):
                classes.add(cls)

    for cls in list(classes):
        if [i for i in classes if i is not cls and issubclass(i, cls)]:
            classes = [i for i in classes if i.__name__ != cls.__name__]

    return classes
