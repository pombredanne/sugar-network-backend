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
import logging
from os.path import exists, join, abspath

from sugar_network import toolkit
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.toolkit import http, coroutine, enforce


_logger = logging.getLogger('db.volume')


class Volume(dict):

    _flush_pool = []

    def __init__(self, root, documents, broadcast=None, index_class=None,
            lazy_open=False):
        Volume._flush_pool.append(self)
        self.resources = {}
        self.broadcast = broadcast or (lambda event: None)
        self._populators = coroutine.Pool()

        if index_class is None:
            index_class = IndexWriter

        self._root = abspath(root)
        _logger.info('Opening %r volume', self._root)

        if not exists(root):
            os.makedirs(root)
        self._index_class = index_class
        self.seqno = toolkit.Seqno(join(self._root, 'seqno'))

        for document in documents:
            if isinstance(document, basestring):
                name = document.split('.')[-1]
            else:
                name = document.__name__.lower()
            self.resources[name] = document
            if not lazy_open:
                self[name] = self._open(name, document)

    @property
    def root(self):
        return self._root

    def mtime(self, name):
        path = join(self._root, name, 'index', 'mtime')
        if exists(path):
            return int(os.stat(path).st_mtime)
        else:
            return 0

    def close(self):
        """Close operations with the server."""
        _logger.info('Closing documents in %r', self._root)
        self._populators.kill()
        while self:
            __, cls = self.popitem()
            cls.close()

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
            enforce(name in self.resources, http.BadRequest,
                    'Unknown %r resource', name)
            directory = self[name] = self._open(name, self.resources[name])
        return directory

    def _open(self, name, resource):
        if isinstance(resource, basestring):
            mod = __import__(resource, fromlist=[name])
            cls = getattr(mod, name.capitalize())
        else:
            cls = resource
        directory = Directory(join(self._root, name), cls, self._index_class,
                lambda event: self._broadcast(name, event), self.seqno)
        self._populators.spawn(self._populate, directory)
        return directory

    def _populate(self, directory):
        for __ in directory.populate():
            coroutine.dispatch()

    def _broadcast(self, resource, event):
        if self.broadcast is not None:
            event['resource'] = resource
            self.broadcast(event)
