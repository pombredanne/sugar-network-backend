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

import os
import logging
from os.path import exists, join, abspath

from sugar_network import toolkit
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.db.blobs import Blobs
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import http, coroutine, enforce


_logger = logging.getLogger('db.volume')


class Volume(dict):

    _flush_pool = []

    def __init__(self, root, resources, index_class=None):
        Volume._flush_pool.append(self)
        self.resources = {}
        self.mute = False
        self._populators = coroutine.Pool()

        if index_class is None:
            index_class = IndexWriter

        self._root = abspath(root)
        _logger.info('Opening %r volume', self._root)

        if not exists(root):
            os.makedirs(root)
        self._index_class = index_class
        self.seqno = toolkit.Seqno(join(self._root, 'var', 'seqno'))
        self.blobs = Blobs(root, self.seqno)

        for document in resources:
            if isinstance(document, basestring):
                name = document.split('.')[-1]
            else:
                name = document.__name__.lower()
            self.resources[name] = document

    @property
    def root(self):
        return self._root

    @property
    def empty(self):
        for directory in self.values():
            if not directory.empty:
                return False
        return True

    @property
    def has_seqno(self):
        for directory in self.values():
            if directory.has_seqno:
                return True
        return False

    @property
    def has_noseqno(self):
        for directory in self.values():
            if directory.has_noseqno:
                return True
        return False

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

    def broadcast(self, event):
        if not self.mute:
            if event['event'] == 'commit':
                this.broadcast(event)
            else:
                this.localcast(event)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getitem__(self, name):
        dir_ = self.get(name)
        if dir_ is None:
            enforce(name in self.resources, http.BadRequest,
                    'Unknown %r resource', name)
            resource = self.resources[name]
            if isinstance(resource, basestring):
                mod = __import__(resource, fromlist=[name])
                cls = getattr(mod, name.capitalize())
            else:
                cls = resource
            dir_ = Directory(self._root, cls, self._index_class, self.seqno,
                    self.broadcast)
            self._populators.spawn(self._populate, dir_)
            self[name] = dir_
        return dir_

    def _populate(self, directory):
        for __ in directory.populate():
            coroutine.dispatch()
