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
from copy import deepcopy
from os.path import exists, join, abspath

from sugar_network import toolkit
from sugar_network.db.directory import Directory
from sugar_network.db.index import IndexWriter
from sugar_network.db.blobs import Blobs
from sugar_network.toolkit import http, coroutine, ranges, enforce


_logger = logging.getLogger('db.volume')


class Volume(dict):

    _flush_pool = []

    def __init__(self, root, documents, index_class=None):
        Volume._flush_pool.append(self)
        self.resources = {}
        self._populators = coroutine.Pool()

        if index_class is None:
            index_class = IndexWriter

        self._root = abspath(root)
        _logger.info('Opening %r volume', self._root)

        if not exists(root):
            os.makedirs(root)
        self._index_class = index_class
        self.seqno = toolkit.Seqno(join(self._root, 'var', 'db.seqno'))
        self.releases_seqno = toolkit.Seqno(
                join(self._root, 'var', 'releases.seqno'))
        self.blobs = Blobs(root, self.seqno)

        for document in documents:
            if isinstance(document, basestring):
                name = document.split('.')[-1]
            else:
                name = document.__name__.lower()
            self.resources[name] = document

    @property
    def root(self):
        return self._root

    def close(self):
        """Close operations with the server."""
        _logger.info('Closing documents in %r', self._root)
        self._populators.kill()
        while self:
            __, cls = self.popitem()
            cls.close()
        self.releases_seqno.commit()

    def populate(self):
        for cls in self.values():
            for __ in cls.populate():
                coroutine.dispatch()

    def diff(self, r, files=None, one_way=False):
        last_seqno = None
        try:
            for resource, directory in self.items():
                if one_way and directory.resource.one_way:
                    continue
                directory.commit()
                yield {'resource': resource}
                for start, end in r:
                    query = 'seqno:%s..' % start
                    if end:
                        query += str(end)
                    docs, __ = directory.find(query=query, order_by='seqno')
                    for doc in docs:
                        seqno, patch = doc.diff(r)
                        if not patch:
                            continue
                        yield {'guid': doc.guid, 'patch': patch}
                        last_seqno = max(last_seqno, seqno)
            for blob in self.blobs.diff(r):
                seqno = int(blob.pop('x-seqno'))
                yield blob
                last_seqno = max(last_seqno, seqno)
            for dirpath in files or []:
                for blob in self.blobs.diff(r, dirpath):
                    seqno = int(blob.pop('x-seqno'))
                    yield blob
                    last_seqno = max(last_seqno, seqno)
        except StopIteration:
            pass

        if last_seqno:
            commit_r = deepcopy(r)
            ranges.exclude(commit_r, last_seqno + 1, None)
            ranges.exclude(r, None, last_seqno)
            yield {'commit': commit_r}

    def patch(self, records):
        directory = None
        commit_r = []
        merged_r = []
        seqno = None

        for record in records:
            resource_ = record.get('resource')
            if resource_:
                directory = self[resource_]
                continue

            if 'guid' in record:
                seqno = directory.patch(record['guid'], record['patch'], seqno)
                continue

            if 'content-length' in record:
                if seqno is None:
                    seqno = self.seqno.next()
                self.blobs.patch(record, seqno)
                continue

            commit = record.get('commit')
            if commit is not None:
                ranges.include(commit_r, commit)
                continue

        if seqno is not None:
            ranges.include(merged_r, seqno, seqno)
        return commit_r, merged_r

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
            dir_ = Directory(self._root, cls, self._index_class, self.seqno)
            self._populators.spawn(self._populate, dir_)
            self[name] = dir_
        return dir_

    def _populate(self, directory):
        for __ in directory.populate():
            coroutine.dispatch()
