# Copyright (C) 2012, Aleksey Lim
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

import bisect
import logging

import xapian
import gevent

from active_document import util, index_queue, env
from active_document.storage import Storage
from active_document.index import IndexReader, IndexWriter, Total


_logger = logging.getLogger('ad.index_proxy')


class IndexProxy(IndexReader):

    def __init__(self, metadata):
        IndexReader.__init__(self, metadata)
        self._cache = {}
        self._wait_for_reopen_job = gevent.spawn(self._wait_for_reopen)

    def commit(self):
        index_queue.commit_and_wait(self.metadata.name)

    def close(self):
        self._wait_for_reopen_job.kill()

    def get_cache(self, guid):
        cached = self._cache.get(guid)
        if cached is not None:
            return cached.properties

    def store(self, guid, properties, new, pre_cb=None, post_cb=None):
        _logger.debug('Push store request to "%s"\'s queue for "%s"',
                self.metadata.name, guid)
        if properties and new is not None:
            # Needs to be called before `index_queue.put()`
            # to give it a chance to read original properties from the storage
            self._cache_update(guid, properties, new)
        index_queue.put(self.metadata.name, IndexWriter.store,
                guid, properties, new, pre_cb, post_cb)

    def delete(self, guid, post_cb=None):
        _logger.debug('Push delete request to "%s"\'s queue for "%s"',
                self.metadata.name, guid)
        index_queue.put(self.metadata.name, IndexWriter.delete, guid, post_cb)

    def find(self, offset, limit, request, query=None, reply=None,
            order_by=None):
        if self._db is None:
            if not self._open():
                return [], Total(0)

        def direct_find():
            return IndexReader.find(self, offset, limit, request, query, reply,
                    order_by)

        if 'guid' in request:
            documents, total = direct_find()
            cache = self._cache.get(request['guid'])
            if cache is None:
                return documents, total

            def patched_guid_find():
                processed = False
                for guid, props in documents:
                    processed = True
                    props.update(cache.properties)
                    yield guid, props
                if not processed:
                    yield cache.guid, cache.properties

            return patched_guid_find(), total

        if not self._cache:
            return direct_find()

        adds, deletes, updates = self._patch_find(request)
        if not adds and not deletes and not updates:
            return direct_find()

        orig_limit = limit
        limit += len(deletes)
        documents, total = direct_find()
        total.value += len(adds)

        def patched_find(orig_limit):
            for guid, props in documents:
                if orig_limit < 1:
                    break
                if guid in deletes:
                    total.value -= 1
                    continue
                cache = updates.get(guid)
                if cache is not None:
                    props.update(cache.properties)
                yield guid, props
                orig_limit -= 1

            for doc in adds:
                if orig_limit < 1:
                    break
                yield doc.guid, doc.properties
                orig_limit -= 1

        return patched_find(orig_limit), total

    def _patch_find(self, request):
        adds = []
        deletes = set()
        updates = {}

        terms = set()
        for prop_name, value in request.items():
            if _is_term(self.metadata[prop_name]):
                terms.add((prop_name, value))

        for cache in self._cache.values():
            if cache.new:
                if terms.issubset(cache.terms):
                    bisect.insort(adds, cache)
            else:
                if terms:
                    if terms.issubset(cache.terms):
                        if not terms.issubset(cache.orig_terms):
                            bisect.insort(adds, cache)
                            continue
                    else:
                        if terms.issubset(cache.orig_terms):
                            deletes.add(cache.guid)
                        continue
                updates[cache.guid] = cache

        return adds, deletes, updates

    def _open(self):
        try:
            self._db = xapian.Database(self.metadata.ensure_path('index', ''))
        except xapian.DatabaseOpeningError:
            util.exception(_logger, 'Cannot open "%s" RO index',
                    self.metadata.name)
            return False
        _logger.debug('Opened "%s" RO index', self.metadata.name)
        return True

    def _wait_for_reopen(self):
        while True:
            index_queue.wait_commit(self.metadata.name)
            self._cache.clear()
            try:
                if self._db is not None:
                    self._db.reopen()
            except Exception:
                util.exception(_logger, 'Cannot reopen "%s" RO index',
                        self.metadata.name)
                self._db = None

    def _cache_update(self, guid, properties, new):
        existing = self._cache.get(guid)
        if existing is None:
            self._cache[guid] = \
                    _CachedDocument(self.metadata, guid, properties, new)
        else:
            existing.update(properties)


class _CachedDocument(object):

    def __init__(self, metadata, guid, properties, new):
        self.guid = guid
        self.properties = properties.copy()
        self.new = new
        self.terms = set()
        self.orig_terms = set()
        self._term_names = []

        if not new:
            record = Storage(metadata).get(guid)
        for prop_name, prop in metadata.items():
            if _is_term(prop):
                self._term_names.append(prop_name)
                if not new:
                    self.orig_terms.add((prop_name, record.get(prop_name)))
        self._update_terms()

    def __sort__(self, other):
        return cmp(self.guid, other.guid)

    def update(self, properties):
        self.properties.update(properties)
        self._update_terms()

    def _update_terms(self):
        self.terms.clear()
        orig_terms = dict(self.orig_terms)
        for prop_name in self._term_names:
            term = self.properties.get(prop_name, orig_terms.get(prop_name))
            self.terms.add((prop_name, term))


def _is_term(prop):
    return prop.permissions & env.ACCESS_WRITE
