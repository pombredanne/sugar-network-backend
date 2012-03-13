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
from os.path import exists

import xapian
import gevent

from active_document import util, index_queue, env
from active_document.storage import Storage
from active_document.index import IndexReader, IndexWriter, Total
from active_document.metadata import StoredProperty


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
            self._open()

        def direct_find():
            if self._db is None:
                return [], Total(0)
            else:
                return IndexReader.find(self, offset, limit, request, query,
                        reply, order_by)

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
            prop = self.metadata[prop_name]
            if not _is_term(prop):
                continue
            try:
                value = prop.convert(value)
            except ValueError, error:
                _logger.debug('Wrong request property value %r for "%s" ' \
                        'property, thus the whole request is empty: %s',
                        value, prop_name, error)
                return None, None, None
            terms.add(_TermValue(prop, value))

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
        path = self.metadata.path('index')
        if not exists(path):
            return
        try:
            self._db = xapian.Database(path)
            _logger.debug('Opened "%s" RO index', self.metadata.name)
        except xapian.DatabaseOpeningError:
            util.exception(_logger, 'Cannot open "%s" RO index',
                    self.metadata.name)

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
        self._term_props = []

        if not new:
            record = Storage(metadata).get(guid)
        for prop_name, prop in metadata.items():
            if not _is_term(prop):
                continue
            self._term_props.append(prop)
            if not new:
                self.orig_terms.add(_TermValue(prop, record.get(prop_name)))

        self._update_terms()

    def __sort__(self, other):
        return cmp(self.guid, other.guid)

    def update(self, properties):
        self.properties.update(properties)
        self._update_terms()

    def _update_terms(self):
        self.terms.clear()
        orig_terms = {}
        for i in self.orig_terms:
            orig_terms[i.prop] = i.value
        for prop in self._term_props:
            term = self.properties.get(prop.name, orig_terms.get(prop))
            self.terms.add(_TermValue(prop, term))


class _TermValue:

    def __init__(self, prop, value):
        self.prop = prop
        self.value = value

    def __cmp__(self, other):
        result = cmp(self.prop.name, other.prop.name)
        if result:
            return result
        if not self.prop.composite:
            return cmp(self.value, other.value)
        self_value = set(self.value)
        other_value = set(other.value)
        if self_value.issubset(other_value) or \
                other_value.issubset(self_value):
            return 0
        else:
            return cmp(self.value, other.value)

    def __hash__(self):
        return hash(self.prop.name)


def _is_term(prop):
    return isinstance(prop, StoredProperty) and \
            prop.permissions & env.ACCESS_WRITE
