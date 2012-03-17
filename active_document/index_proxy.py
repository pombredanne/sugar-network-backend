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

from active_document import util, index_queue, env
from active_document.storage import Storage
from active_document.index import IndexReader, IndexWriter, Total
from active_document.metadata import StoredProperty


_logger = logging.getLogger('ad.index_proxy')


class IndexProxy(IndexReader):

    def __init__(self, metadata):
        IndexReader.__init__(self, metadata)
        self._commit_seqno = 0
        self._cache_seqno = 1
        self._term_props = {}
        self._pages = {}

        for prop in metadata.values():
            if isinstance(prop, StoredProperty) and \
                    prop.permissions & env.ACCESS_WRITE:
                self._term_props[prop.name] = prop

    def commit(self):
        index_queue.commit_and_wait(self.metadata.name)

    def close(self):
        pass

    def get_cached(self, guid):
        self._drop_pages()

        result = {}
        for page in self._sorted_pages:
            cached = page.get(guid)
            if cached is not None:
                result.update(cached.properties)

        return result

    def store(self, guid, properties, new, pre_cb=None, post_cb=None):
        if properties and new is not None:
            if new:
                orig = None
            else:
                orig = self.get_cached(guid)
                try:
                    record = Storage(self.metadata).get(guid)
                    for prop in self._term_props.values():
                        if prop.name not in orig:
                            orig[prop.name] = record.get(prop.name)
                except env.NotFound:
                    pass
            # Needs to be called before `index_queue.put()`
            # to give it a chance to read original properties from the storage
            page = self._pages.get(self._cache_seqno)
            if page is None:
                page = self._pages[self._cache_seqno] = \
                        _CachedPage(self._term_props)
            page.update(guid, properties, orig)

        self._put(IndexWriter.store, guid, properties, new, pre_cb, post_cb)

    def delete(self, guid, post_cb=None):
        self._put(IndexWriter.delete, guid, post_cb)

    def find(self, query):
        self._reopen()

        if query.no_cache:
            pages = []
        else:
            pages = self._sorted_pages

        def next_page_find(query):
            if pages:
                return pages.pop().find(query, next_page_find)
            elif self._db is None:
                return [], Total(0)
            else:
                return IndexReader.find(self, query)

        return next_page_find(query)

    @property
    def _sorted_pages(self):
        return [self._pages[i] for i in sorted(self._pages.keys())]

    def _reopen(self):
        db_path = self.metadata.path('index')

        if self._db is None:
            if not exists(db_path):
                return
        else:
            seqno = index_queue.commit_seqno(self.metadata.name)
            if seqno == self._commit_seqno:
                return

        try:
            if self._db is None:
                self._db = xapian.Database(db_path)
                _logger.debug('Opened "%s" RO index', self.metadata.name)
            else:
                self._db.reopen()
                _logger.debug('Re-opened "%s" RO index', self.metadata.name)
        except Exception:
            util.exception(_logger,
                    'Cannot open "%s" RO index', self.metadata.name)
            self._db = None
            return

        self._drop_pages()

    def _drop_pages(self):
        self._commit_seqno = index_queue.commit_seqno(self.metadata.name)
        for seqno in self._pages.keys():
            if seqno <= self._commit_seqno:
                del self._pages[seqno]

    def _put(self, op, *args):
        _logger.debug('Push %r(%r) to "%s"\'s queue',
                op, args, self.metadata.name)

        new_cache_seqno = index_queue.put(self.metadata.name, op, *args)

        if new_cache_seqno != self._cache_seqno:
            self._cache_seqno = new_cache_seqno
            self._pages[new_cache_seqno] = _CachedPage(self._term_props)


class _CachedPage(dict):

    def __init__(self, term_props):
        self._term_props = term_props

    def update(self, guid, props, orig):
        existing = self.get(guid)
        if existing is None:
            self[guid] = _CachedDocument(self._term_props, guid, props, orig)
        else:
            existing.update(props)

    def find(self, query, direct_find):
        if 'guid' in query.request:
            documents, total = direct_find(query)
            cache = self.get(query.request['guid'])
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

        if not self:
            return direct_find(query)

        adds, deletes, updates = self._patch_find(query.request)
        if not adds and not deletes and not updates:
            return direct_find(query)

        orig_limit = query.limit
        query.limit += len(deletes)
        documents, total = direct_find(query)
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
            prop = self._term_props.get(prop_name)
            if prop is None:
                continue
            try:
                value = prop.convert(value)
            except ValueError, error:
                _logger.debug('Wrong request property value %r for "%s" ' \
                        'property, thus the whole request is empty: %s',
                        value, prop_name, error)
                return None, None, None
            terms.add(_TermValue(prop, value))

        for cache in self.values():
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


class _CachedDocument(object):

    def __init__(self, term_props, guid, properties, orig):
        self.guid = guid
        self.properties = properties.copy()
        self.new = orig is None
        self.terms = set()
        self.orig_terms = set()
        self._term_props = term_props

        for prop in term_props.values():
            if orig is not None:
                self.orig_terms.add(_TermValue(prop, orig.get(prop.name)))

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
        for prop in self._term_props.values():
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
