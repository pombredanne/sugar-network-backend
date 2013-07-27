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
import shutil
import logging
from os.path import exists, join

from sugar_network import toolkit
from sugar_network.toolkit.router import ACL
from sugar_network.db.storage import Storage
from sugar_network.db.metadata import BlobProperty, Metadata, GUID_PREFIX
from sugar_network.db.metadata import IndexedProperty, StoredProperty
from sugar_network.toolkit import http, exception, enforce


MAX_LIMIT = 2147483648

# To invalidate existed index on stcuture changes
_LAYOUT_VERSION = 4

_logger = logging.getLogger('db.directory')


class Directory(object):

    def __init__(self, root, document_class, index_class,
            broadcast=None, seqno=None):
        """
        :param index_class:
            what class to use to access to indexes, for regular casses
            (using `Master` and `Node`, it will be all time ProxyIndex to
            keep writer in separate process).

        """
        if not exists(root):
            os.makedirs(root)

        if document_class.metadata is None:
            # Metadata cannot be recreated
            document_class.metadata = Metadata(document_class)
            document_class.metadata['guid'] = IndexedProperty('guid',
                    slot=0, prefix=GUID_PREFIX, acl=ACL.CREATE | ACL.READ)
        self.metadata = document_class.metadata

        self.document_class = document_class
        self.broadcast = broadcast or (lambda event: None)
        self._index_class = index_class
        self._root = root
        self._seqno = _SessionSeqno() if seqno is None else seqno
        self._storage = None
        self._index = None

        self._open()

    @property
    def mtime(self):
        return self._index.mtime

    @mtime.setter
    def mtime(self, value):
        self._index.mtime = value
        self.broadcast({'event': 'populate', 'mtime': value})

    def wipe(self):
        self.close()
        _logger.debug('Wipe %r directory', self.metadata.name)
        shutil.rmtree(self._root, ignore_errors=True)
        self._open()

    def close(self):
        """Flush index write pending queue and close the index."""
        if self._index is None:
            return
        self._index.close()
        self._storage = None
        self._index = None

    def commit(self):
        """Flush pending chnages to disk."""
        self._index.commit()

    def create(self, props, event=None):
        """Create new document.

        If `guid` property is not specified, it will be auto set.

        :param props:
            new document properties
        :returns:
            GUID of newly created document

        """
        guid = props.get('guid')
        if not guid:
            guid = props['guid'] = toolkit.uuid()
        _logger.debug('Create %s[%s]: %r', self.metadata.name, guid, props)
        post_event = {'event': 'create', 'guid': guid}
        if event:
            post_event.update(event)
        self._index.store(guid, props, self._pre_store, self._post_store,
                post_event)
        return guid

    def update(self, guid, props, event=None):
        """Update properties for an existing document.

        :param guid:
            document GUID to store
        :param kwargs:
            properties to store, not necessary all document's properties

        """
        _logger.debug('Update %s[%s]: %r', self.metadata.name, guid, props)
        post_event = {'event': 'update', 'guid': guid}
        if event:
            post_event.update(event)
        self._index.store(guid, props, self._pre_store, self._post_store,
                post_event)

    def delete(self, guid):
        """Delete document.

        :param guid:
            document GUID to delete

        """
        _logger.debug('Delete %s[%s]', self.metadata.name, guid)
        event = {'event': 'delete', 'guid': guid}
        self._index.delete(guid, self._post_delete, event)

    def exists(self, guid):
        return self._storage.get(guid).consistent

    def get(self, guid):
        cached_props = self._index.get_cached(guid)
        record = self._storage.get(guid)
        enforce(cached_props or record.exists, http.NotFound,
                'Document %r does not exist in %r',
                guid, self.metadata.name)
        return self.document_class(guid, record, cached_props)

    def find(self, *args, **kwargs):
        """Search documents.

        The result will be an array of dictionaries with found documents'
        properties.

        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size;
            the `--find-limit` will be used by default
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param reply:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            property name to sort resulting list; might be prefixed with ``+``
            (or without any prefixes) for ascending order, and ``-`` for
            descending order
        :param group_by:
            property name to group resulting list by; no groupping by default
        :param kwargs:
            a dictionary with property values to restrict the search
        :returns:
            a tuple of (`documents`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        # XXX Hardcode SN layers switch; Remove in 0.9
        if kwargs.get('layer') in ('peruvian-pilot', ['peruvian-pilot']):
            kwargs['layer'] = 'pilot'

        mset = self._index.find(_Query(*args, **kwargs))

        def iterate():
            for hit in mset:
                guid = hit.document.get_value(0)
                record = self._storage.get(guid)
                yield self.document_class(guid, record)

        return iterate(), mset.get_matches_estimated()

    def populate(self):
        """Populate the index.

        This function needs be called right after `init()` to pickup possible
        pending changes made during the previous session when index was not
        propertly closed.

        :returns:
            function is a generator that will be iterated after picking up
            every object to let the caller execute urgent tasks

        """
        found = False
        migrate = (self.mtime == 0)

        for guid in self._storage.walk(self.mtime):
            if not found:
                _logger.info('Start populating %r index', self.metadata.name)
                found = True

            if migrate:
                self._storage.migrate(guid)

            record = self._storage.get(guid)
            try:
                props = {}
                for name, prop in self.metadata.items():
                    if not isinstance(prop, StoredProperty):
                        continue
                    meta = record.get(name)
                    if meta is not None:
                        props[name] = meta['value']
                self._index.store(guid, props)
                yield
            except Exception:
                exception('Cannot populate %r in %r, invalidate it',
                        guid, self.metadata.name)
                record.invalidate()

        if found:
            self._index.checkpoint()
            self._save_layout()
            self.commit()
            self.broadcast({'event': 'populate', 'mtime': self.mtime})

    def diff(self, seq, exclude_seq=None, **params):
        if exclude_seq is None:
            exclude_seq = []
        if 'group_by' in params:
            # Pickup only most recent change
            params['order_by'] = '-seqno'
        else:
            params['order_by'] = 'seqno'
        # TODO On big requests, xapian can raise an exception on edits
        params['limit'] = MAX_LIMIT
        params['no_cache'] = True

        for start, end in seq:
            query = 'seqno:%s..' % start
            if end:
                query += str(end)
            documents, __ = self.find(query=query, **params)

            for doc in documents:

                def patch():
                    for name, prop in self.metadata.items():
                        if name == 'seqno' or prop.acl & ACL.CALC:
                            continue
                        meta = doc.meta(name)
                        if meta is None:
                            continue
                        seqno = meta.get('seqno')
                        if seqno not in seq or seqno in exclude_seq:
                            continue
                        if isinstance(prop, BlobProperty):
                            del meta['seqno']
                        else:
                            meta = {'mtime': meta['mtime'],
                                    'value': meta.get('value'),
                                    }
                        yield name, meta, seqno

                yield doc.guid, patch()

    def merge(self, guid, diff, shift_seqno=True, **kwargs):
        """Apply changes for documents."""
        record = self._storage.get(guid)
        seqno = None
        merged = False

        for prop, meta in diff.items():
            orig_meta = record.get(prop)
            if orig_meta is not None and orig_meta['mtime'] >= meta['mtime']:
                continue
            if shift_seqno:
                if not seqno:
                    seqno = self._seqno.next()
                meta['seqno'] = seqno
            else:
                meta['seqno'] = (orig_meta or {}).get('seqno') or 0
            meta.update(kwargs)
            record.set(prop, **meta)

            merged = True

        if merged and record.consistent:
            props = {}
            if seqno:
                props['seqno'] = seqno
            # No need in after-merge event, further commit event
            # is enough to avoid events flow on nodes synchronization
            self._index.store(guid, props, self._pre_store, self._post_store)

        return seqno, merged

    def _open(self):
        if not exists(self._root):
            os.makedirs(self._root)
        index_path = join(self._root, 'index')
        if self._is_layout_stale():
            if exists(index_path):
                _logger.warning('%r layout is stale, remove index',
                        self.metadata.name)
                shutil.rmtree(index_path, ignore_errors=True)
            self._save_layout()
        self._storage = Storage(self._root, self.metadata)
        self._index = self._index_class(index_path, self.metadata,
                self._post_commit)
        _logger.debug('Initiated %r document', self.document_class)

    def _pre_store(self, guid, changes, event=None):
        seqno = changes.get('seqno')
        if event is not None and not seqno:
            seqno = changes['seqno'] = self._seqno.next()

        record = self._storage.get(guid)
        existed = record.exists

        for name, prop in self.metadata.items():
            value = changes.get(name)
            if isinstance(prop, BlobProperty):
                if value is not None:
                    record.set(name, seqno=seqno, **value)
            elif isinstance(prop, StoredProperty):
                if value is None:
                    enforce(existed or prop.default is not None,
                            'Value is not specified for %r property', name)
                    meta = record.get(name)
                    if meta is not None:
                        value = meta['value']
                    changes[name] = prop.default if value is None else value
                else:
                    if prop.localized:
                        if not isinstance(value, dict):
                            value = {toolkit.default_lang(): value}
                        if existed and \
                                type(value) is dict:  # TODO To reset `value`
                            meta = record.get(name)
                            if meta is not None:
                                meta['value'].update(value)
                                value = meta['value']
                        changes[name] = value
                    record.set(name, value=value, seqno=seqno)

    def _post_store(self, guid, changes, event=None):
        if event is not None:
            self.broadcast(event)

    def _post_delete(self, guid, event):
        self._storage.delete(guid)
        self.broadcast(event)

    def _post_commit(self):
        self._seqno.commit()
        self.broadcast({'event': 'commit', 'mtime': self.mtime})

    def _save_layout(self):
        path = join(self._root, 'layout')
        with toolkit.new_file(path) as f:
            f.write(str(_LAYOUT_VERSION))

    def _is_layout_stale(self):
        path = join(self._root, 'layout')
        if not exists(path):
            return True
        with file(path) as f:
            version = f.read()
        return not version.isdigit() or int(version) != _LAYOUT_VERSION


class _SessionSeqno(object):

    def __init__(self):
        self._value = 0

    @property
    def value(self):
        return self._value

    def next(self):
        self._value += 1
        return self._value

    def commit(self):
        pass


class _Query(object):

    def __init__(self, offset=None, limit=None, query='', reply=None,
            order_by=None, no_cache=False, group_by=None, **kwargs):
        self.query = query
        self.no_cache = no_cache
        self.group_by = group_by

        if offset is None:
            offset = 0
        self.offset = offset

        self.limit = limit or 16

        if reply is None:
            reply = ['guid']
        self.reply = reply

        if order_by is None:
            order_by = 'ctime'
        self.order_by = order_by

        self.request = kwargs

    def __repr__(self):
        return 'offset=%s limit=%s request=%r query=%r order_by=%s ' \
               'group_by=%s' % (self.offset, self.limit, self.request,
                       self.query, self.order_by, self.group_by)
