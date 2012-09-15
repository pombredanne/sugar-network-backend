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
import shutil
import logging
from os.path import exists, join

from active_document import env, http
from active_document.storage import Storage
from active_document.metadata import Metadata, BlobProperty, BrowsableProperty
from active_document.metadata import ActiveProperty, StoredProperty
from active_toolkit import util, enforce


# To invalidate existed index on stcuture changes
_LAYOUT_VERSION = 1

_logger = logging.getLogger('active_document.document')


class Directory(object):

    def __init__(self, root, document_class, index_class,
            notification_cb=None, seqno=None):
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
            document_class.metadata['guid'] = ActiveProperty('guid',
                    permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=0,
                    prefix=env.GUID_PREFIX)
        self.metadata = document_class.metadata

        self.document_class = document_class
        self._root = root
        self._notification_cb = notification_cb
        self._seqno = _SessionSeqno() if seqno is None else seqno

        index_path = join(root, 'index')
        if self._is_layout_stale():
            if exists(index_path):
                _logger.warning('%r layout is stale, remove index',
                        self.metadata.name)
            shutil.rmtree(index_path, ignore_errors=True)
            self._save_layout()

        self._storage = Storage(root, self.metadata)
        self._index = index_class(index_path, self.metadata, self._post_commit)

        _logger.debug('Initiated %r document', document_class)

    def close(self):
        """Flush index write pending queue and close the index."""
        self._index.close()
        self._storage = None
        self._index = None

    def commit(self):
        """Flush pending chnages to disk."""
        self._index.commit()

    def create(self, props=None, **kwargs):
        """Create new document.

        If `guid` property is not specified, it will be auto set.

        :param kwargs:
            new document properties
        :returns:
            GUID of newly created document

        """
        if props is None:
            props = kwargs

        if 'guid' in props:
            guid = props['guid']
        else:
            guid = props['guid'] = env.uuid()

        for prop_name, prop in self.metadata.items():
            if isinstance(prop, StoredProperty):
                if prop_name in props:
                    continue
                enforce(prop.default is not None,
                        'Property %r should be passed for new %r document',
                        prop_name, self.metadata.name)
            if prop.default is not None:
                props[prop_name] = prop.default

        _logger.debug('Create %s[%s]: %r', self.metadata.name, guid, props)
        self._post(guid, props, True)
        return guid

    def update(self, guid, props=None, **kwargs):
        """Update properties for an existing document.

        :param guid:
            document GUID to store
        :param kwargs:
            properties to store, not necessary all document's properties

        """
        if props is None:
            props = kwargs
        if not props:
            return
        _logger.debug('Update %s[%s]: %r', self.metadata.name, guid, props)
        self._post(guid, props, False)

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
        enforce(cached_props or record.exists, env.NotFound,
                'Document %r does not exist in %r',
                guid, self.metadata.name)
        return self.document_class(guid, record, cached_props)

    def find(self, *args, **kwargs):
        """Search documents.

        The result will be an array of dictionaries with found documents'
        properties. Function accepts the same arguments as
        `active_document.Query`.

        :returns:
            a tuple of (`documents`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        query = env.Query(*args, **kwargs)

        for prop_name in query.reply:
            prop = self.metadata[prop_name]
            enforce(isinstance(prop, BrowsableProperty),
                    'Property %r in %r is not suitable for find requests',
                    prop_name, self.metadata.name)

        documents, total = self._index.find(query)

        def iterate():
            for guid, props in documents:
                record = self._storage.get(guid)
                yield self.document_class(guid, record, props)

        return iterate(), total

    def set_blob(self, guid, prop, data=None, size=None, **kwargs):
        """Receive BLOB property.

        This function works in parallel to setting non-BLOB properties values
        and `post()` function.

        :param prop:
            BLOB property name
        :param data:
            stream to read BLOB content, path to file to copy, or, web url
        :param size:
            read only specified number of bytes; otherwise, read until the EOF

        """
        prop = self.metadata[prop]
        record = self._storage.get(guid)
        seqno = self._seqno.next()

        _logger.debug('Received %r BLOB property from %s[%s]',
                prop.name, self.metadata.name, guid)
        if prop.mime_type == 'application/json' and not hasattr(data, 'read'):
            data = json.dumps(data)
        record.set_blob(prop.name, data, size, seqno=seqno,
                mime_type=prop.mime_type, **kwargs)

        if record.consistent:
            self._post(guid, {'seqno': seqno}, False)

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
        migrate = (self._index.mtime == 0)

        for guid in self._storage.walk(self._index.mtime):
            if not found:
                _logger.info('Start populating %r index', self.metadata.name)
                found = True

            if migrate:
                self._storage.migrate(guid)

            props = {}
            record = self._storage.get(guid)
            for name, prop in self.metadata.items():
                if not isinstance(prop, StoredProperty):
                    continue
                meta = record.get(name)
                if meta is not None:
                    props[name] = meta['value']
            self._index.store(guid, props, None, None, None)

            yield

        if found:
            self._save_layout()
            self.commit()
            self._notify({'event': 'populate', 'seqno': self._seqno.value})

    def diff(self, accept_range, limit, clone=False):
        """Return documents' properties for specified times range.

        :param accept_range:
            seqno sequence to accept documents
        :param limit:
            number of documents to return at once
        :param clone:
            is `diff()` call is inteded for initial clonning, e.g.,
            cloning does not assume fetching deleted objects
        :returns:
            a tuple of ((`left-seqno`, `right-seqno`), [(`guid`, `patch`)]),
            where `patch` is a resulting dictionary from `Document.diff()`
            for corresponding `guid`

        """
        if not accept_range:
            return

        # To make fetching more reliable, avoid using intermediate
        # find's offsets (documents can be changed and offset will point
        # to different document).
        if hasattr(accept_range, 'first'):
            seqno = accept_range.first
        else:
            seqno = accept_range[0]

        query = {'limit': limit,
                 'no_cache': True,
                 'reply': ['guid'],
                 'order_by': 'seqno',
                 }
        if clone:
            query['layer'] = 'public'

        while True:
            documents, total = self.find(query='seqno:%s..' % seqno, **query)
            if not total.value:
                break

            for doc in documents:
                seqno = doc.get('seqno')
                if seqno not in accept_range:
                    continue

                diff = {}
                for name in self.metadata.keys():
                    if name == 'seqno':
                        continue
                    meta = doc.meta(name, raw=True)
                    if meta is None or meta['seqno'] not in accept_range:
                        continue

                    if isinstance(self.metadata[name], BlobProperty):
                        header = {
                                'guid': doc.guid,
                                'prop': name,
                                'mime_type': meta['mime_type'],
                                'digest': meta.get('digest'),
                                'mtime': meta['mtime'],
                                }
                        if 'digest' in meta:
                            header['digest'] = meta['digest']
                        if 'path' in meta:
                            with file(meta['path'], 'rb') as f:
                                yield header, f
                        elif 'url' in meta:
                            with http.download(meta['url']) as f:
                                yield header, f
                    else:
                        diff[name] = {
                                'value': meta['value'],
                                'mtime': meta['mtime'],
                                }

                yield {'guid': doc.guid, 'seqno': seqno}, diff

            seqno += 1

    def merge(self, guid, diff, increment_seqno=True, **kwargs):
        """Apply changes for documents."""
        record = self._storage.get(guid)
        props = {}

        def merge(fun, **meta):
            orig_meta = record.get(meta['prop'])
            if orig_meta is not None and orig_meta['mtime'] >= meta['mtime']:
                return False
            if increment_seqno:
                if not props:
                    props['seqno'] = self._seqno.next()
                meta.update(props)
            else:
                meta['seqno'] = (orig_meta or {}).get('seqno') or 0
            fun(**meta)
            return True

        merged = False
        if isinstance(diff, dict):
            for prop, meta in diff.items():
                merged |= merge(record.set, prop=prop, **meta)
        else:
            merged = merge(record.set_blob, prop=kwargs['prop'], data=diff,
                    mime_type=kwargs.get('mime_type'),
                    digest=kwargs.get('digest'),
                    mtime=kwargs.get('mtime'))

        if merged and record.consistent:
            self._index.store(guid, props, False,
                    self._pre_store, self._post_store,
                    # No need in after-merge event, further commit event
                    # is enough to avoid events flow on nodes synchronization
                    None, increment_seqno)

        return props.get('seqno')

    def _pre_store(self, guid, changes, event, increment_seqno):
        seqno = changes.get('seqno')
        if increment_seqno and not seqno:
            seqno = changes['seqno'] = self._seqno.next()

        record = self._storage.get(guid)
        existed = record.exists

        for name, prop in self.metadata.items():
            if not isinstance(prop, StoredProperty):
                continue
            value = changes.get(name)
            if value is None:
                if existed:
                    meta = record.get(name)
                    if meta is not None:
                        value = meta['value']
                changes[name] = prop.default if value is None else value
            else:
                if prop.localized:
                    if not isinstance(value, dict):
                        value = {env.DEFAULT_LANG: value}
                    if existed:
                        meta = record.get(name)
                        if meta is not None:
                            meta['value'].update(value)
                            value = meta['value']
                    changes[name] = value
                record.set(name, value=value, seqno=seqno)

    def _post_store(self, guid, changes, event, increment_seqno):
        if event:
            event['seqno'] = self._seqno.value
            self._notify(event)

    def _post_delete(self, guid, event):
        self._storage.delete(guid)
        self._notify(event)

    def _post_commit(self):
        self._seqno.commit()
        self._notify({'event': 'commit', 'seqno': self._seqno.value})

    def _post(self, guid, props, new):
        for prop_name, value in props.items():
            prop = self.metadata[prop_name]
            enforce(isinstance(prop, StoredProperty),
                    'Property %r in %r cannot be set',
                    prop_name, self.metadata.name)
            try:
                props[prop_name] = prop.decode(value)
            except Exception:
                error = 'Value %r for %r property for %r is invalid' % \
                        (value, prop_name, self.metadata.name)
                util.exception(error)
                raise RuntimeError(error)

        event = {'event': 'create' if new else 'update',
                 'props': props.copy(),
                 'guid': guid,
                 }
        self._index.store(guid, props, new,
                self._pre_store, self._post_store, event, True)

    def _notify(self, even):
        if self._notification_cb is not None:
            self._notification_cb(even)

    def _save_layout(self):
        path = join(self._root, 'layout')
        with util.new_file(path) as f:
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
