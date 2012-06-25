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
import logging
from os.path import exists, join
from gettext import gettext as _

from active_document import env
from active_document.storage import Storage
from active_document.metadata import Metadata, BlobProperty, BrowsableProperty
from active_document.metadata import ActiveProperty, StoredProperty
from active_toolkit import util, enforce


_logger = logging.getLogger('active_document.document')


class Directory(object):

    def __init__(self, root, document_class, index_class,
            notification_cb=None):
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
        self._storage = Storage(root, self.metadata)
        self._index = index_class(root, self.metadata, self._post_commit)
        self._root = root
        self._seqno = 0
        self._notification_cb = notification_cb

        seqno_path = join(root, 'seqno')
        if exists(seqno_path):
            with file(seqno_path) as f:
                self._seqno = int(f.read().strip())

        _logger.debug('Initiated %r document', document_class)

    @property
    def seqno(self):
        return self._seqno

    def close(self):
        """Flush index write pending queue and close the index."""
        self._index.close()
        self._storage = None
        self._index = None

    def commit(self):
        """Flush pending chnages to disk."""
        self._index.commit()

    def create(self, props):
        """Create new document.

        :param props:
            new document properties

        """
        enforce('guid' not in props,
                _('Cannot create new document if "guid" is specified'))
        return self.create_with_guid(env.uuid(), props)

    def create_with_guid(self, guid, props):
        """Create new document for specified GUID.

        :param guid:
            GUID value to create document for
        :param props:
            new document properties

        """
        self.document_class.before_create(props)
        if 'guid' in props:
            guid = props['guid']
        else:
            props['guid'] = guid

        for prop_name, prop in self.metadata.items():
            if isinstance(prop, StoredProperty):
                if prop_name in props:
                    continue
                enforce(prop.default is not None,
                        _('Property %r should be passed for ' \
                                'new %r document'),
                        prop_name, self.metadata.name)
            if prop.default is not None:
                props[prop_name] = prop.default

        _logger.debug('Create %s[%s]: %r', self.metadata.name, guid, props)
        self.document_class.before_post(props)
        self._post(guid, props, True)
        return guid

    def update(self, guid, props):
        """Update properties for an existing document.

        :param guid:
            document GUID to store
        :param props:
            properties to store, not necessary all document's properties

        """
        if not props:
            return
        _logger.debug('Update %s[%s]: %r', self.metadata.name, guid, props)
        self.document_class.before_update(props)
        self.document_class.before_post(props)
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
                _('Document %r does not exist in %r'),
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
                    _('Property %r in %r is not suitable for find requests'),
                    prop_name, self.metadata.name)

        documents, total = self._index.find(query)

        def iterate():
            for guid, props in documents:
                record = self._storage.get(guid)
                yield self.document_class(guid, record, props)

        return iterate(), total

    def set_blob(self, guid, prop, data, size=None):
        """Receive BLOB property.

        This function works in parallel to setting non-BLOB properties values
        and `post()` function.

        :param prop:
            BLOB property name
        :param data:
            stream to read BLOB content or path to file to copy
        :param size:
            read only specified number of bytes; otherwise, read until the EOF

        """
        prop = self.metadata[prop]
        enforce(isinstance(prop, BlobProperty),
                _('Property %r in %r is not a BLOB'),
                prop.name, self.metadata.name)
        record = self._storage.get(guid)
        seqno = self._next_seqno()

        _logger.debug('Received %r BLOB property from %s[%s]',
                prop.name, self.metadata.name, guid)
        record.set_blob(prop.name, data, size, seqno=seqno)

        if record.consistent:
            self._post(guid, {'seqno': seqno}, False)
            event = {'event': 'update_blob',
                     'guid': guid,
                     'prop': prop.name,
                     'seqno': seqno,
                     }
            self._notify(event)

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

        for guid, props in self._storage.walk(self._index.mtime):
            if not found:
                _logger.info(_('Start populating %r index'),
                        self.metadata.name)
                found = True
            self._index.store(guid, props, None, self._pre_store, None)
            yield

        if found:
            self.commit()
            self._notify({'event': 'sync', 'seqno': self._seqno})

    def diff(self, accept_range, limit):
        """Return documents' properties for specified times range.

        :param accept_range:
            seqno sequence to accept documents
        :param limit:
            number of documents to return at once
        :returns:
            a tuple of ((`left-seqno`, `right-seqno`), [(`guid`, `patch`)]),
            where `patch` is a resulting dictionary from `Document.diff()`
            for corresponding `guid`

        """
        ranges = []
        if not accept_range:
            return ranges, []
        return ranges, self._diff(ranges, accept_range, limit)

    def merge(self, diff, touch=True):
        """Apply changes for documents.

        :param diff:
            document changes
        :param touch:
            if `True`, touch local mtime

        """
        common_props = {}

        def merge(record, fun, **meta):
            orig_meta = record.get(meta['prop'])
            if orig_meta is not None and orig_meta['mtime'] >= meta['mtime']:
                return False
            if touch and not common_props:
                common_props['seqno'] = self._next_seqno()
            meta.update(common_props)
            fun(**meta)
            return True

        for header, data in diff:
            guid = header.pop('guid')
            record = self._storage.get(guid)
            merged = False
            if isinstance(data, dict):
                for prop, meta in data.items():
                    merged |= merge(record, record.set, prop=prop, **meta)
            else:
                merged |= merge(record, record.set_blob, data=data, **header)
            if merged and record.consistent:
                self._post(guid, common_props.copy(), False)

    def _diff(self, ranges, accept_range, limit):
        # To make fetching more reliable, avoid using intermediate
        # find's offsets (documents can be changed and offset will point
        # to different document).
        if hasattr(accept_range, 'first'):
            seqno = accept_range.first
        else:
            seqno = accept_range[0]
        start = seqno

        while True:
            documents, total = self.find(
                    query='seqno:%s..' % seqno,
                    order_by='seqno', reply=['guid'],
                    limit=limit, no_cache=True)
            if not total.value:
                break

            for doc in documents:
                seqno = doc.get('seqno')
                if seqno not in accept_range:
                    continue

                if not ranges:
                    ranges[:] = [start, None]
                ranges[1] = seqno

                diff = {}
                for name in self.metadata.keys():
                    if name == 'seqno':
                        continue
                    meta = doc.meta(name)
                    if meta is None:
                        continue
                    if 'path' in meta:
                        with file(meta['path'], 'rb') as f:
                            item = {'guid': doc.guid,
                                    'prop': name,
                                    'mtime': meta['mtime'],
                                    'digest': meta['digest'],
                                    }
                            yield item, f
                    else:
                        diff[name] = {
                                'value': meta['value'],
                                'mtime': meta['mtime'],
                                }
                yield {'guid': doc.guid}, diff

            seqno += 1

    def _pre_store(self, guid, changes):
        seqno = changes.get('seqno')
        if not seqno:
            seqno = changes['seqno'] = self._next_seqno()

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

    def _post_store(self, guid, changes, event=None):
        if event:
            self._notify(event)

    def _post_delete(self, guid, event):
        self._storage.delete(guid)
        self._notify(event)

    def _post_commit(self):
        with util.new_file(join(self._root, 'seqno')) as f:
            f.write(str(self._seqno))
            f.flush()
            os.fsync(f.fileno())
        self._notify({'event': 'commit', 'seqno': self._seqno})

    def _next_seqno(self):
        self._seqno += 1
        return self._seqno

    def _post(self, guid, props, new):
        for prop_name, value in props.items():
            prop = self.metadata[prop_name]
            enforce(isinstance(prop, StoredProperty),
                    _('Property %r in %r cannot be set'),
                    prop_name, self.metadata.name)
            try:
                props[prop_name] = prop.decode(value)
            except Exception:
                error = _('Value %r for %r property for %r is invalid') % \
                        (value, prop_name, self.metadata.name)
                util.exception(error)
                raise RuntimeError(error)

        event = {'event': 'create' if new else 'update',
                 'props': props.copy(),
                 'guid': guid,
                 }
        self._index.store(guid, props, new,
                self._pre_store, self._post_store, event)

    def _notify(self, even):
        if self._notification_cb is not None:
            self._notification_cb(even)
