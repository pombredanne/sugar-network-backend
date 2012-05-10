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
import logging
from os.path import exists, join
from gettext import gettext as _

from active_document import env, util
from active_document.storage import Storage
from active_document.metadata import Metadata, BlobProperty, BrowsableProperty
from active_document.metadata import ActiveProperty, StoredProperty
from active_document.util import enforce


_DIFF_PAGE_SIZE = 256

_logger = logging.getLogger('active_document.document')


class Directory(object):

    def __init__(self, root, document_class, index_class, extra_props=None,
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
            for prop in (extra_props or []):
                document_class.metadata[prop.name] = prop
        self.metadata = document_class.metadata

        self._document_class = document_class
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
        self._document_class.on_create(props)
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
        self._document_class.on_update(props)
        self._post(guid, props, False)

    def delete(self, guid):
        """Delete document.

        :param guid:
            document GUID to delete

        """
        self._index.delete(guid, self._post_delete, {'event': 'update'})

    def exists(self, guid):
        return self._storage.exists(guid)

    def get(self, guid):
        cached_props = self._index.get_cached(guid)
        record = self._storage.get(guid)
        return self._document_class(guid, cached_props, record)

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
                yield self._document_class(guid, props, record)

        return iterate(), total

    def get_blob(self, guid, prop):
        """Read the content of document's BLOB property.

        This function works in parallel to getting non-BLOB properties values.

        :param prop:
            BLOB property name
        :returns:
            file-like object or `None`

        """
        prop = self.metadata[prop]
        enforce(isinstance(prop, BlobProperty),
                _('Property %r in %r is not a BLOB'),
                prop.name, self.metadata.name)
        document = self.get(guid)
        path = self._storage.get_blob(guid, prop.name)
        path = prop.on_get(document, path)
        if not path:
            return None
        return file(path)

    def set_blob(self, guid, prop, stream, size=None):
        """Receive BLOB property from a stream.

        This function works in parallel to setting non-BLOB properties values
        and `post()` function.

        :param prop:
            BLOB property name
        :param stream:
            stream to receive property value from
        :param size:
            read only specified number of bytes; otherwise, read until the EOF

        """
        prop = self.metadata[prop]
        enforce(isinstance(prop, BlobProperty),
                _('Property %r in %r is not a BLOB'),
                prop.name, self.metadata.name)
        seqno = self._next_seqno()
        if self._storage.set_blob(seqno, guid, prop.name, stream, size):
            self._index.store(guid, {'seqno': seqno}, None,
                    self._pre_store, self._post_store)
            event = {'event': 'update_blob',
                     'guid': guid,
                     'prop': prop.name,
                     }
            self._notify(event)

    def stat_blob(self, guid, prop):
        """Receive BLOB property information.

        :param prop:
            BLOB property name
        :returns:
            a dictionary of `size`, `sha1sum` keys

        """
        prop = self.metadata[prop]
        enforce(isinstance(prop, BlobProperty),
                _('Property %r in %r is not a BLOB'),
                prop.name, self.metadata.name)
        return self._storage.stat_blob(guid, prop.name)

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
            self._notify({'event': 'update'})

    def diff(self, accept_range):
        """Return documents' properties for specified times range.

        :param accept_range:
            sequence object with times to accept documents
        :returns:
            tuple of dictionaries for regular properties and BLOBs

        """
        result = [None, None]

        def do():
            # To make fetching docs more reliable, avoid using intermediate
            # find's offsets (documents can be changed and offset will point
            # to different document).
            if hasattr(accept_range, 'first'):
                start = accept_range.first
            else:
                start = accept_range[0]

            while True:
                documents, total = self.find(
                        query='seqno:%s..' % start,
                        order_by='seqno', reply=['guid'],
                        limit=_DIFF_PAGE_SIZE, no_cache=True)
                if not total.value:
                    break
                seqno = None
                for i in documents:
                    start = max(start, i.get('seqno'))
                    diff, __ = self._storage.diff(i.guid, accept_range)
                    if not diff:
                        continue
                    seqno = max(seqno, i.get('seqno'))
                    if result[0] is None:
                        result[0] = seqno
                    yield i.guid, diff
                if seqno:
                    result[1] = seqno
                start += 1

        if accept_range:
            return result, do()
        else:
            return result, []

    def merge(self, guid, diff, touch=True):
        """Apply changes for documents.

        :param guid:
            document's GUID to merge `diff` to
        :param diff:
            document changes
        :param touch:
            if `True`, touch local mtime
        :returns:
            seqno value for applied `diff`;
            `None` if `diff` was not applied

        """
        if touch:
            seqno = self._next_seqno()
        else:
            seqno = None
        if self._storage.merge(seqno, guid, diff):
            self._index.store(guid, {}, None,
                    self._pre_store, self._post_store)
        # TODO Send "populate" event
        return seqno

    def _pre_store(self, guid, changes, is_new):
        is_reindexing = not changes

        if is_reindexing or not is_new:
            record = self._storage.get(guid)
            for prop_name, prop in self.metadata.items():
                if prop_name not in changes and \
                        isinstance(prop, StoredProperty):
                    changes[prop_name] = record.get(prop_name)

        if is_new is not None:
            changes['seqno'] = self._next_seqno()

    def _post_store(self, guid, changes, is_new, event=None):
        self._storage.put(guid, changes)
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
        self._notify({'event': 'commit'})

    def _next_seqno(self):
        self._seqno += 1
        return self._seqno

    def _post(self, guid, props, new):
        if not props:
            return

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

        event = {'event': 'update',
                 'props': props,
                 }
        if not new:
            event['guid'] = guid
        self._index.store(guid, props, new,
                self._pre_store, self._post_store, event)

    def _notify(self, even):
        if self._notification_cb is not None:
            self._notification_cb(even)
