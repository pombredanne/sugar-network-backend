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

import logging
from gettext import gettext as _

from active_document import env
from active_document.storage import Storage
from active_document.metadata import Metadata, active_property
from active_document.metadata import ActiveProperty, StoredProperty
from active_document.metadata import BrowsableProperty
from active_document.util import enforce


_DIFF_PAGE_SIZE = 256

_logger = logging.getLogger('active_document.document')


class DocumentClass(object):

    #: `Metadata` object that describes the document
    metadata = None

    _initated = False
    _storage = None
    _index = None

    @active_property(slot=1000, prefix='IC', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def ctime(self, value):
        return value

    @active_property(slot=1001, prefix='IM', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def mtime(self, value):
        return value

    @active_property(slot=1002, prefix='IS', typecast=int,
            permissions=0, default=0)
    def seqno(self, value):
        return value

    @active_property(prefix='IL', typecast=[env.LAYERS],
            permissions=env.ACCESS_READ)
    def layers(self, value):
        return value

    @active_property(prefix='IA', typecast=[],
            permissions=env.ACCESS_READ)
    def author(self, value):
        return value

    def post(self):
        """Store changed properties."""
        raise NotImplementedError()

    @classmethod
    def create(cls, properties):
        """Create new document.

        :param properties:
            new document properties
        :returns:
            created `Document` object

        """
        doc = cls(**(properties or {}))
        doc.post()
        return doc

    @classmethod
    def update(cls, guid, properties):
        """Update properties for an existing document.

        :param guid:
            document GUID to store
        :param properties:
            properties to store, not necessary all document's properties

        """
        doc = cls(guid, **(properties or {}))
        doc.post()

    @classmethod
    def delete(cls, guid, raw=False):
        """Delete document.

        :param guid:
            document GUID to delete

        """
        if raw:
            cls._index.delete(guid, lambda guid: cls._storage.delete(guid))
        else:
            self = cls(guid)
            # TODO until implementing layers support
            # pylint: disable-msg=E1101
            self.set('layers', ['deleted'], raw=True)
            self.post()

    @classmethod
    def find(cls, *args, **kwargs):
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
        # TODO until implementing layers support
        query.request['layers'] = 'public'

        for prop_name in query.reply:
            prop = cls.metadata[prop_name]
            enforce(isinstance(prop, BrowsableProperty),
                    _('Property "%s" in "%s" is not suitable ' \
                            'for find requests'),
                    prop_name, cls.metadata.name)

        documents, total = cls._index.find(query)

        def iterate():
            for guid, props in documents:
                yield cls(guid, indexed_props=props)

        return iterate(), total

    @classmethod
    def close(cls):
        """Flush index write pending queue and close the index."""
        cls._index.close()
        cls._storage = None
        cls._index = None
        cls._initated = False

    @classmethod
    def populate(cls):
        """Populate the index.

        This function needs be called right after `init()` to pickup possible
        pending changes made during the previous session when index was not
        propertly closed.

        :returns:
            function is a generator that will be iterated after picking up
            every object to let the caller execute urgent tasks

        """
        first_population = True
        for guid, props in cls._storage.walk(cls._index.mtime):
            if first_population:
                _logger.info(_('Start populating "%s" index'),
                        cls.metadata.name)
                first_population = False
            cls._index.store(guid, props, None, cls._pre_store, None)
            yield

    @classmethod
    def commit(cls):
        """Flush pending chnages to disk."""
        cls._index.commit()

    @classmethod
    def diff(cls, accept_range):
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
                documents, total = cls.find(
                        query='seqno:%s..' % start,
                        order_by='seqno', reply=['guid'],
                        limit=_DIFF_PAGE_SIZE, no_cache=True)
                if not total.value:
                    break
                seqno = None
                for i in documents:
                    start = max(start, i.get('seqno', raw=True))
                    diff, __ = cls._storage.diff(i.guid, accept_range)
                    if not diff:
                        continue
                    seqno = max(seqno, i.get('seqno', raw=True))
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

    @classmethod
    def merge(cls, guid, diff, touch=True):
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
            seqno = cls.metadata.next_seqno()
        else:
            seqno = None
        if cls._storage.merge(seqno, guid, diff):
            cls._index.store(guid, {}, None, cls._pre_store, cls._post_store)
        return seqno

    @classmethod
    def init(cls, index_class):
        """Initialize `DocumentClass` class usage.

        This method should be called before any usage of the `DocumentClass`
        (and its derivates) class. For regular cases, it will be done
        implicitly from `Master` and `Node` classes.

        :param index_class:
            what class to use to access to indexes, for regular casses
            (using `Master` and `Node`, it will be all time ProxyIndex to
            keep writer in separate process).

        """
        if cls._initated:
            return

        if cls.metadata is None:
            # Metadata cannot be recreated
            cls.metadata = Metadata(cls)
            cls.metadata['guid'] = ActiveProperty('guid',
                    permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=0,
                    prefix=env.GUID_PREFIX)
        cls.metadata.ensure_path('')

        cls._storage = Storage(cls.metadata)
        cls._index = index_class(cls.metadata)
        cls._initated = True

        _logger.debug('Initiated %r document: %r', cls, cls.metadata)

    @classmethod
    def _pre_store(cls, guid, changes, is_new):
        is_reindexing = not changes

        if is_reindexing or not is_new:
            record = cls._storage.get(guid)
            for prop_name, prop in cls.metadata.items():
                if prop_name not in changes and \
                        isinstance(prop, StoredProperty):
                    changes[prop_name] = record.get(prop_name)

        if is_new is not None:
            changes['seqno'] = cls.metadata.next_seqno()

    @classmethod
    def _post_store(cls, guid, changes, is_new):
        cls._storage.put(guid, changes)
