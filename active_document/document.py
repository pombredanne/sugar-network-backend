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

import time
import uuid
import logging
from datetime import datetime
from gettext import gettext as _

from active_document import env, util
from active_document.storage import Storage
from active_document.metadata import Metadata
from active_document.metadata import ActiveProperty, StoredProperty
from active_document.metadata import GuidProperty, BlobProperty, SeqnoProperty
from active_document.metadata import AggregatorProperty, IndexedProperty
from active_document.index import IndexWriter
from active_document.index_proxy import IndexProxy
from active_document.seqno import Seqno
from active_document.util import enforce


_logger = logging.getLogger('ad.document')


def active_property(property_class=ActiveProperty, *args, **kwargs):

    def getter(func, self):
        value = self[func.__name__]
        return func(self, value)

    def setter(func, self, value):
        value = func(self, value)
        self[func.__name__] = value

    def decorate_setter(func, attr):
        attr.prop.writable = True
        attr.writer = lambda self, value: setter(func, self, value)
        return attr

    def decorate_getter(func):
        enforce(func.__name__ != 'guid',
                _('Active property should not have "guid" name'))
        attr = lambda self, * args: getter(func, self)
        attr.setter = lambda func: decorate_setter(func, attr)
        attr._is_active_property = True
        attr.name = func.__name__
        attr.prop = property_class(attr.name, *args, **kwargs)
        return attr

    return decorate_getter


class Document(object):

    #: `Metadata` object that describes the document
    metadata = None

    _initated = False
    _storage = None
    _index = None
    _seqno = None

    def __init__(self, guid=None, indexed_props=None, raw=None, **kwargs):
        """
        :param guid:
            GUID of existing document; if omitted, newly created object
            will be associated with new document; new document will be saved
            only after calling `post`
        :param indexed_props:
            property values got from index to populate the cache
        :param raw:
            list of property names to avoid any checks for
            users' visible properties; only for server local use
        :param kwargs:
            optional key arguments with new property values; specifing these
            arguments will mean the same as setting properties after `Document`
            object creation

        """
        self.init()
        self._raw = raw or []
        self._is_new = False
        self._cache = {}
        self._record = None

        if guid:
            self._guid = guid
            if not indexed_props:
                indexed_props = self._index.get_cache(guid)
            for prop_name, value in (indexed_props or {}).items():
                self._cache[prop_name] = (value, None)
            self.authorize_document(env.ACCESS_READ, self)
        else:
            self._is_new = True

            cache = {}
            self.on_create(kwargs, cache)
            for name, value in cache.items():
                self._cache[name] = (None, value)
            self._guid = cache['guid']

            for name, prop in self.metadata.items():
                if isinstance(prop, StoredProperty):
                    if name in kwargs or name in self._cache:
                        continue
                    enforce(prop.default is not None,
                            _('Property "%s" should be passed for ' \
                                    'new "%s" document'),
                            name, self.metadata.name)
                if prop.default is not None:
                    self._cache[name] = (None, prop.default)

        for prop_name, value in kwargs.items():
            self[prop_name] = value

    @property
    def guid(self):
        """Document GUID."""
        return self._guid

    @active_property(slot=1000, prefix='IC', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def ctime(self, value):
        return value

    @active_property(slot=1001, prefix='IM', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def mtime(self, value):
        return value

    @active_property(SeqnoProperty, slot=1002, prefix='IS')
    def seqno(self, value):
        return value

    def __getitem__(self, prop_name):
        """Get document's property value.

        :param prop_name:
            property name to get value
        :returns:
            `prop_name` value

        """
        prop = self.metadata[prop_name]
        self.authorize_property(env.ACCESS_READ, prop)

        orig, new = self._cache.get(prop_name, (None, None))
        if new is not None:
            return new
        if orig is not None:
            return orig

        if isinstance(prop, StoredProperty):
            if self._record is None:
                self._record = self._storage.get(self.guid)
            orig = self._record.get(prop_name)
        else:
            if isinstance(prop, IndexedProperty):
                self._get_not_storable_but_indexd_props()
                orig, __ = self._cache.get(prop_name, (None, None))
            if orig is None and isinstance(prop, AggregatorProperty):
                value = self._storage.is_aggregated(
                        self.guid, prop_name, prop.value)
                orig = env.value(value)
            enforce(orig is not None, _('Property "%s" in "%s" cannot be get'),
                    prop_name, self.metadata.name)

        self._cache[prop_name] = (orig, new)
        return orig

    def get_list(self, prop_name):
        """If property value contains several values, list them all.

        :param prop_name:
            property name to return value for
        :returns:
            list of value's portions; for not multiple properties,
            return singular value as the only part of the list

        """
        value = self[prop_name]
        prop = self.metadata[prop_name]
        if isinstance(prop, IndexedProperty):
            return prop.list_value(value)
        else:
            return [value]

    def __setitem__(self, prop_name, value):
        """set document's property value.

        :param prop_name:
            property name to set
        :param value:
            property value to set

        """
        if prop_name == 'guid':
            enforce(self._is_new, _('GUID can be set only for new documents'))

        prop = self.metadata[prop_name]
        if self._is_new:
            self.authorize_property(env.ACCESS_CREATE, prop)
        else:
            self.authorize_property(env.ACCESS_WRITE, prop)

        if isinstance(prop, StoredProperty):
            pass
        elif isinstance(prop, AggregatorProperty):
            enforce(value.isdigit(),
                    _('Property "%s" in "%s" should be either "0" or "1"'),
                    prop_name, self.metadata.name)
            value = env.value(bool(int(value)))
        else:
            raise RuntimeError(_('Property "%s" in "%s" cannot be set') % \
                    (prop_name, self.metadata.name))

        orig, __ = self._cache.get(prop_name, (None, None))
        self._cache[prop_name] = (orig, value)

        if prop_name == 'guid':
            self._guid = value

    def post(self):
        """Store changed properties."""
        changes = {}
        for prop_name, (__, new) in self._cache.items():
            if new is not None:
                changes[prop_name] = new
        if not changes:
            return

        if self._is_new:
            self.authorize_document(env.ACCESS_CREATE, self)
        else:
            self.authorize_document(env.ACCESS_WRITE, self)
            self.on_modify(changes)
        self.on_post(changes)

        for prop_name, value in changes.items():
            prop = self.metadata[prop_name]
            if not isinstance(prop, IndexedProperty) or \
                    prop.typecast is None:
                continue
            try:
                value_parts = []
                for part in prop.list_value(value):
                    if prop.typecast is int:
                        part = str(int(part))
                    elif prop.typecast is bool:
                        part = str(int(bool(int(part))))
                    else:
                        enforce(part in prop.typecast,
                                _('Value "%s" is not from "%s" list'),
                                part, ', '.join(prop.typecast))
                    value_parts.append(part)
                changes[prop_name] = (prop.separator or ' ').join(value_parts)
            except Exception:
                error = _('Value for "%s" property for "%s" is invalid') % \
                        (prop_name, self.metadata.name)
                util.exception(error)
                raise RuntimeError(error)

        if self._is_new:
            _logger.debug('Create new document "%s"', self.guid)

        self._index.store(self.guid, changes, self._is_new,
                self._pre_store, self._post_store)
        self._is_new = False

    def get_blob(self, prop_name):
        """Read BLOB property content.

        This function works in parallel to getting non-BLOB properties values.

        :param prop_name:
            property name
        :returns:
            generator that returns data by portions

        """
        prop = self.metadata[prop_name]
        self.authorize_property(env.ACCESS_READ, prop)
        enforce(isinstance(prop, BlobProperty),
                _('Property "%s" in "%s" is not a BLOB'),
                prop_name, self.metadata.name)
        return self._storage.get_blob(self.guid, prop_name)

    def set_blob(self, prop_name, stream, size=None):
        """Receive BLOB property from a stream.

        This function works in parallel to setting non-BLOB properties values
        and `post()` function.

        :param prop_name:
            property name
        :param stream:
            stream to receive property value from
        :param size:
            read only specified number of bytes; otherwise, read until the EOF

        """
        prop = self.metadata[prop_name]
        self.authorize_property(env.ACCESS_WRITE, prop)
        enforce(isinstance(prop, BlobProperty),
                _('Property "%s" in "%s" is not a BLOB'),
                prop_name, self.metadata.name)
        self._storage.set_blob(self.guid, prop_name, stream, size)

    def on_create(self, properties, cache):
        """Call back to call on document creation.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with new document properties values
        :param cache:
            properties to use as predefined values

        """
        cache['guid'] = str(uuid.uuid1())

        ts = str(int(time.mktime(datetime.utcnow().timetuple())))
        cache['ctime'] = ts
        cache['mtime'] = ts

        self._set_seqno(cache)

    def on_modify(self, properties):
        """Call back to call on existing document modification.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        ts = str(int(time.mktime(datetime.utcnow().timetuple())))
        properties['mtime'] = ts

        self._set_seqno(properties)

    def on_post(self, properties):
        """Call back to call on exery `post()` call.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        pass

    def authorize_property(self, mode, prop):
        """Does caller have permissions to access to the specified property.

        If caller does not have permissions, function should raise
        `active_document.Forbidden` exception.

        :param mode:
            one of `active_document.ACCESS_*` constants
            to specify the access mode
        :param prop:
            property to check access for

        """
        enforce(prop.name in self._raw or \
                mode & prop.permissions, env.Forbidden,
                _('%s access is disabled for "%s" property in "%s"'),
                env.ACCESS_NAMES[mode], prop.name, self.metadata.name)

    @classmethod
    def authorize_document(cls, mode, document=None):
        """Does caller have permissions to access to the document.

        If caller does not have permissions, function should raise
        `active_document.Forbidden` exception.

        :param mode:
            one of `active_document.ACCESS_*` constants
            to specify the access mode
        :param document:
            option document if `mode` needs it;
            might be `Document` object or GUID value

        """
        pass

    @classmethod
    def create(cls, properties, raw=None):
        """Create new document.

        :param properties:
            new document properties
        :param raw:
            list of property names to avoid any checks for
            users' visible properties; only for server local use
        :returns:
            created `Document` object

        """
        doc = cls(raw=raw, **(properties or {}))
        doc.post()
        return doc

    @classmethod
    def update(cls, guid, properties, raw=None):
        """Update properties for an existing document.

        :param guid:
            document GUID to store
        :param properties:
            properties to store, not necessary all document's properties
        :param raw:
            list of property names to avoid any checks for
            users' visible properties; only for server local use

        """
        doc = cls(guid, raw=raw, **(properties or {}))
        doc.post()

    @classmethod
    def delete(cls, guid):
        """Delete document.

        :param guid:
            document GUID to delete

        """
        cls.authorize_document(env.ACCESS_DELETE, guid)
        cls._index.delete(guid, lambda guid: cls._storage.delete(guid))

    @classmethod
    def find(cls, offset=None, limit=None, request=None, query='',
            reply=None, order_by=None):
        """Search documents.

        The result will be an array of dictionaries with found documents'
        properties.

        :param offset:
            the resulting list should start with this offset;
            0 by default
        :param limit:
            the resulting list will be at least `limit` size;
            the `--find-limit` will be used by default
        :param request:
            a dictionary with property values to restrict the search
        :param query:
            a string in Xapian serach format, empty to avoid text search
        :param reply:
            an array of property names to use only in the resulting list;
            only GUID property will be used by default
        :param order_by:
            property name to sort resulting list; might be prefixed with ``+``
            (or without any prefixes) for ascending order, and ``-`` for
            descending order
        :returns:
            a tuple of (`documents`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        cls.authorize_document(env.ACCESS_READ)

        if offset is None:
            offset = 0
        if limit is None:
            limit = env.find_limit.value
        elif limit > env.find_limit.value:
            _logger.warning(_('The find limit for "%s" is restricted to "%s"'),
                    cls.metadata.name, env.find_limit.value)
            limit = env.find_limit.value
        if request is None:
            request = {}
        if not reply:
            reply = ['guid']
        if order_by is None:
            order_by = 'ctime'

        for prop_name in reply:
            enforce(cls.metadata[prop_name].is_trait,
                    _('Property "%s" in "%s" is not suitable ' \
                            'for find requests'),
                    prop_name, cls.metadata.name)

        documents, total = cls._index.find(offset, limit, request, query,
                reply, order_by)

        def iterate():
            for guid, props in documents:
                yield cls(guid, indexed_props=props)

        return iterate(), total

    @classmethod
    def close(cls):
        """Flush index write pending queue and close the index."""
        cls._index.close()

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
        aggregated_props = []
        for prop in cls.metadata.values():
            if isinstance(prop, AggregatorProperty) and prop.counter:
                aggregated_props.append(prop)

        for guid, props in cls._storage.walk(cls._index.mtime):
            for prop in aggregated_props:
                props[prop.counter] = env.value(
                        cls._storage.count_aggregated(guid, prop.name))
            cls._index.store(guid, props, True)
            yield

    @classmethod
    def init(cls, final_cls=None):
        if final_cls is not None:
            cls = final_cls

        if cls._initated:
            return

        cls.metadata = Metadata()
        cls.metadata.name = cls.__name__.lower()
        cls.metadata['guid'] = GuidProperty()

        cls._storage = Storage(cls.metadata)
        cls._seqno = Seqno(cls.metadata)

        slots = {}
        prefixes = {}
        for attr in [getattr(cls, i) for i in dir(cls)]:
            if not hasattr(attr, '_is_active_property'):
                continue
            if hasattr(attr.prop, 'slot'):
                enforce(attr.prop.slot is None or \
                        attr.prop.slot not in slots,
                        _('Property "%s" has a slot already defined ' \
                                'for "%s"'),
                        attr.prop.name, slots.get(attr.prop.slot))
                slots[attr.prop.slot] = attr.prop.name
            if hasattr(attr.prop, 'prefix'):
                enforce(not attr.prop.prefix or \
                        attr.prop.prefix not in prefixes,
                        _('Property "%s" has a prefix already defined ' \
                                'for "%s"'),
                        attr.prop.name, prefixes.get(attr.prop.prefix))
                prefixes[attr.prop.prefix] = attr.prop.name
            if attr.prop.writable:
                setattr(cls, attr.name, property(attr, attr.writer))
            else:
                setattr(cls, attr.name, property(attr))
            cls.metadata[attr.prop.name] = attr.prop

        if env.index_write_queue.value > 0:
            cls._index = IndexProxy(cls.metadata)
        else:
            cls._index = IndexWriter(cls.metadata)

        cls._initated = True

    @classmethod
    def _pre_store(cls, guid, changes):
        for prop_name, new in changes.items():
            prop = cls.metadata[prop_name]
            if not isinstance(prop, AggregatorProperty):
                continue
            orig = cls._storage.is_aggregated(guid, prop_name, prop.value)
            if new == env.value(orig):
                del changes[prop_name]
            elif prop.counter:
                changes[prop.counter] = '1' if int(new) else '-1'

    @classmethod
    def _post_store(cls, guid, changes):
        for prop_name, new in changes.items():
            prop = cls.metadata[prop_name]
            if not isinstance(prop, AggregatorProperty):
                continue
            if int(new):
                cls._storage.aggregate(guid, prop_name, prop.value)
            else:
                cls._storage.disaggregate(guid, prop_name, prop.value)
            if prop.counter:
                del changes[prop.counter]
            del changes[prop_name]

        cls._storage.put(guid, changes)

    def _get_not_storable_but_indexd_props(self):
        prop_names = []
        for name, prop in self.metadata.items():
            if not isinstance(prop, StoredProperty) and \
                    isinstance(prop, IndexedProperty):
                prop_names.append(name)
        documents, __ = self.find(0, 1,
                request={'guid': self.guid}, reply=prop_names)
        for doc in documents:
            for name in prop_names:
                __, new = self._cache.get(name, (None, None))
                self._cache[name] = (doc[name], new)

    def _set_seqno(self, properties):
        if 'seqno' not in self._raw:
            properties['seqno'] = self._seqno.next()
