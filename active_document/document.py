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

import uuid
import logging
from gettext import gettext as _

from active_document import env, util
from active_document.storage import Storage
from active_document.metadata import Metadata
from active_document.metadata import ActiveProperty, StoredProperty
from active_document.metadata import GuidProperty, GroupedProperty
from active_document.metadata import AggregatorProperty, IndexedProperty
from active_document.metadata import BlobProperty
from active_document.index import IndexWriter
from active_document.index_proxy import IndexProxy
from active_document.util import enforce


_logger = logging.getLogger('ad.document')


class Document(object):

    #: `Metadata` object that describes the document
    metadata = None

    _initated = False
    _storage = None
    _index = None

    def __init__(self, guid=None, indexed_props=None, **kwargs):
        """
        :param guid:
            GUID of existing document; if omitted, newly created object
            will be associated with new document; new document will be saved
            only after calling `post`
        :param indexed_props:
            property values got from index to populate the cache
        :param kwargs:
            optional key arguments with new property values; specifing these
            arguments will mean the same as setting properties after `Document`
            object creation

        """
        self.init()
        self._is_new = False
        self._cache = {}
        self._record = None

        if indexed_props is None:
            indexed_props = {}

        if guid:
            self._guid = guid

            for prop_name, value in indexed_props.items():
                self._cache[prop_name] = (value, None)
        else:
            self._is_new = True
            self._guid = str(uuid.uuid1())
            kwargs['guid'] = self.guid
            self.on_create(kwargs)

            for name, prop in self.metadata.items():
                if isinstance(prop, StoredProperty):
                    if name in kwargs:
                        continue
                    enforce(prop.default is not None,
                            _('Property "%s" should be passed while ' \
                                    'creating new %s document'),
                            name, self.metadata.name)
                if prop.default is not None:
                    self._cache[name] = (None, prop.default)

        for prop_name, value in kwargs.items():
            self[prop_name] = value

    @property
    def guid(self):
        """Document GUID."""
        return self._guid

    def __getitem__(self, prop_name):
        """Get document's property value.

        :param prop_name:
            property name to get value
        :returns:
            `prop_name` value

        """
        orig, new = self._cache.get(prop_name, (None, None))

        if new is not None:
            return new
        if orig is not None:
            return orig

        prop = self.metadata[prop_name]
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
            enforce(orig is not None, _('Property "%s" in %s cannot be get'),
                    prop_name, self.metadata.name)

        self._cache[prop_name] = (orig, new)
        return orig

    def __setitem__(self, prop_name, value):
        """set document's property value.

        :param prop_name:
            property name to set
        :param value:
            property value to set

        """
        prop = self.metadata[prop_name]

        if isinstance(prop, StoredProperty):
            enforce(self._is_new or not prop.construct_only,
                    _('Property "%s" in %s is creation only'),
                    prop_name, self.metadata.name)
            enforce(self.authorize(prop_name),
                    _('You are not permitted to change "%s" property in %s'),
                    prop_name, self.metadata.name)
        elif isinstance(prop, AggregatorProperty):
            enforce(value.isdigit(),
                    _('Property "%s" in %s should be either "0" or "1"'),
                    prop_name, self.metadata.name)
            value = env.value(bool(int(value)))
        else:
            raise RuntimeError(_('Property "%s" in %s cannot be set') % \
                    (prop_name, self.metadata.name))

        orig, __ = self._cache.get(prop_name, (None, None))
        self._cache[prop_name] = (orig, value)

    def post(self):
        """Store changed properties."""
        changes = {}
        for prop_name, (__, new) in self._cache.items():
            if new is not None:
                changes[prop_name] = new
        if not changes:
            return

        if not self._is_new:
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
                error = _('Value for "%s" property for %s is invalid') % \
                        (prop_name, self.metadata.name)
                util.exception(error)
                raise RuntimeError(error)

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
        enforce(isinstance(self.metadata[prop_name], BlobProperty),
                _('Property "%s" in %s is not a BLOB'),
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
        enforce(isinstance(self.metadata[prop_name], BlobProperty),
                _('Property "%s" in %s is not a BLOB'),
                prop_name, self.metadata.name)
        self._storage.set_blob(self.guid, prop_name, stream, size)

    def authorize(self, prop_name):
        """Does caller have permissions to write to the specified property.

        Function needs to be re-implemented in child classes.

        :param prop_name:
            property name to check access
        :returns:
            `True` if caller can write to `prop_name`

        """
        return True

    def on_create(self, properties):
        """Call back to call on document creation.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with new document properties values

        """
        pass

    def on_modify(self, properties):
        """Call back to call on existing document modification.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        pass

    def on_post(self, properties):
        """Call back to call on exery `post()` call.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        pass

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
    def delete(cls, guid):
        """Delete document.

        :param guid:
            document GUID to delete

        """
        cls._index.delete(guid, lambda guid: cls._storage.delete(guid))

    @classmethod
    def find(cls, offset=None, limit=None, request=None, query='',
            reply=None, order_by=None, group_by=None):
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
            array of properties to sort resulting list; property names might be
            prefixed with ``+`` (or without any prefixes) for ascending order,
            and ``-`` for descending order
        :param group_by:
            a property name to group resulting list by; if was specified,
            every resulting list item will contain `grouped` with
            a number of documents that are represented by the current one;
            no groupping by default
        :returns:
            a tuple of (`documents`, `total_count`); where the `total_count` is
            the total number of documents conforming the search parameters,
            i.e., not only documents that are included to the resulting list

        """
        if offset is None:
            offset = 0
        if limit is None:
            limit = env.find_limit.value
        elif limit > env.find_limit.value:
            _logger.warning(_('The find limit for %s is restricted to %s'),
                    cls.metadata.name, env.find_limit.value)
            limit = env.find_limit.value
        if request is None:
            request = {}
        if not reply:
            reply = ['guid']
        if order_by is None and 'ctime' in cls.metadata:
            order_by = ['+ctime']

        for prop_name in reply:
            enforce(not cls.metadata[prop_name].large,
                    _('Property "%s" in %s is not suitable for find requests'),
                    prop_name, cls.metadata.name)

        documents, total_count = cls._index.find(offset, limit, request, query,
                reply, order_by, group_by)

        def iterate():
            for guid, props in documents:
                yield cls(guid, indexed_props=props)

        return iterate(), total_count

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
    def init(cls):
        if cls._initated:
            return

        cls.metadata = Metadata()
        cls.metadata.name = cls.__name__.lower()
        cls.metadata['guid'] = GuidProperty()
        cls.metadata['grouped'] = GroupedProperty()

        cls._storage = Storage(cls.metadata)

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
