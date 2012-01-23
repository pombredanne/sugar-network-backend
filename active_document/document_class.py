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
from active_document.metadata import Metadata
from active_document.metadata import ActiveProperty, AggregatorProperty
from active_document.metadata import GuidProperty, SeqnoProperty
from active_document.metadata import CounterProperty
from active_document.index import IndexWriter
from active_document.index_proxy import IndexProxy
from active_document.sync import Seqno
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


class DocumentClass(object):

    #: `Metadata` object that describes the document
    metadata = None

    _initated = False
    _storage = None
    _index = None
    _seqno = None

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

    def post(self):
        """Store changed properties."""
        raise NotImplementedError()

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
                counter = env.value(
                        cls._storage.count_aggregated(guid, prop.name))
                if counter != props[prop.counter]:
                    cls._storage.put(guid, {prop.counter: counter})
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
        is_new = 'guid' in changes

        for prop_name, new in changes.items():
            prop = cls.metadata[prop_name]
            if not isinstance(prop, AggregatorProperty):
                continue
            orig = cls._storage.is_aggregated(guid, prop_name, prop.value)
            if new == env.value(orig):
                if not is_new:
                    del changes[prop_name]
            elif prop.counter:
                if int(new):
                    changes[prop.counter] = '1'
                elif not is_new:
                    changes[prop.counter] = '-1'

        if not is_new:
            record = cls._storage.get(guid)
            for prop_name, prop in cls.metadata.items():
                if prop_name in changes:
                    if isinstance(prop, CounterProperty):
                        changes[prop_name] = str(
                                int(record.get(prop_name) or '0') + \
                                int(changes[prop_name]))
                else:
                    changes[prop_name] = record.get(prop_name)

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
            del changes[prop_name]

        cls._storage.put(guid, changes)
