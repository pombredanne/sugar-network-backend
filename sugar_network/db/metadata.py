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
import types
import json
from os.path import exists

from sugar_network import toolkit
from sugar_network.db import env
from sugar_network.toolkit import http, enforce


#: Xapian term prefix for GUID value
GUID_PREFIX = 'I'

_LIST_TYPES = (list, tuple, frozenset, types.GeneratorType)


def indexed_property(property_class=None, *args, **kwargs):

    def getter(func, self):
        value = self[func.__name__]
        return func(self, value)

    def decorate_setter(func, attr):
        attr.prop.setter = lambda self, value: \
                self.set(attr.name, func(self, value))
        attr.prop.on_set = func
        return attr

    def decorate_getter(func):
        enforce(func.__name__ != 'guid',
                "Active property should not have 'guid' name")
        attr = lambda self: getter(func, self)
        attr.setter = lambda func: decorate_setter(func, attr)
        # pylint: disable-msg=W0212
        attr._is_db_property = True
        attr.name = func.__name__
        attr.prop = (property_class or IndexedProperty)(
                attr.name, *args, **kwargs)
        attr.prop.on_get = func
        return attr

    return decorate_getter


stored_property = lambda ** kwargs: indexed_property(StoredProperty, **kwargs)
blob_property = lambda ** kwargs: indexed_property(BlobProperty, **kwargs)


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """

    def __init__(self, cls):
        """
        :param cls:
            class inherited from `db.Document`

        """
        self._name = cls.__name__.lower()

        slots = {}
        prefixes = {}

        for attr in [getattr(cls, i) for i in dir(cls)]:
            if not hasattr(attr, '_is_db_property'):
                continue

            prop = attr.prop

            if hasattr(prop, 'slot'):
                enforce(prop.slot is None or prop.slot not in slots,
                        'Property %r has a slot already defined for %r in %r',
                        prop.name, slots.get(prop.slot), self.name)
                slots[prop.slot] = prop.name

            if hasattr(prop, 'prefix'):
                enforce(not prop.prefix or prop.prefix not in prefixes,
                        'Property %r has a prefix already defined for %r',
                        prop.name, prefixes.get(prop.prefix))
                prefixes[prop.prefix] = prop.name

            if prop.setter is not None:
                setattr(cls, attr.name, property(attr, prop.setter))
            else:
                setattr(cls, attr.name, property(attr))

            self[prop.name] = prop

    @property
    def name(self):
        """Document type name."""
        return self._name

    def __getitem__(self, prop_name):
        enforce(prop_name in self, 'There is no %r property in %r',
                prop_name, self.name)
        return dict.__getitem__(self, prop_name)


class PropertyMetadata(dict):

    BLOB_SUFFIX = '.blob'

    def __init__(self, path_=None, **meta):
        if path_:
            with file(path_) as f:
                meta.update(json.load(f))
            if exists(path_ + PropertyMetadata.BLOB_SUFFIX):
                meta['blob'] = path_ + PropertyMetadata.BLOB_SUFFIX
            meta['mtime'] = int(os.stat(path_).st_mtime)
        dict.__init__(self, meta)


class Property(object):
    """Basic class to collect information about document property."""

    def __init__(self, name, permissions=env.ACCESS_PUBLIC, typecast=None,
            reprcast=None, default=None):
        self.setter = None
        self.on_get = lambda self, x: x
        self.on_set = None
        self._name = name
        self._permissions = permissions
        self._typecast = typecast
        self._reprcast = reprcast
        self._default = default

    @property
    def name(self):
        """Property name."""
        return self._name

    @property
    def permissions(self):
        """Specify access to the property.

        Value might be ORed composition of `db.ACCESS_*`
        constants.

        """
        return self._permissions

    @property
    def typecast(self):
        """Cast property value before storing in the system.

        Supported values are:

        * `None`, string values
        * `int`, interger values
        * `float`, float values
        * `bool`, boolean values repesented by symbols `0` and `1`
        * sequence of strings, property value should confirm one of values
          from the sequence

        """
        return self._typecast

    @property
    def composite(self):
        """Is property value a list of values."""
        is_composite, __ = _is_composite(self.typecast)
        return is_composite

    @property
    def default(self):
        """Default property value or None."""
        return self._default

    def decode(self, value):
        """Convert property value according to its `typecast`."""
        if self.typecast is None:
            return value
        return _decode(self.typecast, value)

    def to_string(self, value):
        """Convert value to list of strings ready to index."""
        result = []

        if self._reprcast is not None:
            value = self._reprcast(value)

        for subvalue in (value if type(value) in _LIST_TYPES else [value]):
            if type(subvalue) is bool:
                subvalue = int(subvalue)
            if type(subvalue) is unicode:
                subvalue = unicode(subvalue).encode('utf8')
            else:
                subvalue = str(subvalue)
            result.append(subvalue)

        return result

    def assert_access(self, mode):
        """Is access to the property permitted.

        If there are no permissions, function should raise
        `http.Forbidden` exception.

        :param mode:
            one of `db.ACCESS_*` constants
            to specify the access mode

        """
        enforce(mode & self.permissions, http.Forbidden,
                '%s access is disabled for %r property',
                env.ACCESS_NAMES[mode], self.name)


class StoredProperty(Property):
    """Property to save only in persistent storage, no index."""

    def __init__(self, name, localized=False, typecast=None, reprcast=None,
            **kwargs):
        """
        :param: **kwargs
            :class:`.Property` arguments

        """
        self._localized = localized

        if localized:
            enforce(typecast is None,
                    'typecast should be None for localized properties')
            enforce(reprcast is None,
                    'reprcast should be None for localized properties')
            typecast = _localized_typecast
            reprcast = _localized_reprcast

        Property.__init__(self, name, typecast=typecast, reprcast=reprcast,
                **kwargs)

    @property
    def localized(self):
        """Property value will be stored per locale."""
        return self._localized


class IndexedProperty(StoredProperty):
    """Property which needs to be indexed."""

    def __init__(self, name, slot=None, prefix=None, full_text=False,
            boolean=False, **kwargs):
        """
        :param: **kwargs
            :class:`.StoredProperty` arguments

        """
        enforce(name == 'guid' or slot != 0,
                "For %r property, slot '0' is reserved for internal needs",
                name)
        enforce(name == 'guid' or prefix != GUID_PREFIX,
                'For %r property, prefix %r is reserved for internal needs',
                name, GUID_PREFIX)
        enforce(slot is not None or prefix or full_text,
                'For %r property, either slot, prefix or full_text '
                'need to be set',
                name)
        enforce(slot is None or _is_sloted_prop(kwargs.get('typecast')),
                'Slot can be set only for properties for str, int, float, '
                'bool types, or, for list of these types')

        StoredProperty.__init__(self, name, **kwargs)
        self._slot = slot
        self._prefix = prefix
        self._full_text = full_text
        self._boolean = boolean

    @property
    def slot(self):
        """Xapian document's slot number to add property value to."""
        return self._slot

    @property
    def prefix(self):
        """Xapian serach term prefix, if `None`, property is not a term."""
        return self._prefix

    @property
    def full_text(self):
        """Property takes part in full-text search."""
        return self._full_text

    @property
    def boolean(self):
        """Xapian will use boolean search for this property."""
        return self._boolean


class BlobProperty(Property):
    """Binary large objects which needs to be fetched alone, no index."""

    def __init__(self, name, permissions=env.ACCESS_PUBLIC,
            mime_type='application/octet-stream', composite=False):
        """
        :param: **kwargs
            :class:`.Property` arguments

        """
        Property.__init__(self, name, permissions=permissions)
        self._mime_type = mime_type
        self._composite = composite

    @property
    def mime_type(self):
        """MIME type for BLOB content.

        By default, MIME type is application/octet-stream.

        """
        return self._mime_type

    @property
    def composite(self):
        """Property is a list of BLOBs."""
        return self._composite


def _is_composite(typecast):
    if type(typecast) in _LIST_TYPES:
        if typecast:
            first = iter(typecast).next()
            if type(first) is not type and \
                    type(first) not in _LIST_TYPES:
                return False, True
        return True, False
    return False, False


def _decode(typecast, value):
    enforce(value is not None, ValueError, 'Property value cannot be None')

    is_composite, is_enum = _is_composite(typecast)

    if is_composite:
        enforce(len(typecast) <= 1, ValueError,
                'List values should contain values of the same type')
        if type(value) not in _LIST_TYPES:
            value = (value,)
        typecast, = typecast or [str]
        value = tuple([_decode(typecast, i) for i in value])
    elif is_enum:
        enforce(value in typecast, ValueError,
                "Value %r is not in '%s' list",
                value, ', '.join([str(i) for i in typecast]))
    elif isinstance(typecast, types.FunctionType):
        value = typecast(value)
    elif typecast is str:
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            value = str(value)
    elif typecast is int:
        value = int(value)
    elif typecast is float:
        value = float(value)
    elif typecast is bool:
        value = bool(value)
    elif typecast is dict:
        value = dict(value)
    else:
        raise ValueError('Unknown typecast')
    return value


def _is_sloted_prop(typecast):
    if typecast in [None, int, float, bool, str]:
        return True
    if type(typecast) in _LIST_TYPES:
        if typecast and [i for i in typecast
                if type(i) in [None, int, float, bool, str]]:
            return True


def _localized_typecast(value):
    if isinstance(value, dict):
        return value
    else:
        return {toolkit.default_lang(): value}


def _localized_reprcast(value):
    if isinstance(value, dict):
        return value.values()
    else:
        return [value]
