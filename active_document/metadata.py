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

import types
from gettext import gettext as _

from active_document import env
from active_document.util import enforce


_LIST_TYPES = (list, tuple, frozenset)


def active_property(property_class=None, *args, **kwargs):

    def getter(func, self):
        value = self[func.__name__]
        return func(self, value)

    def decorate_setter(func, attr):
        attr.prop.setter = lambda self, value: \
                self.set(attr.name, func(self, value))
        return attr

    def decorate_getter(func):
        enforce(func.__name__ != 'guid',
                _("Active property should not have 'guid' name"))
        attr = lambda self: getter(func, self)
        attr.setter = lambda func: decorate_setter(func, attr)
        attr._is_active_property = True
        attr.name = func.__name__
        attr.prop = (property_class or ActiveProperty)(
                attr.name, *args, **kwargs)
        attr.prop.on_get = func
        return attr

    return decorate_getter


def active_command(**kwargs):

    def decorate(func):
        func._is_active_command = True
        func.kwargs = kwargs
        return func

    return decorate


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """

    def __init__(self, cls):
        """
        :param cls:
            class inherited from `active_document.Document`

        """
        self._name = cls.__name__.lower()

        slots = {}
        prefixes = {}

        for attr in [getattr(cls, i) for i in dir(cls)]:
            if not hasattr(attr, '_is_active_property'):
                continue

            prop = attr.prop

            if hasattr(prop, 'slot'):
                enforce(prop.slot is None or prop.slot not in slots,
                        _('Property %r has a slot already defined ' \
                                'for %r in %r'),
                        prop.name, slots.get(prop.slot), self.name)
                slots[prop.slot] = prop.name

            if hasattr(prop, 'prefix'):
                enforce(not prop.prefix or prop.prefix not in prefixes,
                        _('Property %r has a prefix already defined ' \
                                'for %r'),
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
        enforce(prop_name in self, _('There is no %r property in %r'),
                prop_name, self.name)
        return dict.__getitem__(self, prop_name)


class Property(object):
    """Bacis class to collect information about document property."""

    def __init__(self, name, permissions=env.ACCESS_PUBLIC, typecast=None,
            reprcast=None, default=None):
        self.setter = None
        self.on_get = None
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

        Value might be ORed composition of `active_document.ACCESS_*`
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
        return _decode(self.typecast, value)

    def to_string(self, value):
        """Convert value to list of strings ready to index."""
        result = []

        if self._reprcast is not None:
            value = self._reprcast(value)

        for value in (value if type(value) in _LIST_TYPES else [value]):
            if type(value) is bool:
                value = int(value)
            if type(value) is unicode:
                value = unicode(value).encode('utf8')
            else:
                value = str(value)
            result.append(value)

        return result

    def assert_access(self, mode):
        """Is access to the property permitted.

        If there are no permissions, function should raise
        `active_document.Forbidden` exception.

        :param mode:
            one of `active_document.ACCESS_*` constants
            to specify the access mode

        """
        enforce(mode & self.permissions, env.Forbidden,
                _('%s access is disabled for %r property'),
                env.ACCESS_NAMES[mode], self.name)


class BrowsableProperty(object):
    """Property that can be listed while browsing documents."""
    pass


class StoredProperty(Property, BrowsableProperty):
    """Property that can be saved in persistent storare."""
    pass


class ActiveProperty(StoredProperty):
    """Property that need to be indexed."""

    def __init__(self, name, slot=None, prefix=None, full_text=False,
            boolean=False, **kwargs):
        enforce(name == 'guid' or slot != 0,
                _('For %r property, ' \
                        "the slot '0' is reserved for internal needs"),
                name)
        enforce(name == 'guid' or prefix != env.GUID_PREFIX,
                _('For %r property, ' \
                        'the prefix %r is reserved for internal needs'),
                name, env.GUID_PREFIX)
        enforce(slot is not None or prefix or full_text,
                _('For %r property, ' \
                        'either slot, prefix or full_text need to be set'),
                name)
        enforce(slot is None or _is_sloted_prop(kwargs.get('typecast')),
                _('Slot can be set only for properties for str, int, float, ' \
                        'bool types, or, for list of these types'))

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
    """Binary large objects that need to be fetched alone.

    To get access to these properties, use `Document.send()` and
    `Document.receive()` functions.

    """

    def __init__(self, name, permissions=env.ACCESS_PUBLIC,
            mime_type='application/octet-stream'):
        Property.__init__(self, name, permissions=permissions)
        self._mime_type = mime_type

    @property
    def mime_type(self):
        """MIME type for BLOB content.

        By default, MIME type is application/octet-stream.

        """
        return self._mime_type


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
    enforce(value is not None, ValueError, _('Property value cannot be None'))

    is_composite, is_enum = _is_composite(typecast)

    if is_composite:
        enforce(len(typecast) <= 1, ValueError,
                _('List values should contain values of the same type'))
        if type(value) not in _LIST_TYPES:
            value = (value,)
        typecast, = typecast or [str]
        value = tuple([_decode(typecast, i) for i in value])
    elif is_enum:
        enforce(value in typecast, ValueError,
                _("Value %r is not in '%s' list"),
                value, ', '.join([str(i) for i in typecast]))
    elif type(typecast) is types.FunctionType:
        value = typecast(value)
    elif typecast in [None, str]:
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
        raise ValueError(_('Unknown typecast'))
    return value


def _is_sloted_prop(typecast):
    if typecast in [None, int, float, bool, str]:
        return True
    if type(typecast) in _LIST_TYPES:
        if typecast and [i for i in typecast \
                if type(i) in [None, int, float, bool, str]]:
            return True
