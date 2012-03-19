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
import types
from os.path import join, exists, abspath, dirname
from gettext import gettext as _

from active_document import env, util
from active_document.util import enforce


_LIST_TYPES = (list, tuple, frozenset)


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """

    def __init__(self, name):
        """
        :param name:
            document type name

        """
        self._name = name
        self.ensure_path('')
        self._seqno = 0

        seqno_path = self.path('seqno')
        if exists(seqno_path):
            with file(seqno_path) as f:
                self._seqno = int(f.read().strip())

    @property
    def name(self):
        """Document type name."""
        return self._name

    @property
    def last_seqno(self):
        return self._seqno

    def next_seqno(self):
        self._seqno += 1
        return self._seqno

    def commit_seqno(self):
        with util.new_file(self.path('seqno')) as f:
            f.write(str(self._seqno))
            f.flush()
            os.fsync(f.fileno())

    def path(self, *args):
        """Calculate a path from the root.

        If resulting directory path doesn't exists, it will be created.

        :param args:
            path parts to add to the root path; if ends with empty string,
            the resulting path will be treated as a path to a directory
        :returns:
            absolute path

        """
        result = join(env.data_root.value, self.name, *args)
        return abspath(result)

    def ensure_path(self, *args):
        """Calculate a path from the root.

        If resulting directory path doesn't exists, it will be created.

        :param args:
            path parts to add to the root path; if ends with empty string,
            the resulting path will be treated as a path to a directory
        :returns:
            absolute path

        """
        result = join(env.data_root.value, self.name, *args)
        if result.endswith(os.sep):
            result_dir = result = result.rstrip(os.sep)
        else:
            result_dir = dirname(result)
        if not exists(result_dir):
            os.makedirs(result_dir)
        return abspath(result)

    def __getitem__(self, prop_name):
        enforce(prop_name in self, _('There is no "%s" property in "%s"'),
                prop_name, self.name)
        return dict.__getitem__(self, prop_name)


class Property(object):
    """Bacis class to collect information about document property."""

    def __init__(self, name, permissions=env.ACCESS_FULL, typecast=None,
            reprcast=None, default=None):
        self.setter = None
        self.converter = None
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
        """Value type that property's string value should repesent.

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

    def convert(self, value):
        """Convert specified value to property type."""
        return _convert(self.typecast, value)

    def reprcast(self, value):
        """Convert value to list of strings ready to index."""
        result = []

        if self._reprcast is not None:
            value = self._reprcast(value)

        for value in (value if type(value) in _LIST_TYPES else [value]):
            if type(value) is bool:
                value = int(value)
            result.append(str(value))

        return result


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
                _('For "%s" property, ' \
                        'the slot "0" is reserved for internal needs'),
                name)
        enforce(name == 'guid' or prefix != env.GUID_PREFIX,
                _('For "%s" property, ' \
                        'the prefix "I" is reserved for internal needs'),
                name)
        enforce(slot is not None or prefix or full_text,
                _('For "%s" property, ' \
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


class AggregatorProperty(Property, BrowsableProperty):
    """Property that aggregates arbitrary values.

    This properties is repesented by boolean value (int in string notation)
    that shows that `AggregatorProperty.value` is aggregated or not.
    After setting this property, `AggregatorProperty.value` will be added or
    removed from the aggregatation list.

    """

    def __init__(self, name, counter):
        Property.__init__(self, name, typecast=bool, default=False)
        self._counter = counter

    @property
    def counter(self):
        """Name of `CounterProperty` to keep aggregated items number."""
        return self._counter

    @property
    def value(self):
        raise NotImplementedError()


class CounterProperty(ActiveProperty):
    """Only index property that can be changed only by incrementing.

    For reading it is an `int` type (in string as usual) property.
    For setting, new value will be treated as a delta to already indexed
    value.

    """

    def __init__(self, name, slot):
        ActiveProperty.__init__(self, name,
                permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=slot,
                typecast=int, default=0)


class BlobProperty(Property):
    """Binary large objects that need to be fetched alone.

    To get access to these properties, use `Document.send()` and
    `Document.receive()` functions.

    """

    def __init__(self, name, permissions=env.ACCESS_FULL,
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


def _convert(typecast, value):
    enforce(value is not None, ValueError, _('Property value cannot be None'))

    is_composite, is_enum = _is_composite(typecast)

    if is_composite:
        enforce(len(typecast) <= 1, ValueError,
                _('List values should contain values of the same type'))
        if type(value) not in _LIST_TYPES:
            value = (value,)
        typecast, = typecast or [str]
        value = tuple([_convert(typecast, i) for i in value])
    elif is_enum:
        enforce(value in typecast, ValueError,
                _('Value "%s" is not in "%s" list'),
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
