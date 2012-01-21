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
from os.path import join, exists, abspath, dirname
from gettext import gettext as _

from active_document import env
from active_document.util import enforce


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """
    #: Document type name
    name = None

    def path(self, *args):
        """Calculate a path from the root.

        If resulting directory path doesn't exists, it will be created.

        :param args:
            path parts to add to the root path; if ends with empty string,
            the resulting path will be treated as a path to a directory
        :returns:
            absolute path

        """
        enforce(env.data_root.value,
                _('The active_document.data_root.value is not set'))
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
        enforce(env.data_root.value,
                _('The active_document.data_root.value is not set'))
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

    def __init__(self, name, permissions=env.ACCESS_FULL, default=None):
        self._name = name
        self._permissions = permissions
        self._default = default
        self.writable = False

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
    def default(self):
        """Default property value or None."""
        return self._default

    @property
    def is_trait(self):
        """Property to return from find() requests."""
        return True


class IndexedProperty(Property):
    """Property that need to be indexed."""

    def __init__(self, name,
            slot=None, prefix=None, full_text=False,
            boolean=False, multiple=False, separator=None, typecast=None,
            permissions=env.ACCESS_FULL, default=None):
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
        Property.__init__(self, name, permissions=permissions, default=default)
        self._slot = slot
        self._prefix = prefix
        self._full_text = full_text
        self._boolean = boolean
        self._multiple = multiple
        self._separator = separator
        self._typecast = typecast

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

    @property
    def multiple(self):
        """Should property value be treated as a list of words."""
        return self._multiple

    @property
    def separator(self):
        """Separator for multiplied properties, spaces by default."""
        return self._separator

    @property
    def typecast(self):
        """Value type that property's string value should repesent.

        Supported values are:
        * `None`, string values
        * `int`, interger values
        * `bool`, boolean values repesented by symbols `0` and `1`
        * sequence of strings, property value should confirm one of values
          from the sequence

        """
        return self._typecast

    def list_value(self, value):
        """If property value contains several values, list them all."""
        if self._multiple:
            return [i.strip() for i in value.split(self._separator) \
                    if i.strip()]
        else:
            return [value]


class AggregatorProperty(Property):
    """Property that aggregates arbitrary values.

    This properties is repesented by boolean value (int in string notation)
    that shows that `AggregatorProperty.value` is aggregated or not.
    After setting this property, `AggregatorProperty.value` will be added or
    removed from the aggregatation list.

    """

    def __init__(self, name, counter):
        Property.__init__(self, name, default='0')
        self._counter = counter

    @property
    def counter(self):
        """Name of `CounterProperty` to keep aggregated items number."""
        return self._counter

    @property
    def value(self):
        raise NotImplementedError()


class StoredProperty(Property):
    """Property that can be saved in persistent storare."""
    pass


class ActiveProperty(IndexedProperty, StoredProperty):
    """Default property type."""
    pass


class GuidProperty(ActiveProperty):

    def __init__(self):
        ActiveProperty.__init__(self, 'guid',
                permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=0,
                prefix=env.GUID_PREFIX)


class CounterProperty(IndexedProperty):
    """Only index property that can be changed only by incrementing.

    For reading it is an `int` type (in string as usual) property.
    For setting, new value will be treated as a delta to already indexed
    value.

    """

    def __init__(self, name, slot):
        IndexedProperty.__init__(self, name,
                permissions=env.ACCESS_CREATE | env.ACCESS_READ, slot=slot,
                default='0')


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

    @property
    def is_trait(self):
        return False


class SeqnoProperty(ActiveProperty):
    """Seqno property which is not a trait."""

    def __init__(self, name, **kwargs):
        ActiveProperty.__init__(self, name, permissions=0, typecast=int,
                **kwargs)

    @property
    def is_trait(self):
        return False
