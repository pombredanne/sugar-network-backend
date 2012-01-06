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


from gettext import gettext as _

from active_document import env
from active_document.util import enforce


class Metadata(dict):
    """Structure to describe the document.

    Dictionary derived class that contains `Property` objects.

    """
    #: Document type name
    name = None

    #: Function that returns an interator that should return (guid, props)
    #: for every existing document
    crawler = None

    #: Function to convert a tuple of (guid, props) to document object
    to_document = None

    def __getitem__(self, prop_name):
        enforce(prop_name in self, _('There is no "%s" property in %s'),
                prop_name, self.name)
        return dict.__getitem__(self, prop_name)


class Property(object):
    """Bacis class to collect information about document property."""

    def __init__(self, name, large=False, default=None):
        self._name = name
        self._large = large
        self._default = default
        self.writable = False

    @property
    def name(self):
        """Property name."""
        return self._name

    @property
    def large(self):
        """Property values are large enough for `Document.find()`.

        Create `Document` object to get access to these properties for
        particular document.

        """
        return self._large

    @property
    def default(self):
        """Default property value or None."""
        return self._default


class IndexedProperty(Property):
    """Property that need to be indexed."""

    def __init__(self, name,
            slot=None, prefix=None, full_text=False,
            boolean=False, multiple=False, separator=None,
            large=False, default=None, typecast=None):
        enforce(name == 'guid' or slot != 0,
                _('For %s property, ' \
                        'the slot "0" is reserved for internal needs'),
                name)
        enforce(name == 'guid' or prefix != env.GUID_PREFIX,
                _('For %s property, ' \
                        'the prefix "I" is reserved for internal needs'),
                name)
        enforce(slot is not None or prefix or full_text,
                _('For %s property, ' \
                        'either slot, prefix or full_text need to be set'),
                name)
        Property.__init__(self, name, large=large, default=default)
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

    def __init__(self, name,
            construct_only=False, write_access=None,
            large=False, default=None):
        Property.__init__(self, name, large=large, default=default)
        self._construct_only = construct_only
        self._write_access = write_access

    @property
    def construct_only(self):
        """Property can be set only while document creation."""
        return self._construct_only

    @property
    def write_access(self):
        """Write access mode to handled in dowstreamed `Document.authorize`."""
        return self._write_access


class ActiveProperty(IndexedProperty, StoredProperty):

    def __init__(self, name,
            slot=None, prefix=None, full_text=False,
            boolean=False, multiple=False, separator=None,
            construct_only=False, write_access=None,
            large=False, default=None, typecast=None):
        IndexedProperty.__init__(self, name, slot=slot, prefix=prefix,
                full_text=full_text, boolean=boolean, multiple=multiple,
                separator=separator, typecast=typecast)
        StoredProperty.__init__(self, name, construct_only=construct_only,
                write_access=write_access, large=large, default=default)


class GuidProperty(ActiveProperty):

    def __init__(self):
        ActiveProperty.__init__(self, 'guid', slot=0, prefix=env.GUID_PREFIX)


class CounterProperty(IndexedProperty):
    """Only index property that can be changed only by incrementing.

    For reading it is an `int` type (in string as usual) property.
    For setting, new value will be treated as a delta to already indexed
    value.

    """

    def __init__(self, name, slot):
        IndexedProperty.__init__(self, name, slot=slot, default='0')


class BlobProperty(Property):
    """Binary large objects that need to be fetched alone.

    To get access to these properties, use `Document.send()` and
    `Document.receive()` functions.

    """

    def __init__(self, name, mime_type='application/octet-stream'):
        Property.__init__(self, name, large=True)
        self._mime_type = mime_type

    @property
    def mime_type(self):
        """MIME type for BLOB content.

        By default, MIME type is application/octet-stream.

        """
        return self._mime_type


class GroupedProperty(Property):

    def __init__(self):
        Property.__init__(self, 'grouped')
