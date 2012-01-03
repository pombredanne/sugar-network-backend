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


class Property(object):
    """Collect inforamtion about document property."""

    def __init__(self, name, slot=None, prefix=None, default=None,
            full_text=False, large=False, blob=False, boolean=False,
            multiple=False, separator=None, construct_only=False,
            write_access=None):
        enforce(name == 'guid' or slot != 0,
                _('The slot "0" is reserved for internal needs'))
        enforce(name == 'guid' or prefix != env.GUID_PREFIX,
                _('The prefix "I" is reserved for internal needs'))
        self._name = name
        self._slot = slot
        self._prefix = prefix
        self._default = default
        self._full_text = full_text
        self._large = large
        self._blob = blob
        self._boolean = boolean
        self._multiple = multiple
        self._separator = separator
        self._write_access = write_access
        self._construct_only = construct_only
        self.writable = False

    @property
    def name(self):
        """Property name."""
        return self._name

    @property
    def slot(self):
        """Xapian document's slot number to add property value to."""
        return self._slot

    @property
    def prefix(self):
        """Xapian serach term prefix, if `None`, property is not a term."""
        return self._prefix

    @property
    def default(self):
        """Default property value or None."""
        return self._default

    @property
    def full_text(self):
        """Property takes part in full-text search."""
        return self._full_text

    @property
    def large(self):
        """Property values are large enough for `Document.find()`.

        Create `Document` object to get access to these properties for
        particular document.

        """
        return self._large

    @property
    def blob(self):
        """Binary large objects that need to be fetched alone.

        To get access to these properties, use `Document.send()` and
        `Document.receive()` functions.

        """
        return self._blob

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
    def write_access(self):
        """Write access mode to handled in dowstreamed `Document.authorize`."""
        return self._write_access

    @property
    def construct_only(self):
        """Property can be set only while document creation."""
        return self._construct_only

    @property
    def indexed(self):
        """Should property be used in index."""
        return not self.blob and \
                (self.slot is not None or self.prefix or self.full_text)

    def list_value(self, value):
        """If property value contains several values, list them all."""
        if self._multiple:
            return [i.strip() for i in value.split(self._separator) \
                    if i.strip()]
        else:
            return [value]


class GuidProperty(Property):

    def __init__(self):
        Property.__init__(self, 'guid', 0, env.GUID_PREFIX)
