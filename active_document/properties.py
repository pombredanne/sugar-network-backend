# Copyright (C) 2011, Aleksey Lim
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


class Property(object):
    """Collect inforamtion about document property."""

    def __init__(self, name, slot, prefix=None, default=None, boolean=False,
            multiple=False, separator=None):
        """
        :param name:
            property name
        :param slot:
            document's slot number to add property value to;
            if `None`, property is not a Xapian value
        :param prefix:
            serach term prefix;
            if `None`, property is not a search term
        :param default:
            default property value to use while creating new documents
        :param boolean:
            if `prefix` is not `None`, this argument specifies will
            Xapian use boolean search for that property or not
        :param multiple:
            should property value be treated as a list of words
        :param separator:
            if `multiple` set, this will be a separator;
            otherwise any space symbols will be used to separate words

        """
        enforce(name == 'guid' or slot != 0,
                _('The slot "0" is reserved for internal needs'))
        enforce(name == 'guid' or prefix != env.GUID_PREFIX,
                _('The prefix "I" is reserved for internal needs'))
        self._name = name
        self._slot = slot
        self._prefix = prefix
        self._default = default
        self._boolean = boolean
        self._multiple = multiple
        self._separator = separator

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
    def boolean(self):
        """Xapian will use boolean search for this property."""
        return self._boolean

    @property
    def default(self):
        """Default property value or None."""
        return self._default

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
