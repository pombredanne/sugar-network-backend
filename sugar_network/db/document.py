# Copyright (C) 2011-2012 Aleksey Lim
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

from sugar_network.db import env
from sugar_network.db.metadata import StoredProperty, PropertyMetadata
from sugar_network.db.metadata import indexed_property


_logger = logging.getLogger('db.document')


class Document(object):
    """Base class for all data classes."""

    #: `Metadata` object that describes the document
    metadata = None

    def __init__(self, guid, record, cached_props=None, request=None):
        self.props = cached_props or {}
        self.guid = guid
        self.is_new = not bool(guid)
        self._record = record
        self.request = request
        self._modifies = set()

    @property
    def volume(self):
        return self.request.commands.volume

    @property
    def directory(self):
        return self.volume[self.metadata.name]

    @indexed_property(slot=1000, prefix='IC', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def ctime(self, value):
        return value

    @indexed_property(slot=1001, prefix='IM', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def mtime(self, value):
        return value

    @indexed_property(slot=1002, prefix='IS', typecast=int,
            permissions=0, default=0)
    def seqno(self, value):
        return value

    def get(self, prop, accept_language=None):
        """Get document's property value.

        :param prop:
            property name to get value
        :returns:
            `prop` value

        """
        prop = self.metadata[prop]

        value = self.props.get(prop.name)
        if value is None and self._record is not None:
            meta = self._record.get(prop.name)
            if isinstance(prop, StoredProperty):
                if meta is not None:
                    value = meta.get('value')
                else:
                    value = prop.default
            else:
                value = meta or PropertyMetadata()
            self.props[prop.name] = value

        if value is not None and accept_language:
            if isinstance(prop, StoredProperty) and prop.localized:
                value = env.gettext(value, accept_language)

        return value

    def properties(self, props, accept_language=None):
        result = {}
        for i in props:
            result[i] = self.get(i, accept_language)
        return result

    def meta(self, prop):
        return self._record.get(prop)

    def modified(self, prop):
        return prop in self._modifies

    def __getitem__(self, prop):
        return self.get(prop)

    def __setitem__(self, prop, value):
        self.props[prop] = value
        self._modifies.add(prop)
