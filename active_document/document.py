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

import time
import logging
from gettext import gettext as _

from active_document import env
from active_document.metadata import BrowsableProperty, StoredProperty
from active_document.metadata import active_property
from active_toolkit import enforce


_logger = logging.getLogger('active_document.document')


class Document(object):

    #: `Metadata` object that describes the document
    metadata = None

    #: To invalidate existed index on stcuture changes
    LAYOUT_VERSION = 1

    def __init__(self, guid, indexed_props=None, record=None):
        self._guid = guid
        self._props = indexed_props or {}
        self._record = record

    @active_property(slot=1000, prefix='IC', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def ctime(self, value):
        return value

    @active_property(slot=1001, prefix='IM', typecast=int,
            permissions=env.ACCESS_READ, default=0)
    def mtime(self, value):
        return value

    @active_property(slot=1002, prefix='IS', typecast=int,
            permissions=0, default=0)
    def seqno(self, value):
        return value

    @active_property(prefix='IL', full_text=True, typecast=[env.LAYERS],
            default=['public'], permissions=env.ACCESS_READ)
    def layer(self, value):
        return value

    @active_property(prefix='IU', typecast=[],
            permissions=env.ACCESS_CREATE | env.ACCESS_READ)
    def user(self, value):
        return value

    @property
    def guid(self):
        """Document GUID."""
        return self._guid

    def get(self, prop):
        """Get document's property value.

        :param prop:
            property name to get value
        :returns:
            `prop` value

        """
        prop = self.metadata[prop]

        value = self._props.get(prop.name)
        if value is not None:
            return value

        if self._record is not None and isinstance(prop, StoredProperty):
            value = self._record.get(prop.name, prop.default)
        else:
            raise RuntimeError(_('Property %r in %r cannot be get') % \
                    (prop.name, self.metadata.name))

        self._props[prop.name] = value

        return value

    def get_seqno(self, prop):
        prop = self.metadata[prop]
        enforce(self._record is not None, _('Property %r in %r cannot be get'),
                prop.name, self.metadata.name)
        return self._record.get_seqno(prop.name)

    def properties(self, names=None):
        result = {}
        if names:
            for prop_name in names:
                result[prop_name] = self[prop_name]
        else:
            for prop_name, prop in self.metadata.items():
                if isinstance(prop, BrowsableProperty) and \
                        prop.permissions & env.ACCESS_READ:
                    result[prop_name] = self[prop_name]
        return result

    @classmethod
    def before_create(cls, props):
        """Callback to call on document creation.

        Function needs to be re-implemented in child classes.

        :param props:
            dictionary with new document properties values

        """
        ts = int(time.time())
        props['ctime'] = ts
        props['mtime'] = ts

        # TODO until implementing layers support
        props['layer'] = ['public']

    @classmethod
    def before_update(cls, props):
        """Callback to call on existing document modification.

        Function needs to be re-implemented in child classes.

        :param props:
            dictionary with document properties updates

        """
        props['mtime'] = int(time.time())

    @classmethod
    def before_post(cls, props):
        """Callback to call on existing document before posting changes.

        Function needs to be re-implemented in child classes.

        :param props:
            dictionary with document properties updates

        """
        pass

    def __getitem__(self, prop):
        return self.get(prop)
