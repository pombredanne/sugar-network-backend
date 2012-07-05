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
from gettext import gettext as _

from active_document import env
from active_document.metadata import BrowsableProperty, StoredProperty
from active_document.metadata import BlobProperty
from active_document.metadata import active_property
from active_toolkit import enforce


_logger = logging.getLogger('active_document.document')


class Document(object):

    #: `Metadata` object that describes the document
    metadata = None

    def __init__(self, guid, record, cached_props=None):
        self._guid = guid
        self._props = cached_props or {}
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

    @active_property(prefix='IU', typecast=[], default=[],
            permissions=env.ACCESS_CREATE | env.ACCESS_READ)
    def user(self, value):
        return value

    @property
    def guid(self):
        """Document GUID."""
        return self._guid

    def get(self, prop, accept_language=None):
        """Get document's property value.

        :param prop:
            property name to get value
        :returns:
            `prop` value

        """
        prop = self.metadata[prop]

        value = self._props.get(prop.name)
        if value is None:
            enforce(isinstance(prop, StoredProperty),
                    _('No way to get %r property from %s[%s]'),
                    prop.name, self.metadata.name, self.guid)
            meta = self._record.get(prop.name)
            value = prop.default if meta is None else meta['value']
            self._props[prop.name] = value

        if accept_language and prop.localized:
            value = self._localize(value, accept_language)

        return value

    def meta(self, prop):
        prop = self.metadata[prop]
        result = self._record.get(prop.name)
        if result is not None and isinstance(prop, BlobProperty):
            prop.on_get(self, result)
        return result

    def properties(self, names=None, accept_language=None):
        result = {}

        if names:
            for prop_name in names:
                value = self[prop_name]
                if accept_language and self.metadata[prop_name].localized:
                    value = self._localize(value, accept_language)
                result[prop_name] = value
        else:
            for prop_name, prop in self.metadata.items():
                if not isinstance(prop, BrowsableProperty) or \
                        not prop.permissions & env.ACCESS_READ:
                    continue
                value = self[prop_name]
                if accept_language and prop.localized:
                    value = self._localize(value, accept_language)
                result[prop_name] = value

        return result

    def __getitem__(self, prop):
        return self.get(prop)

    def _localize(self, value, accept_language):
        if not value:
            return ''
        if not isinstance(value, dict):
            return value

        for lang in accept_language + [env.DEFAULT_LANG]:
            result = value.get(lang)
            if result is not None:
                return result
            lang = lang.split('-')
            if len(lang) == 1:
                continue
            result = value.get(lang[0])
            if result is not None:
                return result

        # TODO
        return value[sorted(value.keys())[0]]
