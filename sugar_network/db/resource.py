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

from sugar_network import toolkit
from sugar_network.db.metadata import StoredProperty, indexed_property
from sugar_network.toolkit.router import Blob, ACL


class Resource(object):
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
        return self.request.routes.volume

    @property
    def directory(self):
        return self.volume[self.metadata.name]

    @indexed_property(slot=1000, prefix='RC', typecast=int, default=0,
            acl=ACL.READ)
    def ctime(self, value):
        return value

    @indexed_property(slot=1001, prefix='RM', typecast=int, default=0,
            acl=ACL.READ)
    def mtime(self, value):
        return value

    @indexed_property(slot=1002, prefix='RS', typecast=int, default=0, acl=0)
    def seqno(self, value):
        return value

    @indexed_property(prefix='RA', typecast=dict, full_text=True, default={},
            fmt=lambda x: _fmt_authors(x), acl=ACL.READ)
    def author(self, value):
        result = []
        for guid, props in sorted(value.items(),
                cmp=lambda x, y: cmp(x[1]['order'], y[1]['order'])):
            if 'name' in props:
                result.append({
                    'guid': guid,
                    'name': props['name'],
                    'role': props['role'],
                    })
            else:
                result.append({
                    'name': guid,
                    'role': props['role'],
                    })
        return result

    @indexed_property(prefix='RL', typecast=[], default=[])
    def layer(self, value):
        return value

    @indexed_property(prefix='RT', full_text=True, default=[], typecast=[])
    def tags(self, value):
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
                value = meta or Blob()
            self.props[prop.name] = value

        if value is not None and accept_language:
            if isinstance(prop, StoredProperty) and prop.localized:
                value = toolkit.gettext(value, accept_language)

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


def _fmt_authors(value):
    if isinstance(value, dict):
        for guid, props in value.items():
            if not isinstance(props, dict):
                yield guid
            else:
                if 'name' in props:
                    yield props['name']
                if not (props['role'] & ACL.INSYSTEM):
                    yield guid
    else:
        yield value
