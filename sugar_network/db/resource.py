# Copyright (C) 2011-2014 Aleksey Lim
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

from sugar_network.db.metadata import indexed_property, Localized
from sugar_network.db.metadata import Numeric, List, Authors
from sugar_network.db.metadata import Composite, Aggregated
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit.router import ACL


class Resource(object):
    """Base class for all data classes."""

    #: `Metadata` object that describes the document
    metadata = None

    def __init__(self, guid, record, cached_props=None):
        self.props = cached_props or {}
        self.guid = guid
        self.is_new = not bool(guid)
        self.record = record
        self._post_seqno = None

    @property
    def post_seqno(self):
        return self._post_seqno

    @post_seqno.setter
    def post_seqno(self, value):
        if self._post_seqno is None:
            self._post_seqno = value
            self.post('seqno', value)

    @indexed_property(Numeric, slot=1000, prefix='RS', acl=0)
    def seqno(self, value):
        return value

    @indexed_property(Numeric, slot=1001, prefix='RC', default=0, acl=ACL.READ)
    def ctime(self, value):
        return value

    @indexed_property(Numeric, slot=1002, prefix='RM', default=0, acl=ACL.READ)
    def mtime(self, value):
        return value

    @indexed_property(Authors, prefix='RA', default={}, full_text=True,
            acl=ACL.READ)
    def author(self, value):
        return value

    @indexed_property(List, prefix='RL', default=[])
    def layer(self, value):
        return value

    @layer.setter
    def layer(self, value):
        orig = self['layer']
        if 'deleted' in value:
            if this.request.method != 'POST' and 'deleted' not in orig:
                self.deleted()
        elif this.request.method != 'POST' and 'deleted' in orig:
            self.restored()
        return value

    @indexed_property(List, prefix='RT', full_text=True, default=[])
    def tags(self, value):
        return value

    @property
    def exists(self):
        return self.record is not None and self.record.consistent

    def deleted(self):
        pass

    def restored(self):
        pass

    def get(self, prop):
        """Get document's property value.

        :param prop:
            property name to get value
        :returns:
            `prop` value

        """
        prop = self.metadata[prop]
        value = self.props.get(prop.name)
        if value is None and self.record is not None:
            meta = self.record.get(prop.name)
            if meta is not None:
                value = meta.get('value')
            else:
                value = prop.default
            self.props[prop.name] = value
        return value

    def properties(self, props):
        result = {}
        for i in props:
            result[i] = self.get(i)
        return result

    def meta(self, prop):
        if self.record is not None:
            return self.record.get(prop)

    def diff(self, seq):
        for name, prop in self.metadata.items():
            if name == 'seqno' or prop.acl & ACL.CALC:
                continue
            meta = self.meta(name)
            if meta is None:
                continue
            seqno = meta.get('seqno')
            if seqno not in seq:
                continue
            value = meta.get('value')
            if isinstance(prop, Aggregated):
                value_ = {}
                for key, agg in value.items():
                    if agg.pop('seqno') in seq:
                        value_[key] = agg
                value = value_
            meta = {'mtime': meta['mtime'], 'value': value}
            yield name, meta, seqno

    def patch(self, props):
        if not props:
            return {}
        patch = {}
        for prop, value in props.items():
            if self[prop] == value:
                continue
            orig_value = self[prop]
            if orig_value and isinstance(self.metadata[prop], Localized):
                for lang, subvalue in value.items():
                    if orig_value.get(lang) != subvalue:
                        break
                else:
                    continue
            patch[prop] = value
        return patch

    def post(self, prop, value, **meta):
        prop = self.metadata[prop]
        if prop.on_set is not None:
            value = prop.on_set(self, value)
        if isinstance(prop, Aggregated):
            for agg in value.values():
                agg['seqno'] = self.post_seqno
        if isinstance(prop, Composite):
            old_value = self[prop.name]
            if old_value:
                old_value.update(value)
                value = old_value
        self.record.set(prop.name, value=value, seqno=self.post_seqno, **meta)
        self.props[prop.name] = value

    def _set(self, prop, value):
        self.props[prop] = value

    def __contains__(self, prop):
        return prop in self.props

    def __getitem__(self, prop):
        return self.get(prop)
