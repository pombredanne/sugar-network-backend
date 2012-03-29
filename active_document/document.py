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

from active_document import env, util
from active_document.document_class import DocumentClass
from active_document.metadata import BlobProperty, StoredProperty
from active_document.metadata import AggregatorProperty
from active_document.util import enforce


_logger = logging.getLogger('ad.document')


class Document(DocumentClass):

    def __init__(self, guid=None, indexed_props=None, **kwargs):
        """
        :param guid:
            GUID of existing document; if omitted, newly created object
            will be associated with new document; new document will be saved
            only after calling `post`
        :param indexed_props:
            property values got from index to populate the cache
        :param kwargs:
            optional key arguments with new property values; specifing these
            arguments will mean the same as setting properties after `Document`
            object creation

        """
        enforce(self._initated)

        self._is_new = False
        self._cache = {}
        self._record = None

        if guid:
            self._guid = guid
            if not indexed_props:
                indexed_props = self._index.get_cached(guid)
            for prop_name, value in (indexed_props or {}).items():
                self._cache[prop_name] = (value, None)
            self.authorize_document(env.ACCESS_READ, self)
        else:
            self._is_new = True

            cache = {}
            self.on_create(kwargs, cache)
            for name, value in cache.items():
                self._cache[name] = (None, value)
            self._guid = cache['guid']

            for name, prop in self.metadata.items():
                if isinstance(prop, StoredProperty):
                    if name in kwargs or name in self._cache:
                        continue
                    enforce(prop.default is not None,
                            _('Property "%s" should be passed for ' \
                                    'new "%s" document'),
                            name, self.metadata.name)
                if prop.default is not None:
                    default = prop.default
                    if prop.converter is not None:
                        default = prop.converter(self, default)
                    self._cache[name] = (None, default)

        for prop_name, value in kwargs.items():
            self[prop_name] = value

    @property
    def guid(self):
        """Document GUID."""
        return self._guid

    def get(self, prop_name, raw=False):
        """Get document's property value.

        :param prop_name:
            property name to get value
        :param raw:
            if `True`, avoid any checks for users' visible properties;
            only for server local use
        :returns:
            `prop_name` value

        """
        prop = self.metadata[prop_name]

        if not raw:
            self.assert_access(env.ACCESS_READ, prop)

        orig, new = self._cache.get(prop_name, (None, None))
        if new is not None:
            return new
        if orig is not None:
            return orig

        if isinstance(prop, StoredProperty):
            if self._record is None:
                self._record = self._storage.get(self.guid)
            orig = self._record.get(prop_name)
        elif isinstance(prop, AggregatorProperty):
            orig = self._storage.is_aggregated(
                    self.guid, prop_name, prop.value)
        else:
            raise RuntimeError(_('Property "%s" in "%s" cannot be get') % \
                    (prop_name, self.metadata.name))

        self._cache[prop_name] = (orig, new)
        return orig

    def set(self, prop_name, value, raw=False):
        """set document's property value.

        :param prop_name:
            property name to set
        :param raw:
            if `True`, avoid any checks for users' visible properties;
            only for server local use
        :param value:
            property value to set

        """
        if prop_name == 'guid':
            enforce(self._is_new, _('GUID can be set only for new documents'))

        prop = self.metadata[prop_name]

        if not raw:
            if self._is_new:
                self.assert_access(env.ACCESS_CREATE, prop)
            else:
                self.assert_access(env.ACCESS_WRITE, prop)

        enforce(isinstance(prop, StoredProperty) or \
                isinstance(prop, AggregatorProperty),
                _('Property "%s" in "%s" cannot be set'),
                prop_name, self.metadata.name)

        if prop.converter is not None:
            value = prop.converter(self, value)
        orig, __ = self._cache.get(prop_name, (None, None))
        self._cache[prop_name] = (orig, value)

        if prop_name == 'guid':
            self._guid = value

    def post(self):
        changes = {}
        for prop_name, (__, new) in self._cache.items():
            if new is not None:
                changes[prop_name] = new
        if not changes:
            return

        if self._is_new:
            self.authorize_document(env.ACCESS_CREATE, self)
        else:
            self.authorize_document(env.ACCESS_WRITE, self)
            self.on_modify(changes)
        self.on_post(changes)

        for prop_name, value in changes.items():
            prop = self.metadata[prop_name]
            try:
                changes[prop_name] = prop.convert(value)
            except Exception:
                error = _('Value %r for "%s" property for "%s" is invalid') % \
                        (value, prop_name, self.metadata.name)
                util.exception(error)
                raise RuntimeError(error)

        if self._is_new:
            _logger.debug('Create new document "%s"', self.guid)

        self._index.store(self.guid, changes, self._is_new,
                self._pre_store, self._post_store)
        self._is_new = False

    def get_blob(self, prop_name, raw=False):
        """Read the content of document's BLOB property.

        This function works in parallel to getting non-BLOB properties values.

        :param prop_name:
            BLOB property name
        :param raw:
            if `True`, avoid any checks for users' visible properties;
            only for server local use
        :returns:
            file-like object or `None`

        """
        prop = self.metadata[prop_name]
        if not raw:
            self.assert_access(env.ACCESS_READ, prop)
        enforce(isinstance(prop, BlobProperty),
                _('Property "%s" in "%s" is not a BLOB'),
                prop_name, self.metadata.name)
        return self._storage.get_blob(self.guid, prop_name)

    def set_blob(self, prop_name, stream, size=None, raw=False):
        """Receive BLOB property from a stream.

        This function works in parallel to setting non-BLOB properties values
        and `post()` function.

        :param prop_name:
            BLOB property name
        :param stream:
            stream to receive property value from
        :param size:
            read only specified number of bytes; otherwise, read until the EOF
        :param raw:
            if `True`, avoid any checks for users' visible properties;
            only for server local use

        """
        prop = self.metadata[prop_name]
        if not raw:
            self.assert_access(env.ACCESS_WRITE, prop)
        enforce(isinstance(prop, BlobProperty),
                _('Property "%s" in "%s" is not a BLOB'),
                prop_name, self.metadata.name)
        seqno = self._storage.set_blob(self.guid, prop_name, stream, size)
        if seqno:
            self._index.store(self.guid, {'seqno': seqno}, None,
                    self._pre_store, self._post_store)

    def stat_blob(self, prop_name, raw=False):
        """Receive BLOB property information.

        :param prop_name:
            BLOB property name
        :param raw:
            if `True`, avoid any checks for users' visible properties;
            only for server local use
        :returns:
            a dictionary of `size`, `sha1sum` keys

        """
        prop = self.metadata[prop_name]
        if not raw:
            self.assert_access(env.ACCESS_READ, prop)
        enforce(isinstance(prop, BlobProperty),
                _('Property "%s" in "%s" is not a BLOB'),
                prop_name, self.metadata.name)
        return self._storage.stat_blob(self.guid, prop_name)

    def on_create(self, properties, cache):
        """Call back to call on document creation.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with new document properties values
        :param cache:
            properties to use as predefined values

        """
        cache['guid'] = env.uuid()
        ts = int(time.time())
        cache['ctime'] = ts
        cache['mtime'] = ts

    def on_modify(self, properties):
        """Call back to call on existing document modification.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        ts = int(time.time())
        properties['mtime'] = ts

    def on_post(self, properties):
        """Call back to call on exery `post()` call.

        Function needs to be re-implemented in child classes.

        :param properties:
            dictionary with document properties updates

        """
        pass

    def assert_access(self, mode, prop):
        """Does caller have permissions to access to the specified property.

        If caller does not have permissions, function should raise
        `active_document.Forbidden` exception.

        :param mode:
            one of `active_document.ACCESS_*` constants
            to specify the access mode
        :param prop:
            property to check access for

        """
        enforce(mode & prop.permissions, env.Forbidden,
                _('%s access is disabled for "%s" property in "%s"'),
                env.ACCESS_NAMES[mode], prop.name, self.metadata.name)

    def __getitem__(self, prop_name):
        return self.get(prop_name)

    def __setitem__(self, prop_name, value):
        self.set(prop_name, value)
