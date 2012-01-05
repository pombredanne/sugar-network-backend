# Copyright (C) 2012, Aleksey Lim
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
import shutil
import logging
from os.path import exists, join, isdir, dirname
from gettext import gettext as _

from active_document import util, env
from active_document.metadata import IndexedProperty, StoredProperty
from active_document.util import enforce


_PAGE_SIZE = 4096
_DOCUMENT_STAMP = '.document'


class Storage(object):
    """Get access to documents' data storage."""

    def __init__(self, metadata):
        """
        :param name:
            document name

        """
        self.metadata = metadata
        self._root = env.path(metadata.name, '')

    def get(self, guid):
        """Get access to particular document's properties.

        :param guid:
            document GUID to get access to
        :returns:
            `Record` object

        """
        path = self._path(guid)
        enforce(exists(path), env.NotFound,
                _('Cannot find "%s" document in %s'),
                guid, self.metadata.name)
        return Record(path, guid, self.metadata)

    def put(self, guid, properties):
        """Write document's properties to the storage.

        :param guid:
            document to write properties for
        :param properties:
            a dictionary of (name, value) tupes of properties;
            not necessary all properties

        """
        try:
            root = self._ensure_path(guid, '')
            for name, value in properties.items():
                f = util.new_file(join(root, name))
                f.write(value)
                f.close()
            logging.debug('Put %r to "%s" document in %s',
                    properties, guid, self.metadata.name)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot put "%s" document to %s: %s') % \
                    (guid, self.metadata.name, error))

    def delete(self, guid):
        """Remove document properties from the storage.

        :param guid:
            document to remove

        """
        path = self._path(guid)
        if not exists(path):
            return
        try:
            shutil.rmtree(path)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot delete "%s" document from %s: %s') % \
                    (guid, self.metadata.name, error))
        logging.debug('Delete "%s" document from %s', guid, self.metadata.name)

    def walk(self):
        """Generator function to enumerate all existing documents.

        :returns:
            generator returns (guid, properties) typle for all found
            documents;
            the properties dictionary will contain only properties
            or `StoredProperty` and `IndexedProperty` clasess

        """
        logging.debug('Start walking %s', self.metadata.name)

        for guids_dirname in os.listdir(self._root):
            guids_dir = join(self._root, guids_dirname)
            if not isdir(guids_dir):
                continue

            for guid in os.listdir(guids_dir):
                if not exists(join(guids_dir, guid, _DOCUMENT_STAMP)):
                    continue

                properties = {}
                for name, prop in self.metadata.items():
                    if not isinstance(prop, StoredProperty) or \
                            not isinstance(prop, IndexedProperty):
                        continue
                    path = join(guids_dir, guid, name)
                    if exists(path):
                        f = file(path)
                        properties[name] = f.read()
                        f.close()

                yield guid, properties

    def send(self, guid, name, stream):
        """Read the content of document's BLOB property.

        :param guid:
            document GUID to send
        :param name:
            BLOB property name
        :param stream:
            stream to write BLOB property content
        :returns:
            the length of read BLOB property

        """
        path = self._path(guid, name)
        enforce(exists(path), env.NotFound,
                _('Cannot find "%s" property of "%s" document in %s'),
                name, guid, self.metadata.name)
        size = 0
        try:
            f = file(path)
            while True:
                chunk = f.read(_PAGE_SIZE)
                if len(chunk) == 0:
                    break
                stream.write(chunk)
                size += len(chunk)
            f.close()
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot send BLOB "%s" property ' \
                    'of "%s" in %s: %s') % \
                    (name, guid, self.metadata.name, error))
        logging.debug('Sent "%s" BLOB property from "%s" document in %s',
                name, guid, self.metadata.name)
        return size

    def receive(self, guid, name, stream):
        """Write the content of document's BLOB property.

        :param guid:
            document's GUID to receive
        :param name:
            BLOB property name
        :param stream:
            stream to read BLOB property content from
        :returns:
            the length of write to BLOB property

        """
        size = 0
        try:
            f = util.new_file(self._ensure_path(guid, name))
            while True:
                chunk = stream.read(_PAGE_SIZE)
                if len(chunk) == 0:
                    break
                f.write(chunk)
                size += len(chunk)
            f.close()
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot receive BLOB "%s" property ' \
                    'of "%s" in %s: %s') % \
                    (name, guid, self.metadata.name, error))
        logging.debug('Received "%s" BLOB property from "%s" document in %s',
                name, guid, self.metadata.name)
        return size

    def is_aggregated(self, guid, name, value):
        """Check if specified `value` is aggregated to the `name` property.

        :param guid:
            document's GUID to check
        :param name:
            aggregated property name
        :param value:
            value to check for aggregation
        :returns:
            `True` if `value` is aggregated

        """
        return exists(self._path(guid, name, str(value)))

    def aggregate(self, guid, name, value):
        """Append specified `value` to `name` property.

        :param guid:
            document's GUID to aggregate to
        :param name:
            aggregated property name
        :param value:
            value to aggregate

        """
        try:
            _touch(self._ensure_path(guid, name, ''), str(value))
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot aggregate "%s" for "%s" ' \
                    'property of "%s" in %s: %s') % \
                    (value, name, guid, self.metadata.name, error))
        logging.debug('Aggregated %r to "%s" of "%s" document in %s',
                value, name, guid, self.metadata.name)

    def disaggregate(self, guid, name, value):
        """Remove specified `value` to `name` property.

        :param guid:
            document's GUID to remove from
        :param name:
            aggregated property name
        :param value:
            value to remove

        """
        try:
            path = join(self._ensure_path(guid, name, ''), str(value))
            if exists(path):
                os.unlink(path)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot disaggregate absent "%s" for "%s" ' \
                    'property of "%s" in %s: %s') % \
                    (value, name, guid, self.metadata.name, error))
        logging.debug('Disagregated %r from "%s" of "%s" document in %s',
                value, name, guid, self.metadata.name)

    def count_aggregated(self, guid, name):
        """Count number of entities aggregated to `name` property.

        :param guid:
            document's GUID to count in
        :param name:
            aggregated property name
        :returns:
            integer value with number of aggregated entities

        """
        result = 0
        try:
            result = len(os.listdir(self._path(guid, name)))
        except Exception:
            pass
        logging.debug('There are %s entities in "%s" aggregated property ' \
                'of "%s" document in %s',
                result, name, guid, self.metadata.name)
        return result

    def _path(self, guid, *args):
        return join(self._root, guid[:2], guid, *args)

    def _ensure_path(self, guid, *args):
        path = self._path(guid)
        if not exists(path):
            os.makedirs(path)
            _touch(path, _DOCUMENT_STAMP)

        path = join(path, *args)
        if not exists(path):
            dir_path = path if path.endswith(os.sep) else dirname(path)
            if not exists(dir_path):
                os.makedirs(dir_path)

        return path


class Record(object):
    """Interface to document data."""

    def __init__(self, root, guid, metadata):
        self._root = root
        self._guid = guid
        self.metadata = metadata

    def get(self, name):
        path = join(self._root, name)
        enforce(exists(path), _('Cannot find "%s" property of "%s" in %s'),
                name, self._guid, self.metadata.name)
        return file(path).read()


def _touch(path, *args):
    file(join(path, *args), 'w').close()
