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
import sys
import time
import json
import errno
import shutil
import hashlib
import logging
from os.path import exists, join, isdir, dirname, basename
from gettext import gettext as _

from active_document import env
from active_document.metadata import StoredProperty
from active_document.metadata import BlobProperty
from active_toolkit import util, enforce


_PAGE_SIZE = 4096
_SEQNO_SUFFIX = '.seqno'

_logger = logging.getLogger('active_document.storage')


class Storage(object):
    """Get access to documents' data storage."""

    def __init__(self, root, metadata):
        self._root = root
        self.metadata = metadata

    def exists(self, guid):
        """Does specified GUID exist."""
        return exists(self._path(guid))

    def get(self, guid):
        """Get access to particular document's properties.

        :param guid:
            document GUID to get access to
        :returns:
            `Record` object

        """
        path = self._path(guid)
        enforce(exists(path), env.NotFound,
                _('Cannot find %r document in %r'), guid, self.metadata.name)
        return Record(path)

    def put(self, guid, properties):
        """Write document's properties to the storage.

        :param guid:
            document to write properties for
        :param properties:
            a dictionary of (name, value) tupes of properties;
            not necessary all properties

        """
        try:
            self._ensure_path(True, guid, '')
            seqno = properties['seqno']
            for name, value in properties.items():
                self._write_property(guid, name, value, seqno)
            self._write_property(guid, 'seqno', seqno)
            _logger.debug('Put %r to %r document in %r',
                    properties, guid, self.metadata.name)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot put %r document to %r: %s') % \
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
            raise RuntimeError(_('Cannot delete %r document from %r: %s') % \
                    (guid, self.metadata.name, error))
        _logger.debug('Delete %r document from %r', guid, self.metadata.name)

    def walk(self, mtime):
        """Generator function to enumerate all existing documents.

        :param mtime:
            return entities that were modified after `mtime`
        :returns:
            generator returns (guid, properties) typle for all found
            documents; the properties dictionary will contain only
            `StoredProperty` properties

        """
        if not exists(self._root):
            return

        for guids_dirname in os.listdir(self._root):
            guids_dir = join(self._root, guids_dirname)
            if not isdir(guids_dir) or \
                    mtime and os.stat(guids_dir).st_mtime < mtime:
                continue

            for guid in os.listdir(guids_dir):
                guid_path = join(guids_dir, guid)
                if not _is_document(guid_path) or \
                        mtime and os.stat(guid_path).st_mtime < mtime:
                    continue

                properties = {
                        'seqno': Record(guid_path).get('seqno'),
                        }
                for name, prop in self.metadata.items():
                    if not isinstance(prop, StoredProperty):
                        continue
                    path = join(guid_path, name)
                    if exists(path):
                        properties[name] = _read_property(path)

                yield guid, properties

    def set_blob(self, seqno, guid, name, data, size=None):
        """Write the content of document's BLOB property.

        :param seqno:
            seqno to set BLOB for
        :param guid:
            document's GUID to receive
        :param name:
            BLOB property name
        :param data:
            stream to read BLOB content or path to file to copy
        :param size:
            read only specified number of bytes; otherwise, read until the EOF
        :returns:
            `True` if document existed before

        """
        self._set_blob(guid, name, data, size, seqno)
        return _is_document(self._path(guid))

    def stat_blob(self, guid, name):
        path = self._path(guid, name)
        if not exists(path):
            return None
        with file(path + '.sha1') as f:
            sha1sum = f.read().strip()
        return {'path': path,
                'size': os.stat(path).st_size,
                'sha1sum': sha1sum,
                'mime_type': self.metadata[name].mime_type,
                }

    def diff(self, guid, accept_range):
        """Return changed properties for specified times range.

        :param guid:
            document GUID to check changed properties for
        :param accept_range:
            sequence object with times to accept properties
        :returns:
            tuple of dictionaries for regular properties and BLOBs

        """
        traits = {}
        blobs = {}

        for name, prop in self.metadata.items():
            path = self._path(guid, name)
            if not exists(path):
                continue
            if int(os.stat(path + _SEQNO_SUFFIX).st_mtime) not in accept_range:
                continue
            stat_ = os.stat(path)
            if isinstance(prop, BlobProperty):
                blobs[name] = (path, stat_.st_mtime)
            else:
                traits[name] = (_read_property(path), stat_.st_mtime)

        return traits, blobs

    def merge(self, seqno, guid, diff):
        """Apply changes for the document.

        :param seqno:
            seqno to set merged data for
        :param diff:
            dictionary with changes in format that `diff()` returns;
            for BLOB properties, property value is a stream to read BLOB from
        :returns:
            `True` if `diff` was applied

        """
        applied = False
        if 'guid' in diff:
            enforce(guid == diff['guid'][0],
                    _('Malformed document diff, GUID is incorrect'))
            create_stamp = True
        else:
            create_stamp = False

        for name in diff.keys():
            path = self._ensure_path(create_stamp, guid, name)

            prop = self.metadata[name]
            value, ts = diff[name]
            if not exists(path) or os.stat(path).st_mtime < ts:
                if isinstance(prop, BlobProperty):
                    self._set_blob(guid, name, value, None, seqno, ts)
                else:
                    self._write_property(guid, name, value, seqno, ts)
                applied = True

        if applied and seqno:
            self._write_property(guid, 'seqno', seqno)

        return applied and _is_document(self._path(guid))

    def _path(self, guid, *args):
        return join(self._root, guid[:2], guid, *args)

    def _ensure_path(self, create_stamp, guid, *args):

        def mkdir(path):
            if exists(path):
                return
            try:
                os.makedirs(path)
            except OSError, error:
                if error.errno == errno.EEXIST:
                    # Possible race between index readers and writers
                    # processes. Index readers can access to storage
                    # directly to save BLOB properties.
                    pass
                else:
                    raise

        path = self._path(guid)
        mkdir(path)

        if create_stamp:
            stamt_path = join(path, _SEQNO_SUFFIX)
            if not exists(stamt_path):
                file(stamt_path, 'w').close()
                os.utime(stamt_path, (0, 0))

        path = join(path, *args)
        if not exists(path):
            dir_path = path if path.endswith(os.sep) else dirname(path)
            mkdir(dir_path)

        return path

    def _set_blob(self, guid, name, data, size, seqno, mtime=None):
        path = self._ensure_path(False, guid, name)
        digest = hashlib.sha1()

        def read_from_stream(stream, size):
            if size is None:
                size = sys.maxint
            with util.new_file(path) as f:
                while size > 0:
                    chunk = stream.read(min(size, _PAGE_SIZE))
                    if len(chunk) == 0:
                        break
                    f.write(chunk)
                    size -= len(chunk)
                    digest.update(chunk)

        try:
            if hasattr(data, 'read'):
                read_from_stream(data, size)
            else:
                util.cptree(data, path)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot receive BLOB %r property ' \
                    'of %r in %r: %s') % \
                    (name, guid, self.metadata.name, error))

        if mtime:
            os.utime(path, (mtime, mtime))
        with util.new_file(path + '.sha1') as f:
            f.write(digest.hexdigest())
        _touch_seqno(path, seqno)

        _logger.debug('Received %r BLOB property from %r document in %r',
                name, guid, self.metadata.name)

    def _write_property(self, guid, name, value, seqno=None, mtime=None):
        if name == 'seqno':
            _touch_seqno(self._path(guid) + os.sep, value)
            # Touch directory to let it possible to crawl this directory
            # on startup when index was not previously closed properly
            ts = time.time()
            path = self._path(guid)
            os.utime(path, (ts, ts))
            os.utime(join(path, '..'), (ts, ts))
        else:
            path = self._path(guid, name)
            with util.new_file(path) as f:
                json.dump(value, f)
            if mtime:
                os.utime(path, (mtime, mtime))
            _touch_seqno(path, seqno)


class Record(object):
    """Interface to document data."""

    def __init__(self, root):
        self._root = root

    def get(self, name, default=None):
        if name == 'seqno':
            return int(os.stat(join(self._root, _SEQNO_SUFFIX)).st_mtime)
        path = join(self._root, name)
        if not exists(path):
            enforce(default is not None,
                    _('Cannot find %r property in %r'),
                    name, basename(self._root))
            return default
        return _read_property(path)


def _touch_seqno(path, seqno):
    path += _SEQNO_SUFFIX
    if not exists(path):
        file(path, 'w').close()
        if not seqno:
            os.utime(path, (0, 0))
    if seqno:
        os.utime(path, (seqno, seqno))


def _read_property(path):
    with file(path) as f:
        return json.load(f)


def _is_document(path):
    return exists(join(path, _SEQNO_SUFFIX)) and exists(join(path, 'guid'))
