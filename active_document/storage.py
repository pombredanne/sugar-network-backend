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
import stat
import json
import shutil
import hashlib
import logging
from glob import glob
from os.path import exists, join, isdir, dirname, basename
from gettext import gettext as _

from active_document import util, env
from active_document.metadata import StoredProperty, CounterProperty
from active_document.metadata import AggregatorProperty, BlobProperty
from active_document.util import enforce


_PAGE_SIZE = 4096
_SEQNO_SUFFIX = '.seqno'
_AGGREGATE_SUFFIX = '.value'

_logger = logging.getLogger('ad.storage')


class Storage(object):
    """Get access to documents' data storage."""

    def __init__(self, metadata):
        """
        :param name:
            document name

        """
        self.metadata = metadata

    def get(self, guid):
        """Get access to particular document's properties.

        :param guid:
            document GUID to get access to
        :returns:
            `Record` object

        """
        path = self._path(guid)
        enforce(exists(path), env.NotFound,
                _('Cannot find "%s" document in "%s"'),
                guid, self.metadata.name)
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
            seqno = properties.get('seqno')
            if seqno is None:
                seqno = self.metadata.next_seqno()
            for name, value in properties.items():
                self._write_property(guid, name, value, seqno)
            self._write_property(guid, 'seqno', seqno)
            _logger.debug('Put %r to "%s" document in "%s"',
                    properties, guid, self.metadata.name)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot put "%s" document to "%s": %s') % \
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
            raise RuntimeError(
                    _('Cannot delete "%s" document from "%s": %s') % \
                    (guid, self.metadata.name, error))
        _logger.debug('Delete "%s" document from "%s"',
                guid, self.metadata.name)

    def walk(self, mtime):
        """Generator function to enumerate all existing documents.

        :param mtime:
            return entities that were modified after `mtime`
        :returns:
            generator returns (guid, properties) typle for all found
            documents; the properties dictionary will contain only
            `StoredProperty` properties

        """
        root = self.metadata.path()
        if not exists(root):
            return

        for guids_dirname in os.listdir(root):
            guids_dir = join(root, guids_dirname)
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

    def get_blob(self, guid, name):
        """Read the content of document's BLOB property.

        :param guid:
            document GUID to send
        :param name:
            BLOB property name
        :returns:
            generator that returns data by portions

        """
        path = self._path(guid, name)
        if not exists(path):
            return

        try:
            with file(path) as f:
                while True:
                    chunk = f.read(_PAGE_SIZE)
                    if len(chunk) == 0:
                        break
                    try:
                        yield chunk
                    except GeneratorExit:
                        break
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot read BLOB "%s" property ' \
                    'of "%s" in "%s": %s') % \
                    (name, guid, self.metadata.name, error))
        _logger.debug('Sent "%s" BLOB property from "%s" document in "%s"',
                name, guid, self.metadata.name)

    def set_blob(self, guid, name, stream, size=None):
        """Write the content of document's BLOB property.

        :param guid:
            document's GUID to receive
        :param name:
            BLOB property name
        :param stream:
            stream to read BLOB property content from
        :param size:
            read only specified number of bytes; otherwise, read until the EOF
        :returns:
            written seqno; `None` if document was not created before

        """
        seqno = self.metadata.next_seqno()
        self._set_blob(guid, name, stream, size, seqno)
        self._write_property(guid, 'seqno', seqno)
        if _is_document(self._path(guid)):
            return seqno

    def stat_blob(self, guid, name):
        path = self._path(guid, name)
        if not exists(path):
            return
        with file(path + '.sha1') as f:
            sha1sum = f.read().strip()
        return {'size': os.stat(path).st_size,
                'sha1sum': sha1sum,
                }

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
        path = self._path(guid, name + '.' + str(value) + _AGGREGATE_SUFFIX)
        return _aggregated(path)

    def aggregate(self, guid, name, value):
        """Append specified `value` to `name` property.

        :param guid:
            document's GUID to aggregate to
        :param name:
            aggregated property name
        :param value:
            value to aggregate

        """
        seqno = self.metadata.next_seqno()
        self._set_aggregate(guid, name, value, True, seqno, time.time())
        self._write_property(guid, 'seqno', seqno)

    def disaggregate(self, guid, name, value):
        """Remove specified `value` to `name` property.

        :param guid:
            document's GUID to remove from
        :param name:
            aggregated property name
        :param value:
            value to remove

        """
        seqno = self.metadata.next_seqno()
        self._set_aggregate(guid, name, value, False, seqno, time.time())
        self._write_property(guid, 'seqno', seqno)

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
        for i in glob(self._path(guid, name + '.*' + _AGGREGATE_SUFFIX)):
            if _aggregated(i):
                result += 1
        _logger.debug('There are %s entities in "%s" aggregated property ' \
                'of "%s" document in "%s"',
                result, name, guid, self.metadata.name)
        return result

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
            if isinstance(prop, AggregatorProperty):
                values = []
                for i in glob(path + '.*' + _AGGREGATE_SUFFIX):
                    value = basename(i).split('.')[1]
                    seqno_stat = os.stat(path + '.' + value + _SEQNO_SUFFIX)
                    if int(seqno_stat.st_mtime) in accept_range:
                        values.append(
                                ((value, _aggregated(i)), os.stat(i).st_mtime))
                if values:
                    traits[name] = values
            elif exists(path) and not isinstance(prop, CounterProperty):
                if int(os.stat(path + _SEQNO_SUFFIX).st_mtime) in accept_range:
                    stat_ = os.stat(path)
                    if isinstance(prop, BlobProperty):
                        blobs[name] = (path, stat_.st_mtime)
                    else:
                        traits[name] = (_read_property(path), stat_.st_mtime)

        return traits, blobs

    def merge(self, guid, diff, touch=True):
        """Apply changes for the document.

        :param diff:
            dictionary with changes in format that `diff()` returns;
            for BLOB properties, property value is a stream to read BLOB from
        :param touch:
            if `True`, increment seqno
        :returns:
            seqno value for applied `diff`;
            `None` if `diff` was not applied

        """
        applied = False
        if 'guid' in diff:
            enforce(guid == diff['guid'][0],
                    _('Malformed document diff, GUID is incorrect'))
            create_stamp = True
        else:
            create_stamp = False
        if touch:
            seqno = self.metadata.next_seqno()
        else:
            seqno = None

        for name in diff.keys():
            path = self._ensure_path(create_stamp, guid, name)

            prop = self.metadata[name]
            if isinstance(prop, AggregatorProperty):
                for (value, aggregated), ts in diff[name]:
                    agg_path = path + '.' + value + _AGGREGATE_SUFFIX
                    if not exists(agg_path) or os.stat(agg_path).st_mtime < ts:
                        self._set_aggregate(guid, name, value, aggregated,
                                seqno, ts)
                        applied = True
            else:
                value, ts = diff[name]
                if not exists(path) or os.stat(path).st_mtime < ts:
                    if isinstance(prop, BlobProperty):
                        self._set_blob(guid, name, value, None, seqno, ts)
                    else:
                        self._write_property(guid, name, value, seqno, ts)
                    applied = True

        if applied:
            if seqno:
                self._write_property(guid, 'seqno', seqno)
            else:
                seqno = Record(self._path(guid)).get('seqno')
            if _is_document(self._path(guid)):
                return seqno

    def _path(self, guid, *args):
        return self.metadata.path(guid[:2], guid, *args)

    def _ensure_path(self, create_stamp, guid, *args):
        path = self._path(guid)
        if not exists(path):
            os.makedirs(path)
        if create_stamp:
            stamt_path = join(path, _SEQNO_SUFFIX)
            if not exists(stamt_path):
                file(stamt_path, 'w').close()
                os.utime(stamt_path, (0, 0))

        path = join(path, *args)
        if not exists(path):
            dir_path = path if path.endswith(os.sep) else dirname(path)
            if not exists(dir_path):
                os.makedirs(dir_path)

        return path

    def _set_blob(self, guid, name, stream, size, seqno, mtime=None):
        if size is None:
            size = sys.maxint
        final_size = 0
        digest = hashlib.sha1()
        path = self._ensure_path(False, guid, name)

        try:
            with util.new_file(path) as f:
                while size > 0:
                    chunk = stream.read(min(size, _PAGE_SIZE))
                    if len(chunk) == 0:
                        break
                    f.write(chunk)
                    final_size += len(chunk)
                    size -= len(chunk)
                    digest.update(chunk)
            if mtime:
                os.utime(path, (mtime, mtime))
            with util.new_file(path + '.sha1') as f:
                f.write(digest.hexdigest())
            _touch_seqno(path, seqno)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot receive BLOB "%s" property ' \
                    'of "%s" in "%s": %s') % \
                    (name, guid, self.metadata.name, error))

        _logger.debug('Received "%s" BLOB property from "%s" document in "%s"',
                name, guid, self.metadata.name)
        return final_size

    def _set_aggregate(self, guid, name, value, aggregated, seqno, mtime=None):
        try:
            value = str(value)
            enforce(value.isalnum(), _('Aggregated value should be alnum'))
            path = self._ensure_path(False, guid, name + '.' + value)
            agg_path = path + _AGGREGATE_SUFFIX
            if not exists(agg_path):
                file(agg_path, 'w').close()
            if mtime:
                os.utime(agg_path, (mtime, mtime))
            _touch_seqno(path, seqno)
            mode = os.stat(agg_path).st_mode
            if aggregated:
                mode |= stat.S_ISVTX
            else:
                mode &= ~stat.S_ISVTX
            os.chmod(agg_path, mode)
        except Exception, error:
            util.exception()
            raise RuntimeError(_('Cannot change "%s" aggregatation for "%s" ' \
                    'property of "%s" in "%s": %s') % \
                    (value, name, guid, self.metadata.name, error))
        _logger.debug('Changed "%s" aggregattion from "%s" of "%s" document ' \
                'in "%s"', value, name, guid, self.metadata.name)

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
                    _('Cannot find "%s" property in "%s"'),
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


def _aggregated(path):
    return exists(path) and bool(os.stat(path).st_mode & stat.S_ISVTX)


def _is_document(path):
    return exists(join(path, _SEQNO_SUFFIX)) and exists(join(path, 'guid'))
