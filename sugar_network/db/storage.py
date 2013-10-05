# Copyright (C) 2012-2013 Aleksey Lim
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
import time
import json
import shutil
from os.path import exists, join, isdir, basename

from sugar_network import toolkit
from sugar_network.toolkit.router import Blob


_BLOB_SUFFIX = '.blob'


class Storage(object):
    """Get access to documents' data storage."""

    def __init__(self, root, metadata):
        self._root = root
        self.metadata = metadata

    def get(self, guid):
        """Get access to particular document's properties.

        :param guid:
            document GUID to get access to
        :returns:
            `Record` object

        """
        return Record(self._path(guid))

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
            toolkit.exception()
            raise RuntimeError('Cannot delete %r document from %r: %s' %
                    (guid, self.metadata.name, error))

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
                path = join(guids_dir, guid, 'guid')
                if exists(path) and os.stat(path).st_mtime > mtime:
                    yield guid

    def migrate(self, guid):
        pass

    def _path(self, guid, *args):
        return join(self._root, guid[:2], guid, *args)


class Record(object):
    """Interface to document data."""

    def __init__(self, root):
        self._root = root

    @property
    def guid(self):
        return basename(self._root)

    @property
    def exists(self):
        return exists(self._root)

    @property
    def consistent(self):
        return exists(join(self._root, 'guid'))

    def path(self, *args):
        return join(self._root, *args)

    def blob_path(self, prop, *args):
        return join(self._root, prop + _BLOB_SUFFIX, *args)

    def invalidate(self):
        guid_path = join(self._root, 'guid')
        if exists(guid_path):
            os.unlink(guid_path)

    def get(self, prop):
        path = join(self._root, prop)
        if not exists(path):
            return None
        with file(path) as f:
            meta = Blob(json.load(f))
        blob_path = path + _BLOB_SUFFIX
        if exists(blob_path):
            meta['blob'] = blob_path
            if 'blob_size' not in meta:
                meta['blob_size'] = os.stat(blob_path).st_size
        meta['mtime'] = int(os.stat(path).st_mtime)
        return meta

    def set(self, prop, mtime=None, **meta):
        if not exists(self._root):
            os.makedirs(self._root)
        meta_path = join(self._root, prop)

        if 'blob' in meta:
            dst_blob_path = meta_path + _BLOB_SUFFIX
            blob = meta.pop('blob')
            if hasattr(blob, 'read'):
                with toolkit.new_file(dst_blob_path) as f:
                    shutil.copyfileobj(blob, f)
            elif blob is not None:
                os.rename(blob, dst_blob_path)
            elif exists(dst_blob_path):
                os.unlink(dst_blob_path)

        with toolkit.new_file(meta_path) as f:
            json.dump(meta, f)
        if mtime:
            os.utime(meta_path, (mtime, mtime))

        if prop == 'guid':
            if not mtime:
                mtime = time.time()
            # Touch directory to let it possible to crawl it on startup
            # when index was not previously closed properly
            os.utime(join(self._root, '..'), (mtime, mtime))
