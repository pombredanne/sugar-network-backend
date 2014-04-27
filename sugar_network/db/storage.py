# Copyright (C) 2012-2014 Aleksey Lim
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


class Storage(object):
    """Get access to documents' data storage."""

    def __init__(self, root):
        self._root = root

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
        shutil.rmtree(path)

    def walk(self, mtime):
        """Generator function to enumerate all existing documents.

        :param mtime:
            return entities that were modified after `mtime`
        :returns:
            generator returns (guid, properties) tuple for all found
            documents

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

    def invalidate(self):
        guid_path = join(self._root, 'guid')
        if exists(guid_path):
            os.unlink(guid_path)

    def get(self, prop):
        path = join(self._root, prop)
        if not exists(path):
            return None
        with file(path) as f:
            meta = json.load(f)
        meta['mtime'] = int(os.stat(path).st_mtime)
        return meta

    def set(self, prop, mtime=None, **meta):
        if not exists(self._root):
            os.makedirs(self._root)
        meta_path = join(self._root, prop)

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

    def unset(self, prop):
        meta_path = join(self._root, prop)
        if exists(meta_path):
            os.unlink(meta_path)
