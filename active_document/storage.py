# Copyright (C) 2012 Aleksey Lim
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
import shutil
import hashlib
from base64 import b64decode
from os.path import exists, join, isdir, basename, relpath, lexists, isabs

from active_document import env
from active_document.metadata import PropertyMeta, BlobProperty
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import util


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
            util.exception()
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
        root = self._path(guid)
        record = self.get(guid)

        path = join(root, '.seqno')
        if exists(path):
            seqno = int(os.stat(path).st_mtime)
            with file(join(root, 'seqno'), 'w') as f:
                json.dump({'seqno': seqno, 'value': seqno}, f)
            os.unlink(path)

        for name, prop in self.metadata.items():
            path = join(root, name)
            if exists(path + '.seqno'):
                self._migrate_to_1(path, prop)
            elif not exists(path):
                if not isinstance(prop, BlobProperty):
                    record.set(name, seqno=0, value=prop.default)

    def _migrate_to_1(self, path, prop):
        meta = {'seqno': int(os.stat(path + '.seqno').st_mtime)}

        mtime = None
        if lexists(path):
            if exists(path):
                mtime = os.stat(path).st_mtime
            else:
                os.unlink(path)

        if isinstance(prop, BlobProperty):
            if mtime is not None:
                if exists(path + '.sha1'):
                    with file(path + '.sha1') as f:
                        meta['digest'] = f.read().strip()
                    os.unlink(path + '.sha1')
                else:
                    # TODO calculate new digest
                    meta['digest'] = ''
                shutil.move(path, path + PropertyMeta.BLOB_SUFFIX)
                meta['mime_type'] = prop.mime_type
            else:
                if exists(path + '.sha1'):
                    os.unlink(path + '.sha1')
                meta = None
        else:
            if mtime is not None:
                with file(path) as f:
                    value = json.load(f)
                if prop.localized and type(value) is not dict:
                    value = {env.DEFAULT_LANG: value}
            else:
                value = prop.default
            meta['value'] = value

        if meta is not None:
            with file(path, 'w') as f:
                json.dump(meta, f)
            if mtime is not None:
                os.utime(path, (mtime, mtime))

        os.unlink(path + '.seqno')

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

    def invalidate(self):
        guid_path = join(self._root, 'guid')
        if exists(guid_path):
            os.unlink(guid_path)

    def get(self, prop):
        path = join(self._root, prop)
        if exists(path):
            return PropertyMeta(path)

    def set(self, prop, mtime=None, path=None, content=None, **meta):
        if not exists(self._root):
            os.makedirs(self._root)
        meta_path = join(self._root, prop)

        blob_path = join(self._root, prop + PropertyMeta.BLOB_SUFFIX)
        if content is not None:
            with util.new_file(blob_path) as f:
                f.write(b64decode(content))
        elif path and exists(path):
            util.cptree(path, blob_path)

        with util.new_file(meta_path) as f:
            json.dump(meta, f)
        if mtime:
            os.utime(meta_path, (mtime, mtime))

        if prop == 'guid':
            if not mtime:
                mtime = time.time()
            # Touch directory to let it possible to crawl it on startup
            # when index was not previously closed properly
            os.utime(join(self._root, '..'), (mtime, mtime))

    def set_blob(self, prop, data=None, size=None, **kwargs):
        if not exists(self._root):
            os.makedirs(self._root)
        path = join(self._root, prop + PropertyMeta.BLOB_SUFFIX)
        meta = PropertyMeta(**kwargs)

        if data is None:
            if exists(path):
                os.unlink(path)
        elif isinstance(data, PropertyMeta):
            data.update(meta)
            meta = data
        else:
            digest = hashlib.sha1()
            if hasattr(data, 'read'):
                if size is None:
                    size = sys.maxint
                self._set_blob_by_stream(digest, data, size, path)
            elif isabs(data) and exists(data):
                self._set_blob_by_path(digest, data, path)
            else:
                with util.new_file(path) as f:
                    f.write(data)
                digest.update(data)
            meta['digest'] = digest.hexdigest()

        self.set(prop, **meta)

    def _set_blob_by_stream(self, digest, stream, size, path):
        with util.new_file(path) as f:
            while size > 0:
                chunk = stream.read(min(size, BUFFER_SIZE))
                if not chunk:
                    break
                f.write(chunk)
                size -= len(chunk)
                if digest is not None:
                    digest.update(chunk)

    def _set_blob_by_path(self, digest, src_path, dst_path):
        util.cptree(src_path, dst_path)

        def hash_file(path):
            with file(path) as f:
                while True:
                    chunk = f.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    if digest is not None:
                        digest.update(chunk)

        if isdir(dst_path):
            for root, __, files in os.walk(dst_path):
                for filename in files:
                    path = join(root, filename)
                    if digest is not None:
                        digest.update(relpath(path, dst_path))
                    hash_file(path)
        else:
            hash_file(dst_path)
