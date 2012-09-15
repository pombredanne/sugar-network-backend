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
from os.path import exists, join, isdir, basename, relpath, lexists, isabs

from active_document import env
from active_document.metadata import BlobProperty
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import util, enforce


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
                shutil.move(path, path + _BLOB_SUFFIX)
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

    def get(self, prop):
        path = join(self._root, prop)
        if exists(path):
            return Meta(path)

    def set(self, prop, mtime=None, **kwargs):
        if not exists(self._root):
            os.makedirs(self._root)
        path = join(self._root, prop)

        with util.new_file(path) as f:
            json.dump(kwargs, f)
        if mtime:
            os.utime(path, (mtime, mtime))

        if prop == 'guid':
            if not mtime:
                mtime = time.time()
            # Touch directory to let it possible to crawl it on startup
            # when index was not previously closed properly
            os.utime(join(self._root, '..'), (mtime, mtime))

    def set_blob(self, prop, data=None, size=None, **kwargs):
        if not exists(self._root):
            os.makedirs(self._root)
        path = join(self._root, prop + _BLOB_SUFFIX)

        if 'digest' not in kwargs:
            digest = hashlib.sha1()
        else:
            digest = None

        try:
            if data is None:
                digest = None
            elif hasattr(data, 'read'):
                if size is None:
                    size = sys.maxint
                self._set_blob_by_stream(digest, data, size, path)
            elif isabs(data) and exists(data):
                self._set_blob_by_path(digest, data, path)
            else:
                with util.new_file(path) as f:
                    f.write(data)
                digest.update(data)
        except Exception, error:
            util.exception()
            raise RuntimeError('Fail to set BLOB %r property for %r: %s' %
                    (prop, self.guid, error))

        if digest is not None:
            kwargs['digest'] = digest.hexdigest()

        self.set(prop, **kwargs)

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


class Meta(dict):

    def __init__(self, path_=None, **meta):
        if path_:
            with file(path_) as f:
                meta.update(json.load(f))
            if exists(path_ + _BLOB_SUFFIX):
                meta['path'] = path_ + _BLOB_SUFFIX
            meta['mtime'] = os.stat(path_).st_mtime
        dict.__init__(self, meta)

    def url(self, part=None):
        url = self.get('url')
        if url is None or isinstance(url, basestring):
            return url

        if part:
            file_meta = url.get(part)
            enforce(file_meta and 'url' in file_meta,
                    env.NotFound, 'No BLOB for %r', part)
            return file_meta['url']

        return sorted(url.values(),
                cmp=lambda x, y: cmp(x.get('order'), y.get('order')))
