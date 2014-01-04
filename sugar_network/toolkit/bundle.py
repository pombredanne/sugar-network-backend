# Copyright (C) 2010-2012 Aleksey Lim
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
from os.path import join, exists, dirname

from sugar_network.toolkit.spec import Spec


class BundleError(Exception):
    pass


class Bundle(object):

    def __init__(self, bundle, mime_type=None):
        self._rootdir = False

        if mime_type is None:
            mime_type = _detect_mime_type(bundle) or ''

        if mime_type == 'application/zip':
            import zipfile
            self._bundle = zipfile.ZipFile(bundle)
            self._do_get_names = self._bundle.namelist
            self._do_extractfile = self._bundle.open
            self._do_extract = self._bundle.extract
            self._do_getmember = self._bundle.getinfo
            self._cast_info = _ZipInfo
        elif mime_type.split('/')[-1].endswith('-tar'):
            import tarfile
            self._bundle = tarfile.open(bundle)
            self._do_get_names = self._bundle.getnames
            self._do_extractfile = self._bundle.extractfile
            self._do_extract = self._bundle.extract
            self._do_getmember = self._bundle.getmember
            self._cast_info = lambda x: x
        else:
            raise BundleError('Unsupported bundle type for "%s" file, '
                    'it can be either tar or zip.' % bundle)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._bundle.close()
        self._bundle = None

    def get_names(self):
        return self._do_get_names()

    def extract(self, name, dst_path):
        return self._do_extract(name, dst_path)

    def extractfile(self, name):
        return _File(self._do_extractfile(name))

    def extractall(self, dst_root, members=None, prefix=None):
        if not prefix:
            self._bundle.extractall(path=dst_root, members=members)
            return
        try:
            prefix = prefix.strip(os.sep) + os.sep
            for arcname in self.get_names():
                dst_path = arcname.lstrip(os.sep)
                if dst_path.startswith(prefix):
                    dst_path = dst_path[len(prefix):]
                dst_path = join(dst_root, dst_path)
                if dst_path.endswith(os.sep):
                    os.makedirs(dst_path)
                else:
                    if not exists(dirname(dst_path)):
                        os.makedirs(dirname(dst_path))
                    with file(dst_path, 'wb') as dst:
                        with self.extractfile(arcname) as src:
                            shutil.copyfileobj(src, dst)
        except Exception:
            if exists(dst_root):
                shutil.rmtree(dst_root)
            raise

    def getmember(self, name):
        return self._cast_info(self._do_getmember(name))

    @property
    def rootdir(self):
        if self._rootdir is not False:
            return self._rootdir
        self._rootdir = None

        for arcname in self.get_names():
            parts = arcname.split(os.sep)
            if len(parts) > 1:
                if self._rootdir is None:
                    self._rootdir = parts[0]
                elif parts[0] != self._rootdir:
                    self._rootdir = None
                    break

        return self._rootdir

    def get_spec(self):
        if self.rootdir:
            specs = (join(self.rootdir, 'sweets.recipe'),
                     join(self.rootdir, 'activity', 'activity.info'))
        else:
            specs = ('sweets.recipe', join('activity', 'activity.info'))

        for arcname in self.get_names():
            if arcname in specs:
                f = self.extractfile(arcname)
                try:
                    return Spec(f)
                finally:
                    f.close()


def _detect_mime_type(filename):
    if filename.endswith('.xo'):
        return 'application/zip'
    if filename.endswith('.zip'):
        return 'application/zip'
    if filename.endswith('.tar.bz2'):
        return 'application/x-bzip-compressed-tar'
    if filename.endswith('.tar.gz'):
        return 'application/x-compressed-tar'
    if filename.endswith('.tar.lzma'):
        return 'application/x-lzma-compressed-tar'
    if filename.endswith('.tar.xz'):
        return 'application/x-xz-compressed-tar'
    if filename.endswith('.tbz'):
        return 'application/x-bzip-compressed-tar'
    if filename.endswith('.tgz'):
        return 'application/x-compressed-tar'
    if filename.endswith('.tlz'):
        return 'application/x-lzma-compressed-tar'
    if filename.endswith('.txz'):
        return 'application/x-xz-compressed-tar'
    if filename.endswith('.tar'):
        return 'application/x-tar'


class _ZipInfo(object):

    def __init__(self, info):
        self._info = info

    @property
    def name(self):
        return self._info.filename

    @property
    def size(self):
        return self._info.file_size


class _File(object):

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def __getattr__(self, name):
        return getattr(self._fileobj, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._fileobj.close()
