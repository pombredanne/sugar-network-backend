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
import tarfile
import zipfile
from os.path import join

from sugar_network.client.spec import Spec


class BundleError(Exception):
    pass


class Bundle(object):

    def __init__(self, bundle, mime_type=None):
        self._extract = False

        if mime_type is None:
            mime_type = _detect_mime_type(bundle) or ''

        if mime_type == 'application/zip':
            self._bundle = zipfile.ZipFile(bundle)
            self._do_get_names = self._bundle.namelist
            self._do_extractfile = self._bundle.open
        elif mime_type.split('/')[-1].endswith('-tar'):
            self._bundle = tarfile.open(bundle)
            self._do_get_names = self._bundle.getnames
            self._do_extractfile = self._bundle.extractfile
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

    def extractfileto(self, name, dst_path):
        f = file(dst_path, 'w')
        f.write(self._do_extractfile(name).read())
        f.close()

    def extractfile(self, name):
        return self._do_extractfile(name)

    def extractall(self, path, members=None):
        self._bundle.extractall(path=path, members=members)

    @property
    def extract(self):
        if self._extract is not False:
            return self._extract
        self._extract = None

        for arcname in self.get_names():
            parts = arcname.split(os.sep)
            if len(parts) > 1:
                if self._extract is None:
                    self._extract = parts[0]
                elif parts[0] != self._extract:
                    self._extract = None
                    break

        return self._extract

    def get_spec(self):
        if self.extract:
            specs = (join(self.extract, 'sweets.recipe'),
                     join(self.extract, 'activity', 'activity.info'))
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
