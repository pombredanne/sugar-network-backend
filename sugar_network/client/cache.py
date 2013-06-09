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
import shutil
from os.path import exists, join

from sugar_network.client import IPCClient, local_root
from sugar_network.toolkit.bundle import Bundle
from sugar_network.toolkit import pipe, util


def get(guid):
    path = join(local_root.value, 'cache', 'implementation', guid)
    if exists(path):
        pipe.trace('Reuse cached %s implementation from %r', guid, path)
        return path

    pipe.trace('Download %s implementation', guid)
    # TODO Per download progress
    pipe.feedback('download')

    with util.NamedTemporaryFile() as tmp_file:
        IPCClient().download(['implementation', guid, 'data'], tmp_file)
        tmp_file.flush()
        os.makedirs(path)
        try:
            with Bundle(tmp_file.name, 'application/zip') as bundle:
                bundle.extractall(path)
        except Exception:
            shutil.rmtree(path, ignore_errors=True)
            raise

    topdir = os.listdir(path)[-1:]
    if topdir:
        for exec_dir in ('bin', 'activity'):
            bin_path = join(path, topdir[0], exec_dir)
            if not exists(bin_path):
                continue
            for filename in os.listdir(bin_path):
                os.chmod(join(bin_path, filename), 0755)

    return path
