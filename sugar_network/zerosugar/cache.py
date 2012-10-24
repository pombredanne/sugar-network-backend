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
import logging
from os.path import exists, join

from sugar_network import toolkit, Client
from sugar_network.client import local_root
from sugar_network.zerosugar.bundle import Bundle
from sugar_network.toolkit import pipe
from active_toolkit.sockets import BUFFER_SIZE


_logger = logging.getLogger('zerosugar.cache')


def get(guid):
    path = join(local_root.value, 'cache', 'implementation', guid)
    if exists(path):
        return path

    _logger.debug('Fetch %r implementation', guid)
    # TODO Per download progress
    pipe.feedback('download')

    response = Client().request('GET', ['implementation', guid, 'data'],
            allow_redirects=True)
    content_length = int(response.headers.get('Content-Length', '0'))

    with toolkit.NamedTemporaryFile() as tmp_file:
        chunk_size = min(content_length, BUFFER_SIZE)
        # pylint: disable-msg=E1103
        for chunk in response.iter_content(chunk_size=chunk_size):
            tmp_file.write(chunk)
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
