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

import hashlib
import logging
from os.path import exists
from gettext import gettext as _

from active_toolkit import util


_logger = logging.getLogger('crypto')


def ensure_dsa_pubkey(path):
    if not exists(path):
        _logger.info(_('Create DSA server key'))
        util.assert_call([
            '/usr/bin/ssh-keygen', '-q', '-t', 'dsa', '-f', path,
            '-C', '', '-N', ''])

    with file(path + '.pub') as f:
        for line in f:
            line = line.strip()
            if line.startswith('ssh-'):
                key = line.split()[1]
                return str(hashlib.sha1(key).hexdigest())

    raise RuntimeError(_('No valid DSA public key in %r') % path)
