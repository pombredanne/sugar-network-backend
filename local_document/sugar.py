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
import time
import uuid
import random
import hashlib
from os.path import join, exists, dirname
from gettext import gettext as _

import gconf

from active_document import enforce


_XO_SERIAL_PATH = '/ofw/mfg-data/SN'
_XO_UUID_PATH = '/ofw/mfg-data/U#'
_NICKNAME_GCONF = '/desktop/sugar/user/nick'
_COLOR_GCONF = '/desktop/sugar/user/color'


def profile_path(*args):
    """Path within sugar profile directory.

    Missed directories will be created.

    :param args:
        path parts that will be added to the resulting path
    :returns:
        full path with directory part existed

    """
    if os.geteuid():
        root_dir = join(os.environ['HOME'], '.sugar',
                os.environ.get('SUGAR_PROFILE', 'default'))
    else:
        root_dir = '/var/sugar-client'
    result = join(root_dir, *args)
    if not exists(dirname(result)):
        os.makedirs(dirname(result))
    return result


def pubkey():
    pubkey_path = profile_path('owner.key.pub')
    enforce(exists(pubkey_path),
            _('Sugar session was never started, no pubkey'))

    with file(pubkey_path) as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('ssh-'):
                return line
    raise RuntimeError(_('Valid SSH public key was not found in %s') % \
            pubkey_path)


def uid():
    key = pubkey().split()[1]
    return str(hashlib.sha1(key).hexdigest())


def nickname():
    gconf_client = gconf.client_get_default()
    return gconf_client.get_string(_NICKNAME_GCONF)


def color():
    gconf_client = gconf.client_get_default()
    return gconf_client.get_string(_COLOR_GCONF)


def machine_sn():
    if exists(_XO_SERIAL_PATH):
        return _read_XO_value(_XO_SERIAL_PATH)


def machine_uuid():
    if exists(_XO_UUID_PATH):
        return _read_XO_value(_XO_UUID_PATH)


def uuid_new():
    data = '%s%s%s' % \
            (time.time(), random.randint(10000, 100000), uuid.getnode())
    return hashlib.sha1(data).hexdigest()


def _read_XO_value(path):
    return file(path).read().rstrip('\0\n')
