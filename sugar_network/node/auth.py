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
from ConfigParser import ConfigParser
from os.path import join, exists

from sugar_network import node
from sugar_network.toolkit import http, enforce


_config_mtime = 0
_config = None


def validate(request, role):
    enforce(_validate(request, role), http.Forbidden,
            'No enough permissions to proceed the operation')


def try_validate(request, role):
    return _validate(request, role) or False


def reset():
    global _config_mtime
    _config_mtime = 0


def _validate(request, role):
    global _config_mtime, _config

    if role == 'user':
        if request.principal:
            return True

    config_path = join(node.data_root.value, 'authorization.conf')
    if exists(config_path):
        mtime = os.stat(config_path).st_mtime
        if mtime > _config_mtime:
            _config_mtime = mtime
            _config = ConfigParser()
            _config.read(config_path)
    if _config is None:
        return

    user = request.principal or 'anonymous'
    if not _config.has_section(user):
        user = 'DEFAULT'

    if _config.has_option(user, role):
        return _config.get(user, role).strip().lower() in \
                ('true', 'on', '1', 'allow')
