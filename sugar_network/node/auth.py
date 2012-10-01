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

from ConfigParser import ConfigParser
from os.path import join, exists

import active_document as ad
from sugar_network import node
from active_toolkit import enforce


_config = None


def validate(request, role):
    enforce(_validate(request, role), ad.Forbidden,
            'No enough permissions to proceed the operation')


def try_validate(request, role):
    return _validate(request, role) or False


def _validate(request, role):
    global _config

    if role == 'user':
        if request.principal:
            return True
        else:
            request.principal = 'anonymous'

    if _config is None:
        _config = ConfigParser()
        config_path = join(node.data_root.value, 'authorization.conf')
        if exists(config_path):
            _config.read(config_path)

    if _config.has_option(request.principal, role):
        return _config.get(request.principal, role).strip().lower() in \
                ('true', 'on', '1', 'allow')
