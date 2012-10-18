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

import time
import uuid
import random
import hashlib
import logging


_logger = logging.getLogger('local.journal')


def create_activity_id():
    data = '%s%s%s' % (
            time.time(),
            random.randint(10000, 100000),
            uuid.getnode())
    return hashlib.sha1(data).hexdigest()


def exists(guid):
    return False


def get(guid, prop):
    return ''


def update(guid, props, preview, data):
    pass


class Commands(object):
    pass
