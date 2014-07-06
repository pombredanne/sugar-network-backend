# Copyright (C) 2014 Aleksey Lim
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

import urllib
import hashlib
import logging

from sugar_network.toolkit import http, coroutine


_AVATAR_SIZE = 48
_GRAVATAR = 'http://www.gravatar.com/avatar/%(user)s?%(params)s'


_logger = logging.getLogger('node.avatars')
_checking = object()


class Avatars(object):

    def __init__(self):
        _logger.info('Start serving Gravatar avatars')
        self._cache = {}
        self._http = http.Connection()

    def get(self, email, default):
        uid = hashlib.md5(email.strip().lower()).hexdigest()
        url = self._cache.get(uid)
        if url and url is not _checking:
            return url
        if url is False:
            return default
        self._cache[uid] = _checking
        coroutine.spawn(self._check, uid)
        return _GRAVATAR % {
                'user': uid + '.png',
                'params': urllib.urlencode({
                    'd': str(default),
                    's': str(_AVATAR_SIZE),
                    }),
                }

    def _check(self, uid):
        _logger.debug('Checking for %s avatar', uid)
        try:
            self._http.request('GET', _GRAVATAR % {
                'user': uid,
                'params': 'd=404',
                })
        except http.NotFound:
            self._cache[uid] = False
        else:
            self._cache[uid] = _GRAVATAR % {
                    'user': uid + '.png',
                    'params': 's=%d' % _AVATAR_SIZE,
                    }
