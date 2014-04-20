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

import time
import hashlib
import logging
from ConfigParser import ConfigParser
from os.path import join, exists

from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import pylru, http, enforce


_SIGNATURE_LIFETIME = 600
_AUTH_POOL_SIZE = 1024

_logger = logging.getLogger('node.auth')


class Unauthorized(http.Unauthorized):

    def __init__(self, message, nonce=None):
        http.Unauthorized.__init__(self, message)
        if not nonce:
            nonce = int(time.time()) + _SIGNATURE_LIFETIME
        self.headers = {'www-authenticate': 'Sugar nonce="%s"' % nonce}


class Principal(str):

    admin = False
    editor = False
    translator = False

    _backup = None

    def __enter__(self):
        self._backup = (self.admin, self.editor, self.translator)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.admin, self.editor, self.translator = self._backup
        self._backup = None


class SugarAuth(object):

    def __init__(self, root):
        self._config_path = join(root, 'etc', 'authorization.conf')
        self._pool = pylru.lrucache(_AUTH_POOL_SIZE)
        self._config = None

    def reload(self):
        self._config = ConfigParser()
        if exists(self._config_path):
            self._config.read(self._config_path)
        self._pool.clear()

    def logon(self, request):
        auth = request.environ.get('HTTP_AUTHORIZATION')
        enforce(auth, Unauthorized, 'No credentials')

        if self._config is None:
            self.reload()

        from M2Crypto import RSA, BIO
        from urllib2 import parse_http_list, parse_keqv_list

        if auth in self._pool:
            login, nonce = self._pool[auth]
        else:
            scheme, creds = auth.strip().split(' ', 1)
            enforce(scheme.lower() == 'sugar', http.BadRequest,
                    'Unsupported authentication scheme')
            creds = parse_keqv_list(parse_http_list(creds))
            login = creds['username']
            signature = creds['signature']
            nonce = int(creds['nonce'])
            user = this.volume['user'][login]
            enforce(user.available, Unauthorized, 'Principal does not exist')
            key = RSA.load_pub_key_bio(BIO.MemoryBuffer(str(user['pubkey'])))
            data = hashlib.sha1('%s:%s' % (login, nonce)).digest()
            enforce(key.verify(data, signature.decode('hex')),
                    http.Forbidden, 'Bad credentials')
            self._pool[auth] = (login, nonce)

        enforce(abs(time.time() - nonce) <= _SIGNATURE_LIFETIME,
                Unauthorized, 'Credentials expired')
        principal = Principal(login)

        user = principal
        if not self._config.has_option('permissions', user):
            user = 'default'
            if not self._config.has_option('permissions', user):
                user = None
        if user:
            for role in self._config.get('permissions', user).split():
                role = role.lower()
                if role == 'admin':
                    principal.admin = True
                elif role == 'editor':
                    principal.editor = True
                elif role == 'translator':
                    principal.translator = True

        return principal
