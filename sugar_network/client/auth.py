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

import os
import hashlib
from base64 import b64encode
from urllib2 import parse_http_list, parse_keqv_list
from os.path import abspath, expanduser, dirname, exists


class BasicCreds(object):

    def __init__(self, login, password):
        self._login = login
        self._password = password

    @property
    def login(self):
        return self._login

    @property
    def profile(self):
        return None

    def logon(self, challenge):
        creds = '%s:%s' % (self._login, self._password)
        return {'authorization': 'Basic ' + b64encode(creds)}


class SugarCreds(object):

    def __init__(self, key_path):
        self._key_path = abspath(expanduser(key_path))
        self._key = None
        self._pubkey = None
        self._login = None

    @property
    def pubkey(self):
        if self._pubkey is None:
            self.ensure_key()
            from M2Crypto.BIO import MemoryBuffer
            buf = MemoryBuffer()
            self._key.save_pub_key_bio(buf)
            self._pubkey = buf.getvalue()
        return self._pubkey

    @property
    def login(self):
        if self._login is None:
            self._login = str(hashlib.sha1(self.pubkey).hexdigest())
        return self._login

    @property
    def profile(self):
        try:
            import gconf
            gconf_ = gconf.client_get_default()
            name = gconf_.get_string('/desktop/sugar/user/nick')
        except Exception:
            name = self.login
        return {'name': name, 'pubkey': self.pubkey}

    def logon(self, challenge):
        self.ensure_key()
        challenge = challenge.split(' ', 1)[-1]
        nonce = parse_keqv_list(parse_http_list(challenge)).get('nonce')
        data = hashlib.sha1('%s:%s' % (self.login, nonce)).digest()
        signature = self._key.sign(data).encode('hex')
        authorization = 'Sugar username="%s",nonce="%s",signature="%s"' % \
                (self.login, nonce, signature)
        return {'authorization': authorization}

    def ensure_key(self):
        from M2Crypto import RSA

        key_dir = dirname(self._key_path)
        if exists(self._key_path):
            if os.stat(key_dir).st_mode & 077:
                os.chmod(key_dir, 0700)
            self._key = RSA.load_key(self._key_path)
            return

        if not exists(key_dir):
            os.makedirs(key_dir)
        os.chmod(key_dir, 0700)

        _logger.info('Generate RSA private key at %r', self._key_path)
        self._key = RSA.gen_key(1024, 65537, lambda *args: None)
        self._key.save_key(self._key_path, cipher=None)
        os.chmod(self._key_path, 0600)

        pub_key_path = self._key_path + '.pub'
        with file(pub_key_path, 'w') as f:
            f.write('ssh-rsa %s %s@%s' % (
                b64encode('\x00\x00\x00\x07ssh-rsa%s%s' % self._key.pub()),
                self.login,
                os.uname()[1],
                ))
        _logger.info('Saved RSA public key at %r', pub_key_path)
