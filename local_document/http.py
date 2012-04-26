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

import json
import logging
import hashlib
from gettext import gettext as _

import requests
import requests.async
from M2Crypto import DSA

from local_document import env, sugar


_logger = logging.getLogger('local_document.http')
_headers = {}


def request(method, path, data=None, headers=None, **kwargs):
    response = raw_request(method, path, data, headers, **kwargs)
    if response.headers.get('Content-Type') == 'application/json':
        return json.loads(response.content)
    else:
        return response


def raw_request(method, path, data=None, headers=None, **kwargs):
    path = '/'.join([i.strip('/') for i in [env.api_url.value] + path])

    if not _headers:
        uid = sugar.uid()
        _headers['sugar_user'] = uid
        _headers['sugar_user_signature'] = _sign(uid)
    if headers:
        headers.update(_headers)
    else:
        headers = _headers

    if data is not None and headers.get('Content-Type') == 'application/json':
        data = json.dumps(data)

    verify = True
    if env.no_check_certificate.value:
        verify = False
    elif env.certfile.value:
        verify = env.certfile.value

    while True:
        try:
            rs = requests.async.request(method, path, data=data, verify=verify,
                    headers=headers, config={'keep_alive': True}, **kwargs)
            rs.send()
            response = rs.response
        except requests.exceptions.SSLError:
            _logger.warning(_('Pass --no-check-certificate ' \
                    'to avoid SSL checks'))
            raise

        if response.status_code != 200:
            if response.status_code == 401:
                _register()
                continue
            content = response.content
            try:
                error = json.loads(content)
            except Exception:
                _logger.debug('Got %s HTTP error for "%s" request:\n%s',
                        response.status_code, path, content)
                response.raise_for_status()
            else:
                raise RuntimeError(error['error'])

        return response


def _register():
    raw_request('POST', ['user'],
            headers={'Content-Type': 'application/json'},
            data={
                'nickname': sugar.nickname() or '',
                'color': sugar.color() or '#000000,#000000',
                'machine_sn': sugar.machine_sn() or '',
                'machine_uuid': sugar.machine_uuid() or '',
                'pubkey': sugar.pubkey(),
                },
            )


def _sign(data):
    key = DSA.load_key(sugar.profile_path('owner.key'))
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')
