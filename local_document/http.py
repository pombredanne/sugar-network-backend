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
import cgi
import json
import shutil
import logging
import hashlib
import tempfile
from os.path import isdir, exists, dirname, join
from gettext import gettext as _

import requests
import requests.async
from M2Crypto import DSA

from sweets_recipe import Bundle
from active_toolkit.sockets import decode_multipart, BUFFER_SIZE
from local_document import env, sugar


_logger = logging.getLogger('local_document.http')
_headers = {}


def download(url_path, out_path, seqno=None, extract=False):
    if isdir(out_path):
        shutil.rmtree(out_path)
    elif not exists(dirname(out_path)):
        os.makedirs(dirname(out_path))

    params = {}
    if seqno:
        params['seqno'] = seqno

    response = raw_request('GET', url_path, allow_redirects=True,
            params=params, allowed_response=[404])
    if response.status_code != 200:
        return 'application/octet-stream'

    mime_type = response.headers.get('Content-Type') or \
            'application/octet-stream'

    def fetch(f):
        _logger.debug('Download %r BLOB to %r', '/'.join(url_path), out_path)

        length = int(response.headers.get('Content-Length', BUFFER_SIZE))
        chunk_size = min(length, BUFFER_SIZE)
        empty = True

        for chunk in response.iter_content(chunk_size=chunk_size):
            empty = False
            f.write(chunk)

        return not empty

    def download_multipart(stream, size, boundary):
        stream.readline = None
        for filename, content in decode_multipart(stream, size, boundary):
            dst_path = join(out_path, filename)
            if not exists(dirname(dst_path)):
                os.makedirs(dirname(dst_path))
            shutil.move(content.name, dst_path)

    content_type, params = cgi.parse_header(mime_type)
    if content_type.split('/', 1)[0] == 'multipart':
        try:
            download_multipart(response.raw,
                    int(response.headers['content-length']),
                    params['boundary'])
        except Exception:
            shutil.rmtree(out_path, ignore_errors=True)
            raise
    elif extract:
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if fetch(tmp_file):
                tmp_file.close()
                with Bundle(tmp_file.name, 'application/zip') as bundle:
                    bundle.extractall(out_path)
        finally:
            if exists(tmp_file.name):
                os.unlink(tmp_file.name)
    else:
        with file(out_path, 'w') as f:
            if not fetch(f):
                os.unlink(out_path)

    return mime_type


def request(method, path, data=None, headers=None, **kwargs):
    response = raw_request(method, path, data, headers, **kwargs)
    if response.headers.get('Content-Type') == 'application/json':
        return json.loads(response.content)
    else:
        return response


def raw_request(method, path, data=None, headers=None, allowed_response=None,
        **kwargs):
    if not path:
        path = ['']
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
                _logger.info(_('User is not registered on the server, ' \
                        'registering'))
                _register()
                continue
            if allowed_response and response.status_code in allowed_response:
                return response
            content = response.content
            try:
                error = json.loads(content)
            except Exception:
                _logger.debug('Got %s HTTP error for %r request:\n%s',
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
