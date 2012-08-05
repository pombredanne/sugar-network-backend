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
import cgi
import json
import shutil
import logging
import hashlib
import tempfile
from os.path import isdir, exists, dirname, join

import requests
from requests.sessions import Session
from M2Crypto import DSA

from sweets_recipe import Bundle
from active_toolkit.sockets import decode_multipart, BUFFER_SIZE
from sugar_network.toolkit import sugar
from sugar_network import local

# Let toolkit.http work in concurrence
# TODO Is it safe for the rest of code?
from gevent.monkey import patch_socket
patch_socket(dns=False)


_logger = logging.getLogger('toolkit.http')
_session = None


def reset():
    global _session
    _session = None


def download(url_path, out_path, seqno=None, extract=False):
    if isdir(out_path):
        shutil.rmtree(out_path)
    elif not exists(dirname(out_path)):
        os.makedirs(dirname(out_path))

    params = {}
    if seqno:
        params['seqno'] = seqno

    response = _request('GET', url_path, allow_redirects=True,
            params=params, allowed_response=[404])
    if response.status_code != 200:
        return 'application/octet-stream'

    mime_type = response.headers.get('Content-Type') or \
            'application/octet-stream'

    content_length = response.headers.get('Content-Length')
    content_length = int(content_length) if content_length else 0
    if seqno and not content_length:
        # Local cacheed versions is not stale
        return mime_type

    def fetch(f):
        _logger.debug('Download %r BLOB to %r', '/'.join(url_path), out_path)
        chunk_size = min(content_length, BUFFER_SIZE)
        empty = True
        for chunk in response.iter_content(chunk_size=chunk_size):
            empty = False
            f.write(chunk)
        return not empty

    def fetch_multipart(stream, size, boundary):
        stream.readline = None
        for filename, content in decode_multipart(stream, size, boundary):
            dst_path = join(out_path, filename)
            if not exists(dirname(dst_path)):
                os.makedirs(dirname(dst_path))
            shutil.move(content.name, dst_path)

    content_type, params = cgi.parse_header(mime_type)
    if content_type.split('/', 1)[0] == 'multipart':
        try:
            fetch_multipart(response.raw, content_length, params['boundary'])
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
    response = _request(method, path, data, headers, **kwargs)
    if response.headers.get('Content-Type') == 'application/json':
        return json.loads(response.content)
    else:
        return response


def _request(method, path, data=None, headers=None, allowed_response=None,
        **kwargs):
    global _session

    if _session is None:
        verify = True
        if local.no_check_certificate.value:
            verify = False
        elif local.certfile.value:
            verify = local.certfile.value

        headers = None
        key_path = sugar.profile_path('owner.key')
        if exists(key_path):
            uid = sugar.uid()
            headers = {
                    'sugar_user': uid,
                    'sugar_user_signature': _sign(key_path, uid),
                    }

        _session = Session(headers=headers, verify=verify)

    if not path:
        path = ['']
    if not isinstance(path, basestring):
        path = '/'.join([i.strip('/') for i in [local.api_url.value] + path])

    if data is not None and headers and \
            headers.get('Content-Type') == 'application/json':
        data = json.dumps(data)

    while True:
        try:
            response = requests.request(method, path, data=data,
                    headers=headers, session=_session, **kwargs)
        except requests.exceptions.SSLError:
            _logger.warning('Pass --no-check-certificate to avoid SSL checks')
            raise

        if response.status_code != 200:
            if response.status_code == 401:
                _logger.info('User is not registered on the server, '
                        'registering')
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
    _request('POST', ['user'],
            headers={'Content-Type': 'application/json'},
            data={
                'name': sugar.nickname() or '',
                'color': sugar.color() or '#000000,#000000',
                'machine_sn': sugar.machine_sn() or '',
                'machine_uuid': sugar.machine_uuid() or '',
                'pubkey': sugar.pubkey(),
                },
            )


def _sign(key_path, data):
    key = DSA.load_key(key_path)
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')
