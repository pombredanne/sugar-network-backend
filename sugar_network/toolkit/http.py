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

# pylint: disable-msg=E1103

import os
import cgi
import json
import time
import shutil
import logging
import hashlib
import tempfile
from os.path import isdir, exists, dirname, join

import requests
from requests.sessions import Session
from M2Crypto import DSA

import active_document as ad
from sugar_network.zerosugar import Bundle
from active_toolkit.sockets import decode_multipart, BUFFER_SIZE
from sugar_network.toolkit import sugar
from sugar_network import local
from active_toolkit import coroutine, enforce

# Let toolkit.http work in concurrence
from gevent import monkey
# XXX No DNS because `toolkit.network.res_init()` doesn't work otherwise
monkey.patch_socket(dns=False)
monkey.patch_select()
monkey.patch_ssl()
monkey.patch_time()


_RECONNECTION_NUMBER = 1
_RECONNECTION_TIMEOUT = 3

_logger = logging.getLogger('http')


class Client(object):

    def __init__(self, api_url='', sugar_auth=False, **kwargs):
        self.api_url = api_url
        self.params = kwargs
        self._sugar_auth = sugar_auth

        verify = True
        if local.no_check_certificate.value:
            verify = False
        elif local.certfile.value:
            verify = local.certfile.value

        headers = {'Accept-Language': ad.default_lang()}
        if self._sugar_auth:
            uid = sugar.uid()
            headers['sugar_user'] = uid
            headers['sugar_user_signature'] = _sign(uid)

        self._session = Session(headers=headers, verify=verify, prefetch=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()

    def exists(self, path):
        response = self.request('GET', path, allowed=[404])
        return response.status_code != 404

    def get(self, path_=None, **kwargs):
        response = self.request('GET', path_, params=kwargs)
        return self._decode_reply(response)

    def post(self, path_=None, data_=None, **kwargs):
        response = self.request('POST', path_, data_,
                headers={'Content-Type': 'application/json'}, params=kwargs)
        return self._decode_reply(response)

    def put(self, path_=None, data_=None, **kwargs):
        response = self.request('PUT', path_, data_,
                headers={'Content-Type': 'application/json'}, params=kwargs)
        return self._decode_reply(response)

    def delete(self, path_=None, **kwargs):
        response = self.request('DELETE', path_, params=kwargs)
        return self._decode_reply(response)

    def request(self, method, path=None, data=None, headers=None, allowed=None,
            params=None, **kwargs):
        if not path:
            path = ['']
        if not isinstance(path, basestring):
            path = '/'.join([i.strip('/') for i in [self.api_url] + path])

        if data is not None and headers and \
                headers.get('Content-Type') == 'application/json':
            data = json.dumps(data)

        if params is None:
            params = self.params
        else:
            params.update(self.params)

        while True:
            try:
                response = requests.request(method, path, data=data,
                        headers=headers, session=self._session, params=params,
                        **kwargs)
            except requests.exceptions.SSLError:
                _logger.warning('Use --no-check-certificate to avoid checks')
                raise

            if response.status_code != 200:
                if response.status_code == 401:
                    enforce(self._sugar_auth,
                            'Operation is not available in anonymous mode')
                    _logger.info('User is not registered on the server, '
                            'registering')
                    self._register()
                    continue
                if allowed and response.status_code in allowed:
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

    def call(self, request, response=None):
        params = request.copy()
        method = params.pop('method')
        document = params.pop('document') if 'document' in params else None
        guid = params.pop('guid') if 'guid' in params else None
        prop = params.pop('prop') if 'prop' in params else None

        path = []
        if document:
            path.append(document)
        if guid:
            path.append(guid)
        if prop:
            path.append(prop)

        reply = self.request(method, path, data=request.content,
                params=params, headers={'Content-Type': 'application/json'})

        if response is not None:
            response.content_type = reply.headers['Content-Type']

        result = self._decode_reply(reply)
        if result is None:
            result = reply.content
        return result

    def download(self, url_path, out_path, seqno=None, extract=False):
        if isdir(out_path):
            shutil.rmtree(out_path)
        elif not exists(dirname(out_path)):
            os.makedirs(dirname(out_path))

        params = {}
        if seqno:
            params['seqno'] = seqno

        response = self.request('GET', url_path, allow_redirects=True,
                params=params, allowed=[404])
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
            _logger.debug('Download %r BLOB to %r',
                    '/'.join(url_path), out_path)
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
                fetch_multipart(response.raw, content_length,
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

    def subscribe(self):

        def handshake():
            _logger.debug('Subscribe to %r', self.api_url)
            return self.request('GET', params={'cmd': 'subscribe'}).raw

        def pull_events(stream):
            retries = _RECONNECTION_NUMBER
            while True:
                start_time = time.time()
                try:
                    if stream is None:
                        stream = handshake()
                    for line in _readlines(stream):
                        if line.startswith('data: '):
                            yield json.loads(line.split(' ', 1)[1])
                except Exception:
                    if time.time() - start_time > _RECONNECTION_TIMEOUT * 10:
                        retries = _RECONNECTION_NUMBER
                    if retries <= 0:
                        raise
                _logger.debug('Re-subscribe to %r in %s second(s)',
                        self.api_url, _RECONNECTION_TIMEOUT)
                self.close()
                coroutine.sleep(_RECONNECTION_TIMEOUT)
                retries -= 1
                stream = None

        return pull_events(handshake())

    def _register(self):
        self.request('POST', ['user'],
                headers={
                    'Content-Type': 'application/json',
                    },
                data={
                    'name': sugar.nickname() or '',
                    'color': sugar.color() or '#000000,#000000',
                    'machine_sn': sugar.machine_sn() or '',
                    'machine_uuid': sugar.machine_uuid() or '',
                    'pubkey': sugar.pubkey(),
                    },
                )

    def _decode_reply(self, response):
        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(response.content)


def _sign(data):
    key = DSA.load_key(sugar.privkey_path())
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


def _readlines(stream):
    line = ''
    while True:
        char = stream.read(1)
        if not char:
            break
        if char == '\n':
            yield line
            line = ''
        else:
            line += char
