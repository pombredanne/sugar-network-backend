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

import json
import logging
import hashlib
from os.path import exists

import requests
from requests.sessions import Session
from M2Crypto import DSA

import active_document as ad
from sugar_network.toolkit import sugar
from sugar_network.toolkit.router import Redirect
from sugar_network import client
from active_toolkit import coroutine, util, enforce


_RECONNECTION_NUMBER = 1
_RECONNECTION_TIMEOUT = 3

_logger = logging.getLogger('http')


class Client(object):

    def __init__(self, api_url='', sugar_auth=False, **kwargs):
        self.api_url = api_url
        self.params = kwargs
        self._sugar_auth = sugar_auth

        verify = True
        if client.no_check_certificate.value:
            verify = False
        elif client.certfile.value:
            verify = client.certfile.value

        headers = {'Accept-Language': ad.default_lang()}
        if self._sugar_auth:
            privkey_path = sugar.privkey_path()
            if not exists(privkey_path):
                _logger.warning('Sugar session was never started, '
                        'fallback to anonymous mode')
                self._sugar_auth = False
            else:
                uid = sugar.uid()
                headers['sugar_user'] = uid
                headers['sugar_user_signature'] = _sign(privkey_path, uid)

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
        response = self.request('POST', path_, json.dumps(data_),
                headers={'Content-Type': 'application/json'}, params=kwargs)
        return self._decode_reply(response)

    def put(self, path_=None, data_=None, **kwargs):
        response = self.request('PUT', path_, json.dumps(data_),
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

        if request.content_type == 'application/json':
            request.content = json.dumps(request.content)

        headers = None
        if request.content is not None:
            headers = {}
            headers['Content-Type'] = \
                    request.content_type or 'application/octet-stream'
            headers['Content-Length'] = str(len(request.content))
        elif request.content_stream is not None:
            headers = {}
            headers['Content-Type'] = \
                    request.content_type or 'application/octet-stream'
            # TODO Avoid reading the full content at once
            request.content = request.content_stream.read()
            headers['Content-Length'] = str(len(request.content))

        reply = self.request(method, path, data=request.content,
                params=params, headers=headers, allowed=[303],
                allow_redirects=request.allow_redirects)

        if reply.status_code == 303:
            raise Redirect(reply.headers['Location'])

        if response is not None:
            if 'Content-Disposition' in reply.headers:
                response['Content-Disposition'] = \
                        reply.headers['Content-Disposition']
            if 'Content-Type' in reply.headers:
                response.content_type = reply.headers['Content-Type']

        result = self._decode_reply(reply)
        if result is None:
            result = reply.raw
        return result

    def subscribe(self):
        return _Subscription(self, _RECONNECTION_NUMBER)

    def _register(self):
        self.post(['user'], {
            'name': sugar.nickname() or '',
            'color': sugar.color() or '#000000,#000000',
            'machine_sn': sugar.machine_sn() or '',
            'machine_uuid': sugar.machine_uuid() or '',
            'pubkey': sugar.pubkey(),
            })

    def _decode_reply(self, response):
        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(response.content)
        else:
            return response.content


class _Subscription(object):

    def __init__(self, aclient, tries):
        self._tries = tries or 1
        self._client = aclient
        self._response = None

    def __iter__(self):
        while True:
            event = self.pull()
            if event is not None:
                yield event

    def fileno(self):
        # pylint: disable-msg=W0212
        return self._handshake()._fp.fp.fileno()

    def pull(self):
        for a_try in (1, 0):
            stream = self._handshake()
            try:
                line = _readline(stream)
                enforce(line is not None, 'Subscription aborted')
                break
            except Exception:
                if a_try == 0:
                    raise
                util.exception('Failed to read from %r subscription, '
                        'will resubscribe', self._client.api_url)

        if line.startswith('data: '):
            try:
                return json.loads(line.split(' ', 1)[1])
            except Exception:
                util.exception('Failed to parse %r event from %r subscription',
                        line, self._client.api_url)

    def _handshake(self):
        if self._response is not None:
            return self._response.raw

        _logger.debug('Subscribe to %r', self._client.api_url)

        for a_try in reversed(xrange(self._tries)):
            try:
                self._response = self._client.request('GET',
                        params={'cmd': 'subscribe'})
                break
            except Exception:
                if a_try == 0:
                    raise
                util.exception(_logger,
                        'Cannot subscribe to %r, retry in %s second(s)',
                        self._client.api_url, _RECONNECTION_TIMEOUT)
                coroutine.sleep(_RECONNECTION_TIMEOUT)

        return self._response.raw


def _sign(privkey_path, data):
    key = DSA.load_key(privkey_path)
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


def _readline(stream):
    line = None
    while True:
        char = stream.read(1)
        if not char:
            break
        if line is None:
            line = char
        else:
            line += char
        if char == '\n':
            break
    return line
