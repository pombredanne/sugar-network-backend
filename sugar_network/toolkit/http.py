# Copyright (C) 2012-2013 Aleksey Lim
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

import sys
import json
import types
import logging
from os.path import join, dirname

sys.path.insert(0, join(dirname(__file__), '..', 'lib', 'requests'))

from requests import Session
# pylint: disable-msg=W0611
from requests.exceptions import SSLError, ConnectionError, HTTPError

from sugar_network import client, toolkit
from sugar_network.toolkit import coroutine, enforce


_logger = logging.getLogger('http')


class Status(Exception):

    status = None
    headers = None


class StatusPass(Status):
    pass


class NotModified(StatusPass):

    status = '304 Not Modified'


class Redirect(StatusPass):

    status = '303 See Other'

    def __init__(self, location):
        StatusPass.__init__(self)
        self.headers = {'location': location}


class BadRequest(Status):

    status = '400 Bad Request'
    status_code = 400


class Unauthorized(Status):

    status = '401 Unauthorized'
    headers = {'WWW-Authenticate': 'Sugar'}
    status_code = 401


class Forbidden(Status):

    status = '403 Forbidden'
    status_code = 403


class NotFound(Status):

    status = '404 Not Found'
    status_code = 404


class ServiceUnavailable(Status):

    status = '503 Service Unavailable'
    status_code = 503


def download(url, dst_path=None):
    # TODO (?) Reuse HTTP session
    return Connection().download(url, dst_path)


class Connection(object):

    def __init__(self, api_url='', creds=None, trust_env=True, max_retries=0):
        self.api_url = api_url
        self._get_profile = None
        self._session = session = Session()
        self._max_retries = max_retries

        session.stream = True
        session.trust_env = trust_env
        if client.no_check_certificate.value:
            session.verify = False
        if creds:
            uid, keyfile, self._get_profile = creds
            session.headers['X-SN-login'] = uid
            session.headers['X-SN-signature'] = _sign(keyfile, uid)
        session.headers['accept-language'] = toolkit.default_lang()

    def __repr__(self):
        return '<Connection api_url=%s>' % self.api_url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()

    def exists(self, path):
        response = self.request('GET', path, allowed=[404])
        return response.status_code != 404

    def get(self, path_=None, query_=None, **kwargs):
        response = self.request('GET', path_, params=kwargs)
        return self._decode_reply(response)

    def meta(self, path_=None, query_=None, **kwargs):
        response = self.request('HEAD', path_, params=query_ or kwargs)
        result = {}
        for key, value in response.headers.items():
            if key.startswith('x-sn-'):
                result[key[5:]] = json.loads(value)
            else:
                result[key] = value
        return result

    def post(self, path_=None, data_=None, query_=None, **kwargs):
        response = self.request('POST', path_, json.dumps(data_),
                headers={'Content-Type': 'application/json'},
                params=query_ or kwargs)
        return self._decode_reply(response)

    def put(self, path_=None, data_=None, query_=None, **kwargs):
        response = self.request('PUT', path_, json.dumps(data_),
                headers={'Content-Type': 'application/json'},
                params=query_ or kwargs)
        return self._decode_reply(response)

    def delete(self, path_=None, query_=None, **kwargs):
        response = self.request('DELETE', path_, params=query_ or kwargs)
        return self._decode_reply(response)

    def download(self, path, dst=None):
        response = self.request('GET', path, allow_redirects=True)

        content_length = response.headers.get('Content-Length')
        if content_length:
            chunk_size = min(int(content_length), toolkit.BUFFER_SIZE)
        else:
            chunk_size = toolkit.BUFFER_SIZE

        if dst is None:
            return response.iter_content(chunk_size=chunk_size)

        f = file(dst, 'wb') if isinstance(dst, basestring) else dst
        try:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
        finally:
            if isinstance(dst, basestring):
                f.close()

    def upload(self, path, data, **kwargs):
        with file(data, 'rb') as f:
            response = self.request('POST', path, f, params=kwargs)
        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(response.content)
        else:
            return response.raw

    def request(self, method, path=None, data=None, headers=None, allowed=None,
            params=None, **kwargs):
        if not path:
            path = ['']
        if not isinstance(path, basestring):
            path = '/'.join([i.strip('/') for i in [self.api_url] + path])
        if isinstance(params, basestring):
            path += '?' + params
            params = None

        a_try = 0
        while True:
            a_try += 1
            try:
                response = self._session.request(method, path, data=data,
                        headers=headers, params=params, **kwargs)
            except SSLError:
                _logger.warning('Use --no-check-certificate to avoid checks')
                raise

            if response.status_code != 200:
                if response.status_code == 401:
                    enforce(method not in ('PUT', 'POST') or
                            not hasattr(data, 'read'),
                            'Cannot resend data after authentication')
                    enforce(self._get_profile is not None,
                            'Operation is not available in anonymous mode')
                    _logger.info('User is not registered on the server, '
                            'registering')
                    self.post(['user'], self._get_profile())
                    a_try = 0
                    continue
                if allowed and response.status_code in allowed:
                    return response
                content = response.content
                try:
                    error = json.loads(content)['error']
                except Exception:
                    # On non-JSONified fail response, assume that the error
                    # was not sent by the application level server code, i.e.,
                    # something happaned on low level, like connection abort.
                    # If so, try to resend request.
                    if a_try <= self._max_retries and method == 'GET':
                        continue
                    error = content or response.headers.get('x-sn-error') or \
                            'No error message provided'
                _logger.trace('Request failed, method=%s path=%r params=%r '
                        'headers=%r status_code=%s error=%s',
                        method, path, params, headers, response.status_code,
                        '\n' + error)
                cls = _FORWARD_STATUSES.get(response.status_code, RuntimeError)
                raise cls(error)

            return response

    def call(self, request, response=None):
        if request.content_type == 'application/json':
            request.content = json.dumps(request.content)

        headers = {}
        if request.content is not None:
            headers['content-type'] = \
                    request.content_type or 'application/octet-stream'
            headers['content-length'] = str(len(request.content))
        elif request.content_stream is not None:
            headers['content-type'] = \
                    request.content_type or 'application/octet-stream'
            # TODO Avoid reading the full content at once
            if isinstance(request.content_stream, types.GeneratorType):
                request.content = ''.join([i for i in request.content_stream])
            else:
                request.content = request.content_stream.read()
            headers['content-length'] = str(len(request.content))
        for env_key, key, value in (
                ('HTTP_IF_MODIFIED_SINCE', 'if-modified-since', None),
                ('HTTP_ACCEPT_LANGUAGE', 'accept-language',
                    client.accept_language.value),
                ('HTTP_ACCEPT_ENCODING', 'accept-encoding', None),
                ):
            if value is None:
                value = request.environ.get(env_key)
            if value is not None:
                headers[key] = value

        reply = self.request(request.method, request.path,
                data=request.content, params=request.query or request,
                headers=headers, allow_redirects=True)

        if response is not None:
            if 'transfer-encoding' in reply.headers:
                # `requests` library handles encoding on its own
                del reply.headers['transfer-encoding']
            response.update(reply.headers)

        if request.method != 'HEAD':
            if reply.headers.get('Content-Type') == 'application/json':
                return json.loads(reply.content)
            else:
                return reply.raw

    def subscribe(self, **condition):
        return _Subscription(self, condition)

    def _decode_reply(self, response):
        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(response.content)
        else:
            return response.content


class _Subscription(object):

    def __init__(self, aclient, condition):
        self._client = aclient
        self._content = None
        self._condition = condition

    def __iter__(self):
        while True:
            event = self.pull()
            if event is not None:
                yield event

    def fileno(self):
        # pylint: disable-msg=W0212
        return self._handshake(ping=True)._fp.fp.fileno()

    def pull(self):
        for a_try in (1, 0):
            stream = self._handshake()
            try:
                line = toolkit.readline(stream)
                enforce(line, 'Subscription aborted')
                break
            except Exception:
                if a_try == 0:
                    raise
                toolkit.exception('Failed to read from %r subscription, '
                        'will resubscribe', self._client.api_url)
                self._content = None

        if line.startswith('data: '):
            try:
                return json.loads(line.split(' ', 1)[1])
            except Exception:
                toolkit.exception(
                        'Failed to parse %r event from %r subscription',
                        line, self._client.api_url)

    def _handshake(self, **params):
        if self._content is not None:
            return self._content
        params.update(self._condition)
        params['cmd'] = 'subscribe'
        _logger.debug('Subscribe to %r, %r', self._client.api_url, params)
        response = self._client.request('GET', params=params)
        self._content = response.raw
        return self._content


def _sign(key_path, data):
    import hashlib
    from M2Crypto import DSA
    key = DSA.load_key(key_path)
    # pylint: disable-msg=E1121
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


_FORWARD_STATUSES = {
        BadRequest.status_code: BadRequest,
        Forbidden.status_code: Forbidden,
        NotFound.status_code: NotFound,
        ServiceUnavailable.status_code: ServiceUnavailable,
        }
