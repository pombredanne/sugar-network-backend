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

from sugar_network import client, toolkit
from sugar_network.toolkit import enforce


_REDIRECT_CODES = frozenset([301, 302, 303, 307, 308])

_logger = logging.getLogger('http')


class ConnectionError(Exception):
    pass


class Status(Exception):

    status = None
    headers = None


class StatusPass(Status):
    pass


class NotModified(StatusPass):

    status = '304 Not Modified'
    status_code = 304


class Redirect(StatusPass):

    status = '303 See Other'
    status_code = 303

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


class BadGateway(Status):

    status = '502 Bad Gateway'
    status_code = 502


class ServiceUnavailable(Status):

    status = '503 Service Unavailable'
    status_code = 503


class GatewayTimeout(Status):

    status = '504 Gateway Timeout'
    status_code = 504


def download(url, dst_path=None):
    # TODO (?) Reuse HTTP session
    return Connection().download(url, dst_path)


class Connection(object):

    _Session = None
    _SSLError = None
    _ConnectionError = None

    def __init__(self, api_url='', creds=None, trust_env=True, max_retries=0):
        self.api_url = api_url
        self._get_profile = None
        self._session = None
        self._creds = creds
        self._trust_env = trust_env
        self._max_retries = max_retries

    def __repr__(self):
        return '<Connection api_url=%s>' % self.api_url

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self._session is not None:
            self._session.close()

    def exists(self, path):
        reply = self.request('GET', path, allowed=[404])
        return reply.status_code != 404

    def head(self, path_=None, **kwargs):
        from sugar_network.toolkit.router import Request, Response
        request = Request(method='HEAD', path=path_, **kwargs)
        response = Response()
        self.call(request, response)
        return response.meta

    def get(self, path_=None, query_=None, **kwargs):
        reply = self.request('GET', path_, params=query_ or kwargs)
        return self._decode_reply(reply)

    def post(self, path_=None, data_=None, query_=None, **kwargs):
        reply = self.request('POST', path_, json.dumps(data_),
                headers={'Content-Type': 'application/json'},
                params=query_ or kwargs)
        return self._decode_reply(reply)

    def put(self, path_=None, data_=None, query_=None, **kwargs):
        reply = self.request('PUT', path_, json.dumps(data_),
                headers={'Content-Type': 'application/json'},
                params=query_ or kwargs)
        return self._decode_reply(reply)

    def delete(self, path_=None, query_=None, **kwargs):
        reply = self.request('DELETE', path_, params=query_ or kwargs)
        return self._decode_reply(reply)

    def download(self, path, dst=None):
        reply = self.request('GET', path, allow_redirects=True)

        content_length = reply.headers.get('Content-Length')
        if content_length:
            chunk_size = min(int(content_length), toolkit.BUFFER_SIZE)
        else:
            chunk_size = toolkit.BUFFER_SIZE

        if dst is None:
            return reply.iter_content(chunk_size=chunk_size)

        f = file(dst, 'wb') if isinstance(dst, basestring) else dst
        try:
            for chunk in reply.iter_content(chunk_size=chunk_size):
                f.write(chunk)
        finally:
            if isinstance(dst, basestring):
                f.close()

    def upload(self, path, data, **kwargs):
        if isinstance(data, basestring):
            with file(data, 'rb') as f:
                reply = self.request('POST', path, f, params=kwargs)
        else:
            reply = self.request('POST', path, data, params=kwargs)
        if reply.headers.get('Content-Type') == 'application/json':
            return json.loads(reply.content)
        else:
            return reply.raw

    def request(self, method, path=None, data=None, headers=None, allowed=None,
            params=None, **kwargs):
        if self._session is None:
            self._init()

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
                reply = self._session.request(method, path, data=data,
                        headers=headers, params=params, **kwargs)
            except Connection._SSLError:
                _logger.warning('Use --no-check-certificate to avoid checks')
                raise
            except Connection._ConnectionError, error:
                raise ConnectionError, error, sys.exc_info()[2]
            if reply.status_code != 200:
                if reply.status_code == 401:
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
                if allowed and reply.status_code in allowed:
                    break
                content = reply.content
                try:
                    error = json.loads(content)['error']
                except Exception:
                    # On non-JSONified fail response, assume that the error
                    # was not sent by the application level server code, i.e.,
                    # something happaned on low level, like connection abort.
                    # If so, try to resend request.
                    if a_try <= self._max_retries and method == 'GET':
                        continue
                    error = content or reply.headers.get('x-sn-error') or \
                            'No error message provided'
                cls = _FORWARD_STATUSES.get(reply.status_code, RuntimeError)
                raise cls, error, sys.exc_info()[2]
            break

        return reply

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
                ('HTTP_ACCEPT_LANGUAGE', 'accept-language', ','.join(
                    client.accept_language.value or toolkit.default_langs())),
                ('HTTP_ACCEPT_ENCODING', 'accept-encoding', None),
                ):
            if value is None:
                value = request.environ.get(env_key)
            if value is not None:
                headers[key] = value

        path = request.path
        while True:
            reply = self.request(request.method, path,
                    data=request.content, params=request.query or request,
                    headers=headers, allowed=_REDIRECT_CODES,
                    allow_redirects=False)
            resend = reply.status_code in _REDIRECT_CODES
            if response is not None:
                if 'transfer-encoding' in reply.headers:
                    # `requests` library handles encoding on its own
                    del reply.headers['transfer-encoding']
                for key, value in reply.headers.items():
                    if key.startswith('x-sn-'):
                        response.meta[key[5:]] = json.loads(value)
                    elif not resend:
                        response[key] = value
            if not resend:
                break
            path = reply.headers['location']
            if path.startswith('/'):
                path = self.api_url + path

        if request.method != 'HEAD':
            if reply.headers.get('Content-Type') == 'application/json':
                return json.loads(reply.content)
            else:
                return reply.raw

    def subscribe(self, **condition):
        return _Subscription(self, condition)

    def _decode_reply(self, reply):
        if reply.headers.get('Content-Type') == 'application/json':
            return json.loads(reply.content)
        elif reply.headers.get('Content-Type') == 'text/event-stream':
            return _pull_events(reply.raw)
        else:
            return reply.content

    def _init(self):
        if Connection._Session is None:
            sys.path.insert(0,
                    join(dirname(__file__), '..', 'lib', 'requests'))
            from requests import Session
            from requests.exceptions import SSLError
            from requests.exceptions import ConnectionError as _ConnectionError
            Connection._Session = Session
            Connection._SSLError = SSLError
            Connection._ConnectionError = _ConnectionError

        self._session = Connection._Session()
        self._session.stream = True
        self._session.trust_env = self._trust_env
        if client.no_check_certificate.value:
            self._session.verify = False
        if self._creds:
            uid, keyfile, self._get_profile = self._creds
            self._session.headers['X-SN-login'] = uid
            self._session.headers['X-SN-signature'] = _sign(keyfile, uid)
        self._session.headers['accept-language'] = toolkit.default_lang()


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
        return _parse_event(line)

    def _handshake(self, **params):
        if self._content is not None:
            return self._content
        params.update(self._condition)
        params['cmd'] = 'subscribe'
        _logger.debug('Subscribe to %r, %r', self._client.api_url, params)
        response = self._client.request('GET', params=params)
        self._content = response.raw
        return self._content


def _pull_events(stream):
    while True:
        line = toolkit.readline(stream)
        if not line:
            break
        event = _parse_event(line)
        if event is not None:
            yield event


def _parse_event(line):
    if line and line.startswith('data: '):
        try:
            return json.loads(line[6:])
        except Exception:
            _logger.exception('Failed to parse %r event', line)


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
        BadGateway.status_code: BadGateway,
        ServiceUnavailable.status_code: ServiceUnavailable,
        GatewayTimeout.status_code: GatewayTimeout,
        }
