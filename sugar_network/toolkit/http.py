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

import os
import sys
import json
import types
import hashlib
import logging
from os.path import join, dirname, exists, expanduser, abspath

from sugar_network import toolkit
from sugar_network.toolkit import i18n, enforce


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


class _ConnectionError(Status):

    status = '999 For testing purpose only'
    status_code = 999


class Connection(object):

    _Session = None

    def __init__(self, api_url='', auth=None, max_retries=0, **session_args):
        self.api_url = api_url
        self.auth = auth
        self._max_retries = max_retries
        self._session_args = session_args
        self._session = None
        self._nonce = None

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
        reply = self.request('HEAD', path, allowed=[NotFound.status_code])
        return reply.status_code != NotFound.status_code

    def head(self, path_=None, **kwargs):
        from sugar_network.toolkit.router import Request, Response
        request = Request(method='HEAD', path=path_, **kwargs)
        response = Response()
        self.call(request, response)
        return response

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

        try_ = 0
        while True:
            try_ += 1
            reply = self._session.request(method, path, data=data,
                    headers=headers, params=params, **kwargs)
            if reply.status_code == Unauthorized.status_code:
                enforce(self.auth is not None, Unauthorized, 'No credentials')
                self._authenticate(reply.headers.get('www-authenticate'))
                try_ = 0
            elif reply.status_code == 200 or \
                    allowed and reply.status_code in allowed:
                break
            else:
                content = reply.content
                try:
                    error = json.loads(content)['error']
                except Exception:
                    # On non-JSONified fail response, assume that the error
                    # was not sent by the application level server code, i.e.,
                    # something happaned on low level, like connection abort.
                    # If so, try to resend request.
                    if try_ <= self._max_retries and method in ('GET', 'HEAD'):
                        continue
                    error = content or reply.headers.get('x-sn-error') or \
                            'No error message provided'
                cls = _FORWARD_STATUSES.get(reply.status_code, RuntimeError) \
                        or ConnectionError
                raise cls(error)

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
        for env_key, key in (
                ('HTTP_IF_MODIFIED_SINCE', 'if-modified-since'),
                ('HTTP_ACCEPT_LANGUAGE', 'accept-language'),
                ('HTTP_ACCEPT_ENCODING', 'accept-encoding'),
                ):
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
                if resend:
                    response.relocations += 1
                else:
                    for key, value in reply.headers.items():
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
            sys_path = join(dirname(dirname(__file__)), 'lib', 'requests')
            sys.path.insert(0, sys_path)
            from requests import Session, exceptions
            Connection._Session = Session
            # pylint: disable-msg=W0601
            global ConnectionError
            ConnectionError = exceptions.ConnectionError

        self._session = Connection._Session()
        self._session.headers['accept-language'] = \
                ','.join(i18n.default_langs())
        for arg, value in self._session_args.items():
            setattr(self._session, arg, value)
        self._session.stream = True

    def _authenticate(self, challenge):
        from urllib2 import parse_http_list, parse_keqv_list

        nonce = None
        if challenge:
            challenge = challenge.split(' ', 1)[-1]
            nonce = parse_keqv_list(parse_http_list(challenge)).get('nonce')

        if self._nonce and nonce == self._nonce:
            enforce(self.auth.profile(), Unauthorized, 'Bad credentials')
            _logger.info('Register on the server')
            self.post(['user'], self.auth.profile())

        self._session.headers['authorization'] = self.auth(nonce)
        self._nonce = nonce


class SugarAuth(object):

    def __init__(self, key_path, profile=None):
        self._key_path = abspath(expanduser(key_path))
        self._profile = profile or {}
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

    def profile(self):
        if 'name' not in self._profile:
            self._profile['name'] = self.login
        self._profile['pubkey'] = self.pubkey
        return self._profile

    def __call__(self, nonce):
        self.ensure_key()
        data = hashlib.sha1('%s:%s' % (self.login, nonce)).digest()
        signature = self._key.sign(data).encode('hex')
        return 'Sugar username="%s",nonce="%s",signature="%s"' % \
                (self.login, nonce, signature)

    def ensure_key(self):
        from M2Crypto import RSA
        from base64 import b64encode

        key_dir = dirname(self._key_path)
        if exists(self._key_path):
            if os.stat(key_dir) & 077:
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
        for try_ in (1, 0):
            stream = self._handshake()
            try:
                line = toolkit.readline(stream)
                enforce(line, 'Subscription aborted')
                break
            except Exception:
                if try_ == 0:
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


_FORWARD_STATUSES = {
        BadRequest.status_code: BadRequest,
        Forbidden.status_code: Forbidden,
        NotFound.status_code: NotFound,
        BadGateway.status_code: BadGateway,
        ServiceUnavailable.status_code: ServiceUnavailable,
        GatewayTimeout.status_code: GatewayTimeout,
        _ConnectionError.status_code: None,
        }
