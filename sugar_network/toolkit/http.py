# Copyright (C) 2012-2014 Aleksey Lim
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
import logging
import platform
from os.path import join, dirname

from sugar_network import toolkit, version
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

    def __init__(self, url='', creds=None, max_retries=0, auth_request=None,
            **session_args):
        self.url = url.rstrip('/')
        self.creds = creds
        self._max_retries = max_retries
        self._session_args = session_args
        self._session = None
        self._auth_request = auth_request

    def __repr__(self):
        return '<Connection url=%s>' % self.url

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
        if data_ is not None:
            data_ = json.dumps(data_)
        reply = self.request('POST', path_, data_,
                headers={'Content-Type': 'application/json'},
                params=query_ or kwargs)
        return self._decode_reply(reply)

    def put(self, path_=None, data_=None, query_=None, **kwargs):
        if data_ is not None:
            data_ = json.dumps(data_)
        reply = self.request('PUT', path_, data_,
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
        return reply

    def upload(self, path_=None, data=None, **kwargs):
        reply = self.request('POST', path_, data, params=kwargs)
        if reply.headers.get('Content-Type') == 'application/json':
            return json.loads(reply.content)
        else:
            return reply.raw

    def request(self, method, path=None, data=None, headers=None, allowed=None,
            params=None, **kwargs):
        if data is not None and self._auth_request:
            auth_request = self._auth_request
            self._auth_request = None
            self.request(**auth_request)

        if self._session is None:
            self._init()

        if not path:
            path = ['']
        if not isinstance(path, basestring):
            path = '/'.join([i.strip('/') for i in path])
            if self.url.startswith('file://'):
                path = self.url + '#' + path
            else:
                path = self.url + '/' + path

        # TODO Disable cookies on requests library level
        self._session.cookies.clear()

        try_ = 0
        challenge = None
        while True:
            try_ += 1
            reply = self._session.request(method, path, data=data,
                    headers=headers, params=params, **kwargs)
            if reply.status_code == Unauthorized.status_code:
                enforce(data is None,
                        'Authorization is requited '
                        'but no way to resend posting data')
                enforce(self.creds is not None, Unauthorized, 'No credentials')
                challenge_ = reply.headers.get('www-authenticate')
                if challenge and challenge == challenge_:
                    profile = self.creds.profile
                    enforce(profile, Unauthorized, 'No way to self-register')
                    _logger.info('Register on the server')
                    self.post(['user'], profile)
                challenge = challenge_
                self._session.headers.update(self.creds.logon(challenge))
                self._auth_request = None
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
                    # was not sent by the application-level server code, i.e.,
                    # something happaned on low level, like connection abort.
                    # If so, try to resend request.
                    if try_ <= self._max_retries and data is None:
                        continue
                    error = content or reply.headers.get('x-error') or \
                            'No error message provided'
                cls = _FORWARD_STATUSES.get(reply.status_code, RuntimeError) \
                        or ConnectionError
                raise cls(error)

        return reply

    def call(self, request, response=None):
        headers = {
            'content-type': request.content_type or 'application/octet-stream',
            }
        for env_key, key in (
                ('CONTENT_LENGTH', 'content-length'),
                ('HTTP_IF_MODIFIED_SINCE', 'if-modified-since'),
                ('HTTP_ACCEPT_LANGUAGE', 'accept-language'),
                ('HTTP_ACCEPT_ENCODING', 'accept-encoding'),
                ):
            value = request.environ.get(env_key)
            if value is not None:
                headers[key] = value
        headers.update(request.headers)

        data = None
        if request.method in ('POST', 'PUT'):
            if request.content_type == 'application/json':
                data = json.dumps(request.content)
            else:
                data = request.content

        path = request.path
        while True:
            reply = self.request(request.method, path, data=data,
                    params=request.query or request, headers=headers,
                    allowed=_REDIRECT_CODES, allow_redirects=False)
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
                path = self.url + path

        if request.method != 'HEAD':
            if reply.headers.get('Content-Type') == 'application/json':
                if reply.content:
                    return json.loads(reply.content)
                else:
                    return None
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
        self._session.headers['user-agent'] = \
                'sugar-network-client/%s %s/%s' % (
                        version,
                        platform.system(),
                        platform.release(),
                        )
        for arg, value in self._session_args.items():
            setattr(self._session, arg, value)
        self._session.stream = True


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
                _logger.exception(
                        'Failed to read from %r subscription, resubscribe',
                        self._client.url)
                self._content = None
        return _parse_event(line)

    def _handshake(self, **params):
        if self._content is not None:
            return self._content
        params.update(self._condition)
        params['cmd'] = 'subscribe'
        _logger.debug('Subscribe to %r, %r', self._client.url, params)
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
