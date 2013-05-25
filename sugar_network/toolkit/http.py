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
from requests.exceptions import SSLError

from sugar_network import client, toolkit
from sugar_network.toolkit import coroutine, util
from sugar_network.toolkit import BUFFER_SIZE, exception, enforce


_RECONNECTION_NUMBER = 1
_RECONNECTION_TIMEOUT = 3

_logger = logging.getLogger('http')


class Status(Exception):

    status = None
    headers = None
    result = None


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


class Client(object):

    def __init__(self, api_url='', creds=None, trust_env=True):
        self.api_url = api_url
        self._get_profile = None
        self._session = session = Session()

        session.stream = True
        session.trust_env = trust_env
        if client.no_check_certificate.value:
            session.verify = False
        if creds:
            uid, keyfile, self._get_profile = creds
            session.headers['SUGAR_USER'] = uid
            session.headers['SUGAR_USER_SIGNATURE'] = _sign(keyfile, uid)
        session.headers['Accept-Language'] = toolkit.default_lang()

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

    def download(self, path, dst):
        response = self.request('GET', path, allow_redirects=True)
        content_length = response.headers.get('Content-Length')
        if content_length:
            chunk_size = min(int(content_length), BUFFER_SIZE)
        else:
            chunk_size = BUFFER_SIZE
        f = file(dst, 'wb') if isinstance(dst, basestring) else dst
        try:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
        finally:
            if isinstance(dst, basestring):
                f.close()

    def request(self, method, path=None, data=None, headers=None, allowed=None,
            params=None, **kwargs):
        if not path:
            path = ['']
        if not isinstance(path, basestring):
            path = '/'.join([i.strip('/') for i in [self.api_url] + path])

        while True:
            try:
                response = self._session.request(method, path, data=data,
                        headers=headers, params=params, **kwargs)
            except SSLError:
                _logger.warning('Use --no-check-certificate to avoid checks')
                raise

            if response.status_code != 200:
                if response.status_code == 401:
                    enforce(self._get_profile is not None,
                            'Operation is not available in anonymous mode')
                    _logger.info('User is not registered on the server, '
                            'registering')
                    self.post(['user'], self._get_profile())
                    continue
                if allowed and response.status_code in allowed:
                    return response
                content = response.content
                try:
                    error = json.loads(content)
                except Exception:
                    _logger.error('Request failed, '
                            'method=%s path=%r params=%r headers=%r '
                            'status_code=%s content=%s',
                            method, path, params, headers,
                            response.status_code,
                            '\n' + content if content else None)
                    response.raise_for_status()
                else:
                    for cls in _FORWARD_STATUSES:
                        if response.status_code == cls.status_code:
                            raise cls(error['error'])
                    raise RuntimeError(error['error'])

            return response

    def call(self, request, response=None):
        params = request.copy()
        method = params.pop('method')
        document = params.pop('document') if 'document' in params else None
        guid = params.pop('guid') if 'guid' in params else None
        prop = params.pop('prop') if 'prop' in params else None

        if request.path is not None:
            path = request.path
        else:
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
            if isinstance(request.content_stream, types.GeneratorType):
                request.content = ''.join([i for i in request.content_stream])
            else:
                request.content = request.content_stream.read()
            headers['Content-Length'] = str(len(request.content))

        reply = self.request(method, path, data=request.content,
                params=params, headers=headers, allowed=[303],
                allow_redirects=request.allow_redirects)

        if reply.status_code == 303:
            raise Redirect(reply.headers.get('location'))

        if response is not None:
            if 'transfer-encoding' in reply.headers:
                # `requests` library handles encoding on its own
                del reply.headers['transfer-encoding']
            response.update(reply.headers)
            """
            if 'Content-Disposition' in reply.headers:
                response['Content-Disposition'] = \
                        reply.headers['Content-Disposition']
            if 'Content-Type' in reply.headers:
                response.content_type = reply.headers['Content-Type']
            if 'Content-Length' in reply.headers:
                response.content_length = int(reply.headers['Content-Length'])
            """

        if reply.headers.get('Content-Type') == 'application/json':
            return json.loads(reply.content)
        else:
            return reply.raw

    def subscribe(self, **condition):
        return _Subscription(self, condition, _RECONNECTION_NUMBER)

    def _decode_reply(self, response):
        if response.headers.get('Content-Type') == 'application/json':
            return json.loads(response.content)
        else:
            return response.content


class _Subscription(object):

    def __init__(self, aclient, condition, tries):
        self._tries = tries or 1
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
                line = util.readline(stream)
                enforce(line, 'Subscription aborted')
                break
            except Exception:
                if a_try == 0:
                    raise
                exception('Failed to read from %r subscription, '
                        'will resubscribe', self._client.api_url)
                self._content = None

        if line.startswith('data: '):
            try:
                return json.loads(line.split(' ', 1)[1])
            except Exception:
                exception('Failed to parse %r event from %r subscription',
                        line, self._client.api_url)

    def _handshake(self, **params):
        if self._content is not None:
            return self._content
        params.update(self._condition)
        params['cmd'] = 'subscribe'

        _logger.debug('Subscribe to %r, %r', self._client.api_url, params)

        for a_try in reversed(xrange(self._tries)):
            try:
                response = self._client.request('GET', params=params)
                break
            except Exception:
                if a_try == 0:
                    raise
                exception(_logger,
                        'Cannot subscribe to %r, retry in %s second(s)',
                        self._client.api_url, _RECONNECTION_TIMEOUT)
                coroutine.sleep(_RECONNECTION_TIMEOUT)

        self._content = response.raw
        return self._content


def _sign(key_path, data):
    import hashlib
    from M2Crypto import DSA
    key = DSA.load_key(key_path)
    # pylint: disable-msg=E1121
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')


_FORWARD_STATUSES = [
        BadRequest,
        Forbidden,
        NotFound,
        ]
