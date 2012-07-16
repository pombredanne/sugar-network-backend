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
import types
import urllib
import logging
from urlparse import parse_qsl
from bisect import bisect_left

import active_document as ad
from sugar_network import node
from active_toolkit.sockets import BUFFER_SIZE
from active_toolkit import util, enforce


_logger = logging.getLogger('node.router')


class Router(object):

    def __init__(self, cp):
        self._cp = cp
        self._authenticated = set()

        if 'SSH_ASKPASS' in os.environ:
            # Otherwise ssh-keygen will popup auth dialogs on registeration
            del os.environ['SSH_ASKPASS']

    def __call__(self, environ, start_response):
        response = _Response()
        result = None
        try:
            request = _Request(environ)
            request.principal = self._authenticate(request)
            result = self._cp.call(request, response)
        except ad.Redirect, error:
            response.status = '303 See Other'
            response['Location'] = error.location
            result = ''
        except Exception, error:
            util.exception('Error while processing %r request',
                    environ['PATH_INFO'] or '/')

            if isinstance(error, ad.NotFound):
                response.status = '404 Not Found'
            elif isinstance(error, ad.Forbidden):
                response.status = '403 Forbidden'
            elif isinstance(error, node.HTTPStatus):
                response.status = error.status
                response.update(error.headers or {})
                result = error.result
            else:
                response.status = '500 Internal Server Error'

            if result is None:
                result = {'error': str(error),
                          'request': environ['PATH_INFO'] or '/',
                          }
                response.content_type = 'application/json'

        if hasattr(result, 'read'):
            # pylint: disable-msg=E1103
            if hasattr(result, 'fileno'):
                response.content_length = os.fstat(result.fileno()).st_size
            elif hasattr(result, 'seek'):
                result.seek(0, 2)
                response.content_length = result.tell()
                result.seek(0)
            result = _stream_reader(result)
        result_streamed = isinstance(result, types.GeneratorType)

        if not result_streamed and response.content_type == 'application/json':
            result = json.dumps(result)
            response.content_length = len(result)

        _logger.debug('Process request=%r response=%r result=%r streamed=%r',
                request, response, result, result_streamed)

        start_response(response.status, response.items())

        if result_streamed:
            for i in result:
                yield i
        elif result is not None:
            yield result

    def _authenticate(self, request):
        user = request.environ.get('HTTP_SUGAR_USER')
        if user is None:
            return None

        if user not in self._authenticated and \
                (request.path != ['user'] or request['method'] != 'POST'):
            _logger.debug('Logging %r user', user)
            request = ad.Request(method='GET', cmd='exists',
                    document='user', guid=user)
            enforce(self._cp.call(request, ad.Response()), node.Unauthorized,
                    'Principal user does not exist')
            self._authenticated.add(user)

        return user


class _Request(ad.Request):

    def __init__(self, environ):
        ad.Request.__init__(self)

        self.access_level = ad.ACCESS_REMOTE
        self.environ = environ
        path = environ['PATH_INFO'] or '/'
        __, path = urllib.splittype(path)
        __, path = urllib.splithost(path)
        self.url = path
        self.path = [i for i in path.strip('/').split('/') if i]
        self['method'] = environ['REQUEST_METHOD']
        self.content = None
        self.content_stream = environ.get('wsgi.input')
        self.content_length = 0
        self.accept_language = _parse_accept_language(
                environ.get('HTTP_ACCEPT_LANGUAGE'))
        self.principal = None

        query = environ.get('QUERY_STRING') or ''
        for attr, value in parse_qsl(query):
            param = self.get(attr)
            if type(param) is list:
                param.append(value)
            elif param is not None:
                self[str(attr)] = [param, value]
            else:
                self[str(attr)] = value
        if query:
            self.url += '?' + query

        content_length = environ.get('CONTENT_LENGTH')
        if content_length:
            self.content_length = int(content_length)
            ctype, __ = cgi.parse_header(environ.get('CONTENT_TYPE', ''))
            if ctype.lower() == 'application/json':
                content = self.read()
                if content:
                    self.content = json.loads(content)
            elif ctype.lower() == 'multipart/form-data':
                files = cgi.FieldStorage(fp=environ['wsgi.input'],
                        environ=environ)
                enforce(len(files.list) == 1,
                        'Multipart request should contain only one file')
                self.content_stream = files.list[0].file

        scope = len(self.path)
        enforce(scope >= 0 and scope < 4, node.BadRequest,
                'Incorrect requested path')
        if scope == 3:
            self['document'], self['guid'], self['prop'] = self.path
        elif scope == 2:
            self['document'], self['guid'] = self.path
        elif scope == 1:
            self['document'], = self.path


class _Response(ad.Response):

    status = '200 OK'

    def get_content_length(self):
        return self.get('Content-Length')

    def set_content_length(self, value):
        self['Content-Length'] = value

    content_length = property(get_content_length, set_content_length)

    def get_content_type(self):
        return self.get('Content-Type')

    def set_content_type(self, value):
        self['Content-Type'] = value

    content_type = property(get_content_type, set_content_type)

    def items(self):
        for key, value in dict.items(self):
            if type(value) in (list, tuple):
                for i in value:
                    yield key, i
            else:
                yield key, value


def _parse_accept_language(accept_language):
    if not accept_language:
        return []

    langs = []
    qualities = []

    for chunk in accept_language.split(','):
        lang, params = (chunk.split(';', 1) + [None])[:2]
        lang = lang.strip()
        if not lang:
            continue

        quality = 1
        if params:
            params = params.split('=', 1)
            if len(params) > 1 and params[0].strip() == 'q':
                quality = float(params[1])

        index = bisect_left(qualities, quality)
        qualities.insert(index, quality)
        langs.insert(len(langs) - index, lang)

    return langs


def _stream_reader(stream):
    try:
        while True:
            chunk = stream.read(BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        stream.close()
