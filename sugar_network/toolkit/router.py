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
import cgi
import json
import time
import types
import logging
import mimetypes
from email.utils import parsedate, formatdate
from urlparse import parse_qsl, urlsplit
from bisect import bisect_left
from os.path import join, isfile, split, splitext

from sugar_network import db, static
from sugar_network.toolkit import BUFFER_SIZE
from sugar_network.toolkit import sugar, coroutine, exception, enforce


_logger = logging.getLogger('router')


class HTTPStatus(Exception):

    status = None
    headers = None
    result = None


class HTTPStatusPass(HTTPStatus):
    pass


class NotModified(HTTPStatusPass):

    status = '304 Not Modified'


class Redirect(HTTPStatusPass):

    status = '303 See Other'

    def __init__(self, location):
        HTTPStatus.__init__(self)
        self.headers = {'Location': location}


class BadRequest(HTTPStatus):

    status = '400 Bad Request'


class Unauthorized(HTTPStatus):

    status = '401 Unauthorized'
    headers = {'WWW-Authenticate': 'Sugar'}


def route(method, path):
    path = path.strip('/').split('/')
    # Only top level paths for now
    enforce(len(path) == 1)

    def decorate(func):
        func.route = (method, path[0])
        return func

    return decorate


def stream_reader(stream):
    try:
        while True:
            chunk = stream.read(BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        if hasattr(stream, 'close'):
            stream.close()


class Request(db.Request):

    principal = None
    mountpoint = None
    if_modified_since = None
    allow_redirects = False


class Router(object):

    def __init__(self, commands):
        self.commands = commands
        self._authenticated = set()
        self._valid_origins = set()
        self._invalid_origins = set()
        self._host = None
        self._routes = {}

        self._scan_for_routes(commands)
        self._scan_for_routes(self)

        if 'SSH_ASKPASS' in os.environ:
            # Otherwise ssh-keygen will popup auth dialogs on registeration
            del os.environ['SSH_ASKPASS']

    def authenticate(self, request):
        user = request.environ.get('HTTP_SUGAR_USER')
        if user is None:
            return None

        if user not in self._authenticated and \
                (request.path != ['user'] or request['method'] != 'POST'):
            _logger.debug('Logging %r user', user)
            request = Request(method='GET', cmd='exists',
                    document='user', guid=user)
            enforce(self.commands.call(request, db.Response()), Unauthorized,
                    'Principal user does not exist')
            self._authenticated.add(user)

        return user

    def call(self, request, response):
        if 'HTTP_ORIGIN' in request.environ:
            enforce(self._assert_origin(request.environ), db.Forbidden,
                    'Cross-site is not allowed for %r origin',
                    request.environ['HTTP_ORIGIN'])
            response['Access-Control-Allow-Origin'] = \
                    request.environ['HTTP_ORIGIN']

        if request['method'] == 'OPTIONS':
            # TODO Process OPTIONS request per url?
            if request.environ['HTTP_ORIGIN']:
                response['Access-Control-Allow-Methods'] = \
                        request.environ['HTTP_ACCESS_CONTROL_REQUEST_METHOD']
                response['Access-Control-Allow-Headers'] = \
                        request.environ['HTTP_ACCESS_CONTROL_REQUEST_HEADERS']
            else:
                response['Allow'] = 'GET, POST, PUT, DELETE'
            response.content_length = 0
            return None

        request.principal = self.authenticate(request)
        if request.path[:1] == ['static']:
            path = join(static.PATH, *request.path[1:])
            result = db.PropertyMetadata(blob=path,
                    mime_type=_get_mime_type(path), filename=split(path)[-1])
        else:
            rout = self._routes.get((
                request['method'],
                request.path[0] if request.path else ''))
            if rout:
                result = rout(request, response)
            else:
                result = self.commands.call(request, response)

        if isinstance(result, db.PropertyMetadata):
            if 'url' in result:
                raise Redirect(result['url'])

            path = result['blob']
            enforce(isfile(path), 'No such file')

            mtime = result.get('mtime') or os.stat(path).st_mtime
            if request.if_modified_since and mtime and \
                    mtime <= request.if_modified_since:
                raise NotModified()
            response.last_modified = mtime

            response.content_type = result.get('mime_type') or \
                    'application/octet-stream'

            filename = result.get('filename')
            if not filename:
                filename = _filename(result.get('name') or
                        splitext(split(path)[-1])[0],
                    response.content_type)
            response['Content-Disposition'] = \
                    'attachment; filename="%s"' % filename

            result = file(path, 'rb')

        if hasattr(result, 'read'):
            if hasattr(result, 'fileno'):
                response.content_length = os.fstat(result.fileno()).st_size
            elif hasattr(result, 'seek'):
                result.seek(0, 2)
                response.content_length = result.tell()
                result.seek(0)
            result = stream_reader(result)

        return result

    def __call__(self, environ, start_response):
        request = _Request(environ)
        request_repr = str(request) if _logger.level <= logging.DEBUG else None
        response = _Response()

        js_callback = None
        if 'callback' in request:
            js_callback = request.pop('callback')

        result = None
        try:
            result = self.call(request, response)
        except HTTPStatusPass, error:
            response.status = error.status
            if error.headers:
                response.update(error.headers)
            response.content_type = None
        except Exception, error:
            exception('Error while processing %r request', request.url)

            if isinstance(error, db.NotFound):
                response.status = '404 Not Found'
            elif isinstance(error, db.Forbidden):
                response.status = '403 Forbidden'
            elif isinstance(error, HTTPStatus):
                response.status = error.status
                response.update(error.headers or {})
                result = error.result
            else:
                response.status = '500 Internal Server Error'

            if result is None:
                result = {'error': str(error),
                          'request': request.url,
                          }
                response.content_type = 'application/json'

        result_streamed = isinstance(result, types.GeneratorType)

        if js_callback:
            if result_streamed:
                result = ''.join(result)
                result_streamed = False
            result = '%s(%s);' % (js_callback, json.dumps(result))
            response.content_length = len(result)
        elif not result_streamed:
            if response.content_type == 'application/json':
                result = json.dumps(result)
            response.content_length = len(result) if result else 0

        _logger.trace('Called %s: response=%r result=%r streamed=%r',
                request_repr, response, result, result_streamed)

        start_response(response.status, response.items())

        if result_streamed:
            for i in result:
                yield i
        elif result is not None:
            yield result

    def _assert_origin(self, environ):
        origin = environ['HTTP_ORIGIN']
        if origin in self._valid_origins:
            return True
        if origin in self._invalid_origins:
            return False

        valid = True
        if origin == 'null' or origin.startswith('file://'):
            # True all time for local apps
            pass
        else:
            if self._host is None:
                http_host = environ['HTTP_HOST'].split(':', 1)[0]
                self._host = coroutine.gethostbyname(http_host)
            ip = coroutine.gethostbyname(urlsplit(origin).hostname)
            valid = (self._host == ip)

        if valid:
            _logger.info('Allow cross-site for %r origin', origin)
            self._valid_origins.add(origin)
        else:
            _logger.info('Disallow cross-site for %r origin', origin)
            self._invalid_origins.add(origin)
        return valid

    def _scan_for_routes(self, obj):
        cls = obj.__class__
        while cls is not None:
            for name in dir(cls):
                attr = getattr(obj, name)
                if hasattr(attr, 'route'):
                    self._routes[attr.route] = attr
            # pylint: disable-msg=E1101
            cls = cls.__base__


class IPCRouter(Router):

    def authenticate(self, request):
        return sugar.uid()

    def call(self, request, response):
        request.access_level = db.ACCESS_LOCAL
        return Router.call(self, request, response)


class _Request(Request):

    environ = None
    url = None
    path = None

    def __init__(self, environ=None):
        Request.__init__(self)

        if not environ:
            return

        self.access_level = db.ACCESS_REMOTE
        self.environ = environ
        self.url = '/' + environ['PATH_INFO'].strip('/')
        self.path = [i for i in self.url[1:].split('/') if i]
        self['method'] = environ['REQUEST_METHOD']
        self.content = None
        self.content_stream = environ.get('wsgi.input')
        self.content_length = 0
        self.accept_language = _parse_accept_language(
                environ.get('HTTP_ACCEPT_LANGUAGE'))
        self.principal = None
        self.query = {}

        enforce('..' not in self.path, 'Relative url path')

        query = environ.get('QUERY_STRING') or ''
        for attr, value in parse_qsl(query):
            attr = str(attr)
            param = self.get(attr)
            if type(param) is list:
                param.append(value)
            else:
                if param is not None:
                    value = [param, value]
                self[attr] = value
                if attr != 'cmd':
                    self.query[attr] = value
        if query:
            self.url += '?' + query

        content_length = environ.get('CONTENT_LENGTH')
        if content_length:
            self.content_length = int(content_length)
            self.content_type, __ = \
                    cgi.parse_header(environ.get('CONTENT_TYPE', ''))
            if self.content_type.lower() == 'application/json':
                self.content = json.load(self.content_stream)
            elif self.content_type.lower() == 'multipart/form-data':
                files = cgi.FieldStorage(fp=environ['wsgi.input'],
                        environ=environ)
                enforce(len(files.list) == 1,
                        'Multipart request should contain only one file')
                self.content_stream = files.list[0].file

        if_modified_since = environ.get('HTTP_IF_MODIFIED_SINCE')
        if if_modified_since:
            if_modified_since = parsedate(if_modified_since)
            enforce(if_modified_since is not None,
                    'Failed to parse If-Modified-Since')
            self.if_modified_since = time.mktime(if_modified_since)

        scope = len(self.path)
        if scope == 3:
            self['document'], self['guid'], self['prop'] = self.path
        elif scope == 2:
            self['document'], self['guid'] = self.path
        elif scope == 1:
            self['document'], = self.path

        self.sync = bool(environ.get('HTTP_SUGAR_SYNC'))

    def clone(self):
        request = Request.clone(self)
        request.environ = self.environ
        request.url = self.url
        request.path = self.path
        request.principal = self.principal
        return request


class _Response(db.Response):
    # pylint: disable-msg=E0202

    status = '200 OK'

    @property
    def content_length(self):
        return int(self.get('Content-Length') or '0')

    @content_length.setter
    def content_length(self, value):
        self['Content-Length'] = str(value)

    @property
    def content_type(self):
        return self.get('Content-Type')

    @content_type.setter
    def content_type(self, value):
        if value:
            self['Content-Type'] = value
        elif 'Content-Type' in self:
            del self['Content-Type']

    @property
    def last_modified(self):
        return self.get('Last-Modified')

    @last_modified.setter
    def last_modified(self, value):
        self['Last-Modified'] = formatdate(value, localtime=False, usegmt=True)

    def items(self):
        result = []
        for key, value in dict.items(self):
            if type(value) in (list, tuple):
                for i in value:
                    result.append((key, str(i)))
            else:
                result.append((key, str(value)))
        return result

    def __repr__(self):
        args = ['status=%r' % self.status,
                ] + ['%s=%r' % i for i in self.items()]
        return '<db.Response %s>' % ' '.join(args)


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
        langs.insert(len(langs) - index, lang.lower().replace('_', '-'))

    return langs


def _get_mime_type(path):
    if not mimetypes.inited:
        mimetypes.init()
    suffix = '.' + path.rsplit('.', 1)[-1]
    return mimetypes.types_map.get(suffix)


def _filename(names, mime_type):
    if type(names) not in (list, tuple):
        names = [names]
    parts = []
    for name in names:
        if isinstance(name, dict):
            name = db.gettext(name)
        parts.append(''.join([i.capitalize() for i in str(name).split()]))
    result = '-'.join(parts)
    if mime_type:
        if not mimetypes.inited:
            mimetypes.init()
        result += mimetypes.guess_extension(mime_type) or ''
    return result.replace(os.sep, '')
