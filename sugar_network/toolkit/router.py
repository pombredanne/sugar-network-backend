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
import calendar
import mimetypes
from base64 import b64decode
from bisect import bisect_left
from urllib import urlencode
from urlparse import parse_qsl, urlsplit
from email.utils import parsedate, formatdate
from os.path import isfile, split, splitext

from sugar_network import toolkit
from sugar_network.toolkit import http, coroutine, enforce


_SIGNATURE_LIFETIME = 600
_NOT_SET = object()

_logger = logging.getLogger('router')


def route(method, path=None, cmd=None, **kwargs):
    if path is None:
        path = []
    enforce(method, 'Method should not be empty')

    def decorate(func):
        func.route = (False, method, path, cmd, kwargs)
        return func

    return decorate


def fallbackroute(method=None, path=None, **kwargs):
    if path is None:
        path = []
    enforce(not [i for i in path if i is None],
            'Wildcards is not allowed for fallbackroute')

    def decorate(func):
        func.route = (True, method, path, None, kwargs)
        return func

    return decorate


def preroute(func):
    func.is_preroute = True
    return func


def postroute(func):
    func.is_postroute = True
    return func


class ACL(object):

    INSYSTEM = 1 << 0
    ORIGINAL = 1 << 1

    CREATE = 1 << 2
    WRITE = 1 << 3
    READ = 1 << 4
    DELETE = 1 << 5
    INSERT = 1 << 6
    REMOVE = 1 << 7
    PUBLIC = CREATE | WRITE | READ | DELETE | INSERT | REMOVE

    AUTH = 1 << 8
    AUTHOR = 1 << 9
    SUPERUSER = 1 << 10

    LOCAL = 1 << 11
    CALC = 1 << 12

    NAMES = {
            CREATE: 'Create',
            WRITE: 'Write',
            READ: 'Read',
            DELETE: 'Delete',
            INSERT: 'Insert',
            REMOVE: 'Remove',
            }


class Unauthorized(http.Unauthorized):

    def __init__(self, message, nonce=None):
        http.Unauthorized.__init__(self, message)
        if not nonce:
            nonce = int(time.time()) + _SIGNATURE_LIFETIME
        self.headers = {'www-authenticate': 'Sugar nonce="%s"' % nonce}


class Request(dict):

    principal = None
    subcall = lambda *args: enforce(False)

    def __init__(self, environ=None, method=None, path=None, cmd=None,
            content=None, content_stream=None, content_type=None, session=None,
            **kwargs):
        dict.__init__(self)

        self.path = []
        self.cmd = None
        self.environ = {}
        self.session = session or {}

        self._content = _NOT_SET
        self._dirty_query = False
        self._if_modified_since = _NOT_SET
        self._accept_language = _NOT_SET
        self._content_stream = content_stream or _NOT_SET
        self._content_type = content_type or _NOT_SET
        self._authorization = _NOT_SET

        if environ:
            url = environ.get('PATH_INFO', '').strip('/')
            self.path = [i for i in url.split('/') if i]
            query = environ.get('QUERY_STRING') or ''
            for key, value in parse_qsl(query, keep_blank_values=True):
                key = str(key)
                param = self.get(key)
                if type(param) is list:
                    param.append(value)
                else:
                    if param is not None:
                        value = [param, value]
                    if key == 'cmd':
                        self.cmd = value
                    else:
                        dict.__setitem__(self, key, value)
            self.environ = environ

        if method:
            self.environ['REQUEST_METHOD'] = method
        if path:
            self.environ['PATH_INFO'] = '/' + '/'.join(path)
            self.path = path
        if cmd:
            self.cmd = cmd
            self._dirty_query = True
        if content is not None:
            self._content = content
        if kwargs:
            self.update(kwargs)
            self._dirty_query = True

        enforce('..' not in self.path, 'Relative url path')

    def __setitem__(self, key, value):
        self._dirty_query = True
        if key == 'cmd':
            self.cmd = value
        else:
            dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        enforce(key in self, 'Cannot find %r request argument', key)
        return self.get(key)

    @property
    def method(self):
        return self.environ.get('REQUEST_METHOD')

    @property
    def url(self):
        result = self.environ['PATH_INFO']
        if self.query:
            result += '?' + self.query
        return result

    @property
    def content_type(self):
        if self._content_type is _NOT_SET:
            value, __ = cgi.parse_header(
                    self.environ.get('CONTENT_TYPE', ''))
            self._content_type = value.lower()
        return self._content_type

    @content_type.setter
    def content_type(self, value):
        self._content_type = value

    @property
    def content(self):
        self.ensure_content()
        return self._content

    @content.setter
    def content(self, value):
        self._content = value

    @property
    def content_length(self):
        value = self.environ.get('CONTENT_LENGTH')
        if value is not None:
            return int(value)

    @content_length.setter
    def content_length(self, value):
        self.environ['CONTENT_LENGTH'] = str(value)

    @property
    def content_stream(self):
        if self._content_stream is _NOT_SET:
            s = self.environ.get('wsgi.input')
            if s is None:
                self._content_stream = None
            else:
                self._content_stream = _ContentStream(s, self.content_length)
        return self._content_stream

    @content_stream.setter
    def content_stream(self, value):
        self._content_stream = value

    @property
    def resource(self):
        if self.path:
            return self.path[0]

    @property
    def guid(self):
        if len(self.path) > 1:
            return self.path[1]

    @property
    def prop(self):
        if len(self.path) > 2:
            return self.path[2]

    @property
    def static_prefix(self):
        http_host = self.environ.get('HTTP_HOST')
        if http_host:
            return 'http://' + http_host

    @property
    def if_modified_since(self):
        if self._if_modified_since is _NOT_SET:
            value = parsedate(self.environ.get('HTTP_IF_MODIFIED_SINCE'))
            if value is not None:
                self._if_modified_since = calendar.timegm(value)
            else:
                self._if_modified_since = 0
        return self._if_modified_since

    @property
    def accept_language(self):
        if self._accept_language is _NOT_SET:
            self._accept_language = _parse_accept_language(
                    self.environ.get('HTTP_ACCEPT_LANGUAGE'))
        return self._accept_language

    @property
    def accept_encoding(self):
        return self.environ.get('HTTP_ACCEPT_ENCODING')

    @accept_encoding.setter
    def accept_encoding(self, value):
        self.environ['HTTP_ACCEPT_ENCODING'] = value

    @property
    def query(self):
        if self._dirty_query:
            if self.cmd:
                query = self.copy()
                query['cmd'] = self.cmd
            else:
                query = self
            self.environ['QUERY_STRING'] = urlencode(query, doseq=True)
            self._dirty_query = False
        return self.environ.get('QUERY_STRING')

    @property
    def authorization(self):
        if self._authorization is _NOT_SET:
            auth = self.environ.get('HTTP_AUTHORIZATION')
            if not auth:
                self._authorization = None
            else:
                auth = self._authorization = _Authorization(auth)
                auth.scheme, creds = auth.strip().split(' ', 1)
                auth.scheme = auth.scheme.lower()
                if auth.scheme == 'basic':
                    auth.login, auth.password = b64decode(creds).split(':')
                elif auth.scheme == 'sugar':
                    from urllib2 import parse_http_list, parse_keqv_list
                    creds = parse_keqv_list(parse_http_list(creds))
                    auth.login = creds['username']
                    auth.signature = creds['signature']
                    auth.nonce = int(creds['nonce'])
                else:
                    raise http.BadRequest('Unsupported authentication scheme')
        return self._authorization

    def add(self, key, *values):
        existing_value = self.get(key)
        for value in values:
            if existing_value is None:
                existing_value = self[key] = value
            elif type(existing_value) is list:
                existing_value.append(value)
            else:
                existing_value = self[key] = [existing_value, value]

    def call(self, response=None, **kwargs):
        environ = {}
        for key in ('HTTP_HOST',
                    'HTTP_ACCEPT_LANGUAGE',
                    'HTTP_ACCEPT_ENCODING',
                    'HTTP_IF_MODIFIED_SINCE',
                    'HTTP_AUTHORIZATION',
                    ):
            if key in self.environ:
                environ[key] = self.environ[key]
        request = Request(environ, **kwargs)
        if response is None:
            response = Response()
        request.principal = self.principal
        request.subcall = self.subcall
        return self.subcall(request, response)

    def ensure_content(self):
        if self._content is not _NOT_SET:
            return
        if self.content_stream is None:
            self._content = None
        elif self.content_type == 'application/json':
            self._content = json.load(self.content_stream)
        else:
            self._content = self.content_stream.read()

    def __repr__(self):
        return '<Request method=%s path=%r cmd=%s query=%r>' % \
                (self.method, self.path, self.cmd, dict(self))


class Response(dict):

    status = '200 OK'
    relocations = 0

    def __init__(self, **kwargs):
        dict.__init__(self, kwargs)
        self.meta = {}

    @property
    def content_length(self):
        return int(self.get('content-length') or '0')

    @content_length.setter
    def content_length(self, value):
        self.set('content-length', value)

    @property
    def content_type(self):
        return self.get('content-type')

    @content_type.setter
    def content_type(self, value):
        if value:
            self.set('content-type', value)
        elif 'content-type' in self:
            self.remove('content-type')

    @property
    def last_modified(self):
        return self.get('last-modified')

    @last_modified.setter
    def last_modified(self, value):
        self.set('last-modified',
                formatdate(value, localtime=False, usegmt=True))

    def items(self):
        result = []
        for key, value in dict.items(self):
            if type(value) in (list, tuple):
                for i in value:
                    result.append((_to_ascii(key), _to_ascii(i)))
            else:
                result.append((_to_ascii(key), _to_ascii(value)))
        return result

    def __repr__(self):
        items = ['%s=%r' % i for i in self.items() + self.meta.items()]
        return '<Response %r>' % items

    def __contains__(self, key):
        dict.__contains__(self, key.lower())

    def __getitem__(self, key):
        return self.get(key.lower())

    def __setitem__(self, key, value):
        return self.set(key.lower(), value)

    def __delitem__(self, key, value):
        self.remove(key.lower())

    def set(self, key, value):
        dict.__setitem__(self, key, value)

    def remove(self, key):
        dict.__delitem__(self, key)


class Blob(dict):
    pass


class Router(object):

    def __init__(self, routes_model, allow_spawn=False):
        self._routes_model = routes_model
        self._allow_spawn = allow_spawn
        self._valid_origins = set()
        self._invalid_origins = set()
        self._host = None
        self._routes = _Routes()
        self._preroutes = set()
        self._postroutes = set()

        processed = set()
        cls = type(routes_model)
        while cls is not None:
            for name in dir(cls):
                attr = getattr(cls, name)
                if name in processed:
                    continue
                if hasattr(attr, 'is_preroute'):
                    self._preroutes.add(getattr(routes_model, name))
                    continue
                elif hasattr(attr, 'is_postroute'):
                    self._postroutes.add(getattr(routes_model, name))
                    continue
                elif not hasattr(attr, 'route'):
                    continue
                fallback, method, path, cmd, kwargs = attr.route
                routes = self._routes
                for i, part in enumerate(path):
                    enforce(i == 0 or not routes.fallback_ops or
                            (fallback and i == len(path) - 1),
                            'Fallback route should not have sub-routes')
                    if part is None:
                        enforce(not fallback, 'Fallback route with wildcards')
                        if routes.wildcards is None:
                            routes.wildcards = _Routes(routes.parent)
                        routes = routes.wildcards
                    else:
                        routes = routes.setdefault(part, _Routes(routes))
                ops = routes.fallback_ops if fallback else routes.ops
                route_ = _Route(getattr(routes_model, name), method, path, cmd,
                        **kwargs)
                enforce(route_.op not in ops, 'Route %s already exists',
                        route_)
                ops[route_.op] = route_
                processed.add(name)
            cls = cls.__base__

    def call(self, request, response):
        request.subcall = self.call
        result = self._call_route(request, response)

        if isinstance(result, Blob):
            if 'url' in result:
                raise http.Redirect(result['url'])

            path = result['blob']
            enforce(isfile(path), 'No such file')

            mtime = result.get('mtime') or int(os.stat(path).st_mtime)
            if request.if_modified_since and mtime and \
                    mtime <= request.if_modified_since:
                raise http.NotModified()
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
            result = _stream_reader(result)

        return result

    def __repr__(self):
        return '<Router %s>' % type(self._routes_model).__name__

    def __call__(self, environ, start_response):
        request = Request(environ)
        response = Response()

        js_callback = None
        if 'callback' in request:
            js_callback = request.pop('callback')

        result = None
        try:
            if 'HTTP_ORIGIN' in request.environ:
                enforce(self._assert_origin(request.environ), http.Forbidden,
                        'Cross-site is not allowed for %r origin',
                        request.environ['HTTP_ORIGIN'])
                response['Access-Control-Allow-Origin'] = \
                        request.environ['HTTP_ORIGIN']
            result = self.call(request, response)
        except http.StatusPass, error:
            response.status = error.status
            if error.headers:
                response.update(error.headers)
            response.content_type = None
        except Exception, error:
            toolkit.exception('Error while processing %r request', request.url)
            if isinstance(error, http.Status):
                response.status = error.status
                response.update(error.headers or {})
            else:
                response.status = '500 Internal Server Error'
            if request.method == 'HEAD':
                response.meta['error'] = str(error)
            else:
                result = {'error': str(error),
                          'request': request.url,
                          }
                response.content_type = 'application/json'

        result_streamed = isinstance(result, types.GeneratorType)

        if request.method == 'HEAD':
            result_streamed = False
            result = None
        elif js_callback:
            if result_streamed:
                result = ''.join(result)
                result_streamed = False
            result = '%s(%s);' % (js_callback, json.dumps(result))
            response.content_length = len(result)
        elif not result_streamed:
            if response.content_type == 'application/json':
                result = json.dumps(result)
            if 'content-length' not in response:
                response.content_length = len(result) if result else 0

        for key, value in response.meta.items():
            response.set('X-SN-%s' % _to_ascii(key), json.dumps(value))

        if request.method == 'HEAD' and result is not None:
            _logger.warning('Content from HEAD response is ignored')
            result = None

        _logger.trace('%s call: request=%s response=%r result=%r',
                self, request.environ, response, repr(result)[:256])
        start_response(response.status, response.items())

        if result_streamed:
            if response.content_type == 'text/event-stream':
                for event in _event_stream(request, result):
                    yield 'data: %s\n\n' % json.dumps(event)
            else:
                for i in result:
                    yield i
        elif result is not None:
            yield result

    def _call_route(self, request, response):
        route_ = self._resolve_route(request)
        request.routes = self._routes_model

        for arg, cast in route_.arguments.items():
            value = request.get(arg)
            if value is None:
                if not hasattr(cast, '__call__'):
                    request[arg] = cast
                continue
            if not hasattr(cast, '__call__'):
                cast = type(cast)
            try:
                request[arg] = _typecast(cast, value)
            except Exception, error:
                raise http.BadRequest(
                        'Cannot typecast %r argument: %s' % (arg, error))
        kwargs = {}
        for arg in route_.kwarg_names:
            if arg == 'request':
                kwargs[arg] = request
            elif arg == 'response':
                kwargs[arg] = response
            elif arg not in kwargs:
                kwargs[arg] = request.get(arg)

        for i in self._preroutes:
            i(route_, request, response)
        result = None
        exception = None
        try:
            result = route_.callback(**kwargs)
            if route_.mime_type == 'text/event-stream' and \
                    self._allow_spawn and 'spawn' in request:
                _logger.debug('Spawn event stream for %r', request)
                request.ensure_content()
                coroutine.spawn(self._event_stream, request, result)
                result = None
        except Exception, exception:
            raise
        else:
            if not response.content_type:
                if isinstance(result, Blob):
                    response.content_type = result.get('mime_type')
                if not response.content_type:
                    response.content_type = route_.mime_type
        finally:
            for i in self._postroutes:
                i(request, response, result, exception)

        return result

    def _resolve_route(self, request):
        found_path = [False]

        def resolve_path(routes, path):
            if not path:
                if routes.ops:
                    found_path[0] = True
                return routes.ops.get((request.method, request.cmd)) or \
                       routes.fallback_ops.get((request.method, None)) or \
                       routes.fallback_ops.get((None, None))
            subroutes = routes.get(path[0])
            if subroutes is None:
                route_ = routes.fallback_ops.get((request.method, None)) or \
                        routes.fallback_ops.get((None, None))
                if route_ is not None:
                    return route_
            for subroutes in (subroutes, routes.wildcards):
                if subroutes is None:
                    continue
                route_ = resolve_path(subroutes, path[1:])
                if route_ is not None:
                    return route_

        route_ = resolve_path(self._routes, request.path) or \
                self._routes.fallback_ops.get((request.method, None)) or \
                self._routes.fallback_ops.get((None, None))
        if route_ is None:
            if found_path[0]:
                raise http.BadRequest('No such operation')
            else:
                raise http.NotFound('Path not found')
        return route_

    def _event_stream(self, request, stream):
        commons = {'method': request.method}
        if request.cmd:
            commons['cmd'] = request.cmd
        if request.resource:
            commons['resource'] = request.resource
        if request.guid:
            commons['guid'] = request.guid
        if request.prop:
            commons['prop'] = request.prop
        for event in _event_stream(request, stream):
            event.update(commons)
            self._routes_model.broadcast(event)

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
            _logger.info('%s allow cross-site for %r origin', self, origin)
            self._valid_origins.add(origin)
        else:
            _logger.info('%s disallow cross-site for %r origin', self, origin)
            self._invalid_origins.add(origin)
        return valid


class _ContentStream(object):

    def __init__(self, stream, length):
        self._stream = stream
        self._length = length
        self._pos = 0

    def fileno(self):
        return self._stream.rfile.fileno()

    def read(self, size=None):
        if self._length:
            the_rest = max(0, self._length - self._pos)
            size = the_rest if size is None else min(the_rest, size)
        result = self._stream.read(size)
        if not result:
            return ''
        self._pos += len(result)
        return result


def _filename(names, mime_type):
    if type(names) not in (list, tuple):
        names = [names]
    parts = []
    for name in names:
        if isinstance(name, dict):
            name = toolkit.gettext(name)
        parts.append(''.join([i.capitalize() for i in name.split()]))
    result = '-'.join(parts)
    if mime_type:
        if not mimetypes.inited:
            mimetypes.init()
        result += mimetypes.guess_extension(mime_type) or ''
    return result.replace(os.sep, '')


def _stream_reader(stream):
    try:
        while True:
            chunk = stream.read(toolkit.BUFFER_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        if hasattr(stream, 'close'):
            stream.close()


def _event_stream(request, stream):
    try:
        for event in stream:
            if type(event) is tuple:
                for i in event[1:]:
                    event[0].update(i)
                event = event[0]
            yield event
    except Exception, error:
        _logger.exception('Event stream %r failed', request)
        event = {'event': 'failure',
                 'exception': type(error).__name__,
                 'error': str(error),
                 }
        event.update(request.session)
        yield event
    _logger.debug('Event stream %r exited', request)


def _typecast(cast, value):
    if cast is list or cast is tuple:
        if isinstance(value, basestring):
            if value:
                return value.split(',')
            else:
                return ()
        return list(value)
    if isinstance(value, (list, tuple)):
        value = value[-1]
    if cast is int:
        if isinstance(value, basestring) and not value:
            return 0
        return int(value)
    if cast is bool:
        if isinstance(value, basestring):
            return value.strip().lower() in ('true', '1', 'on', '')
        return bool(value)
    return cast(value)


def _parse_accept_language(value):
    if not value:
        return [toolkit.default_lang()]
    langs = []
    qualities = []
    for chunk in value.split(','):
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


def _to_ascii(value):
    if not isinstance(value, basestring):
        return str(value)
    if isinstance(value, unicode):
        return value.encode('utf8')
    return value


class _Routes(dict):

    def __init__(self, parent=None):
        dict.__init__(self)
        self.parent = parent
        self.wildcards = None
        self.ops = {}
        self.fallback_ops = {}


class _Route(object):

    def __init__(self, callback, method, path, cmd, mime_type=None, acl=0,
            arguments=None):
        self.op = (method, cmd)
        self.callback = callback
        self.method = method
        self.path = path
        self.cmd = cmd
        self.mime_type = mime_type
        self.acl = acl
        self.arguments = arguments or {}
        self.kwarg_names = []

        if hasattr(callback, 'im_func'):
            callback = callback.im_func
        if hasattr(callback, 'func_code'):
            code = callback.func_code
            # `1:` is for skipping the first, `self` or `cls`, argument
            self.kwarg_names = code.co_varnames[1:code.co_argcount]

    def __repr__(self):
        path = '/'.join(['*' if i is None else i for i in self.path])
        if self.cmd:
            path += ('?cmd=%s' % self.cmd)
        return '%s /%s (%s)' % (self.method, path, self.callback.__name__)


class _Authorization(str):
    scheme = None
    login = None
    password = None
    signature = None
    nonce = None
