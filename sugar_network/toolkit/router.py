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
import types
import logging
import calendar
import mimetypes
from bisect import bisect_left
from urllib import urlencode
from urlparse import parse_qsl, urlsplit
from email.utils import parsedate, formatdate
from os.path import isfile, split, splitext

from sugar_network import toolkit
from sugar_network.toolkit import http, coroutine, enforce


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
    PUBLIC = CREATE | WRITE | READ | DELETE

    AUTH = 1 << 6
    AUTHOR = 1 << 7
    SUPERUSER = 1 << 8

    LOCAL = 1 << 9
    CALC = 1 << 10

    NAMES = {
            CREATE: 'Create',
            WRITE: 'Write',
            READ: 'Read',
            DELETE: 'Delete',
            }


class Request(dict):

    environ = None
    url = None
    method = None
    path = None
    cmd = None
    content = None
    content_type = None
    content_length = 0
    principal = None
    _if_modified_since = None
    _accept_language = None

    def __init__(self, environ=None, method=None, path=None, cmd=None,
            **kwargs):
        dict.__init__(self)
        self._pos = 0
        self._dirty_query = False

        if environ is None:
            self.environ = {}
            self.method = method
            self.path = path
            self.cmd = cmd
            self.update(kwargs)
            return

        self.environ = environ
        self.url = '/' + environ['PATH_INFO'].strip('/')
        self.path = [i for i in self.url[1:].split('/') if i]
        self.method = environ['REQUEST_METHOD']

        enforce('..' not in self.path, 'Relative url path')

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
        if query:
            self.url += '?' + query

        content_length = self.environ.get('CONTENT_LENGTH')
        if content_length is not None:
            self.content_length = int(content_length)

        content_type, __ = cgi.parse_header(environ.get('CONTENT_TYPE', ''))
        self.content_type = content_type.lower()
        if self.content_type == 'application/json':
            self.content = json.load(environ['wsgi.input'])

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
    def content_stream(self):
        return self.environ.get('wsgi.input')

    @property
    def static_prefix(self):
        http_host = self.environ.get('HTTP_HOST')
        if http_host:
            return 'http://' + http_host

    @property
    def if_modified_since(self):
        if self._if_modified_since is None:
            value = parsedate(self.environ.get('HTTP_IF_MODIFIED_SINCE'))
            if value is not None:
                self._if_modified_since = calendar.timegm(value)
            else:
                self._if_modified_since = 0
        return self._if_modified_since

    @property
    def accept_language(self):
        if self._accept_language is None:
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

    def read(self, size=None):
        if self.content_stream is None:
            return ''
        rest = max(0, self.content_length - self._pos)
        size = rest if size is None else min(rest, size)
        result = self.content_stream.read(size)
        if not result:
            return ''
        self._pos += len(result)
        return result

    def add(self, key, *values):
        existing_value = self.get(key)
        for value in values:
            if existing_value is None:
                existing_value = self[key] = value
            elif type(existing_value) is list:
                existing_value.append(value)
            else:
                existing_value = self[key] = [existing_value, value]

    def __repr__(self):
        return '<Request method=%s path=%r cmd=%s query=%r>' % \
                (self.method, self.path, self.cmd, dict(self))


class Response(dict):

    status = '200 OK'

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
                    result.append((key, str(i)))
            else:
                result.append((key, str(value)))
        return result

    def __repr__(self):
        items = ['%s=%r' % i for i in self.items()]
        return '<Response %s>' % ' '.join(items)

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

    def __init__(self, routes_model):
        self._valid_origins = set()
        self._invalid_origins = set()
        self._host = None
        self._routes = _Routes()
        self._routes_model = routes_model
        self._preroutes = []
        self._postroutes = []

        processed = set()
        cls = type(routes_model)
        while cls is not None:
            for name in dir(cls):
                attr = getattr(cls, name)
                if name in processed:
                    continue
                if hasattr(attr, 'is_preroute'):
                    self._preroutes.append(getattr(routes_model, name))
                    continue
                elif hasattr(attr, 'is_postroute'):
                    self._postroutes.append(getattr(routes_model, name))
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
        result = None
        try:
            result = self._call(request, response)

            if isinstance(result, Blob):
                if 'url' in result:
                    raise http.Redirect(result['url'])

                path = result['blob']
                enforce(isfile(path), 'No such file')

                mtime = result.get('mtime') or os.stat(path).st_mtime
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
        finally:
            _logger.trace('%s call: request=%s response=%r result=%r',
                    self, request.environ, response, result)
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
            response.set('X-SN-%s' % str(key), json.dumps(value))

        start_response(response.status, response.items())

        if request.method == 'HEAD':
            enforce(result is None, 'HEAD responses should not contain body')
        elif result_streamed:
            for i in result:
                yield i
        elif result is not None:
            yield result

    def _call(self, request, response):
        route_ = self._resolve(request)
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
            i(route_, request)
        result = None
        exception = None
        try:
            result = route_.callback(**kwargs)
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

    def _resolve(self, request):
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


def _filename(names, mime_type):
    if type(names) not in (list, tuple):
        names = [names]
    parts = []
    for name in names:
        if isinstance(name, dict):
            name = toolkit.gettext(name)
        parts.append(''.join([i.capitalize() for i in str(name).split()]))
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
