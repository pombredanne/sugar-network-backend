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
from base64 import b64decode, b64encode
from bisect import bisect_left
from urllib import urlencode
from Cookie import SimpleCookie
from urlparse import parse_qsl, urlsplit
from email.utils import parsedate, formatdate
from os.path import isfile, basename, exists

from sugar_network import toolkit
from sugar_network.toolkit.coroutine import this
from sugar_network.toolkit import i18n, http, coroutine, enforce


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
    REPLACE = 1 << 8
    PUBLIC = CREATE | WRITE | READ | DELETE | INSERT | REMOVE

    AUTH = 1 << 10
    AUTHOR = 1 << 11
    AGG_AUTHOR = 1 << 12

    LOCAL = 1 << 14

    NAMES = {
            CREATE: 'Create',
            WRITE: 'Write',
            READ: 'Read',
            DELETE: 'Delete',
            INSERT: 'Insert',
            REMOVE: 'Remove',
            REPLACE: 'Replace',
            }


class Request(dict):

    def __init__(self, environ=None, method=None, path=None, cmd=None,
            content=None, content_type=None, principal=None, **kwargs):
        dict.__init__(self)

        self.path = []
        self.cmd = None
        self.environ = {}
        self.principal = principal

        self._content = _NOT_SET
        self._dirty_query = False
        self._if_modified_since = _NOT_SET
        self._accept_language = _NOT_SET
        self._content_type = content_type or _NOT_SET

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
        self.headers = _RequestHeaders(self.environ)

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
        if self._content is not _NOT_SET:
            return self._content
        stream = self.environ.get('wsgi.input')
        if stream is None:
            self._content = None
        else:
            stream = _ContentStream(stream, self.content_length)
            if self.content_type == 'application/json':
                self._content = json.load(stream)
            else:
                self._content = stream
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
    def resource(self):
        if self.path:
            return self.path[0]

    @resource.setter
    def resource(self, value):
        self.path[0] = value

    @property
    def guid(self):
        if len(self.path) > 1:
            return self.path[1]

    @guid.setter
    def guid(self, value):
        self.path[1] = value

    @property
    def prop(self):
        if len(self.path) > 2:
            return self.path[2]

    @prop.setter
    def prop(self, value):
        self.path[2] = value

    @property
    def key(self):
        if len(self.path) > 3:
            return self.path[3]

    @key.setter
    def key(self, value):
        self.path[3] = value

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


class Response(toolkit.CaseInsensitiveDict):

    status = '200 OK'
    relocations = 0

    def __init__(self):
        toolkit.CaseInsensitiveDict.__init__(self)
        self.headers = _ResponseHeaders(self)

    @property
    def content_length(self):
        return int(self.get('content-length') or '0')

    @content_length.setter
    def content_length(self, value):
        self.set('content-length', str(value))

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
                    result.append((toolkit.ascii(key), toolkit.ascii(i)))
            else:
                result.append((toolkit.ascii(key), toolkit.ascii(value)))
        return result

    def __repr__(self):
        items = ['%s=%r' % i for i in self.items()]
        return '<Response %r>' % items


class File(str):

    AWAY = None

    class Digest(str):
        pass

    def __new__(cls, path=None, digest=None, meta=None):
        meta = toolkit.CaseInsensitiveDict(meta or [])

        url = ''
        if meta:
            url = meta.get('location')
        if not url and digest:
            url = '%s/blobs/%s' % (this.request.static_prefix, digest)
        self = str.__new__(cls, url)

        self.meta = meta
        self.path = path
        self.digest = File.Digest(digest) if digest else None
        self.stat = None

        return self

    @property
    def exists(self):
        return self.path and exists(self.path)

    @property
    def size(self):
        if self.stat is None:
            if not self.exists:
                size = self.meta.get('content-length', 0)
                return int(size) if size else 0
            self.stat = os.stat(self.path)
        return self.stat.st_size

    @property
    def mtime(self):
        if self.stat is None:
            self.stat = os.stat(self.path)
        return int(self.stat.st_mtime)

    @property
    def name(self):
        if self.path:
            return basename(self.path)

    def iter_content(self):
        if self.path:
            return self._iter_content()
        url = self.meta.get('location')
        enforce(url, http.NotFound, 'No location')
        blob = this.http.request('GET', url, allow_redirects=True,
                # Request for uncompressed data
                headers={'accept-encoding': ''})
        self.meta.clear()
        for tag in ('content-length', 'content-type', 'content-disposition'):
            value = blob.headers.get(tag)
            if value:
                self.meta[tag] = value
        return blob.iter_content(toolkit.BUFFER_SIZE)

    def _iter_content(self):
        with file(self.path, 'rb') as f:
            while True:
                chunk = f.read(toolkit.BUFFER_SIZE)
                if not chunk:
                    break
                yield chunk


class Router(object):

    def __init__(self, routes_model, allow_spawn=False):
        self._routes_model = routes_model
        self._allow_spawn = allow_spawn
        self._valid_origins = set()
        self._invalid_origins = set()
        self._host = None
        self._routes = _Routes()
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
                    route_ = getattr(routes_model, name)
                    if route_ not in self._preroutes:
                        self._preroutes.append(route_)
                    continue
                elif hasattr(attr, 'is_postroute'):
                    route_ = getattr(routes_model, name)
                    if route_ not in self._postroutes:
                        self._postroutes.append(route_)
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

        this.call = self.call

    def call(self, request=None, response=None, environ=None, **kwargs):
        if request is None:
            if this.request is not None:
                if not environ:
                    environ = {}
                    for key in ('HTTP_HOST',
                                'HTTP_ACCEPT_LANGUAGE',
                                'HTTP_ACCEPT_ENCODING',
                                'HTTP_IF_MODIFIED_SINCE',
                                'HTTP_AUTHORIZATION',
                                ):
                        if key in this.request.environ:
                            environ[key] = this.request.environ[key]
            request = Request(environ=environ, **kwargs)
        if response is None:
            response = Response()

        this.request = request
        this.response = response
        route_ = self._resolve_route(request)

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
            kwargs[arg] = request.get(arg)

        for i in self._preroutes:
            i(route_)
        result = None
        exception = None
        try:
            result = route_.callback(**kwargs)
            if route_.mime_type == 'text/event-stream' and \
                    self._allow_spawn and 'spawn' in request:
                _logger.debug('Spawn event stream for %r', request)
                coroutine.spawn(self._event_stream, request, result)
                result = None
            elif route_.mime_type and 'content-type' not in response:
                response.set('content-type', route_.mime_type)
        except Exception, exception:
            # To populate `exception` only
            raise
        finally:
            for i in self._postroutes:
                result = i(result, exception)

        return result

    def __repr__(self):
        return '<Router %s>' % type(self._routes_model).__name__

    def __call__(self, environ, start_response):
        request = Request(environ)
        response = Response()

        js_callback = None
        if 'callback' in request:
            js_callback = request.pop('callback')

        content = None
        try:
            this.cookie = _load_cookie(request, 'sugar_network_node')

            if 'HTTP_ORIGIN' in request.environ:
                enforce(self._assert_origin(request.environ), http.Forbidden,
                        'Cross-site is not allowed for %r origin',
                        request.environ['HTTP_ORIGIN'])
                response['Access-Control-Allow-Origin'] = \
                        request.environ['HTTP_ORIGIN']

            result = self.call(request, response)

            if isinstance(result, File):
                enforce(result is not File.AWAY, http.NotFound, 'No such file')
                response.update(result.meta)
                if 'location' in result.meta:
                    raise http.Redirect(result.meta['location'])
                enforce(isfile(result.path), 'No such file')
                if request.if_modified_since and \
                        result.mtime <= request.if_modified_since:
                    raise http.NotModified()
                result = file(result.path, 'rb')

            if not hasattr(result, 'read'):
                content = result
            else:
                if hasattr(result, 'fileno'):
                    response.content_length = os.fstat(result.fileno()).st_size
                elif hasattr(result, 'seek'):
                    result.seek(0, 2)
                    response.content_length = result.tell()
                    result.seek(0)
                content = _stream_reader(result)

        except http.StatusPass, error:
            response.status = error.status
            if error.headers:
                response.update(error.headers)
        except Exception, error:
            _logger.exception('Error while processing %r request', request.url)
            if isinstance(error, http.Status):
                response.status = error.status
                response.update(error.headers or {})
            else:
                response.status = '500 Internal Server Error'
            if request.method == 'HEAD':
                response.status = response.status[:4] + str(error)
            else:
                content = {'error': str(error), 'request': request.url}
                response.content_type = 'application/json'

        streamed_content = isinstance(content, types.GeneratorType)
        if js_callback or response.content_type == 'application/json':
            if streamed_content:
                content = ''.join(content)
                streamed_content = False
            else:
                content = json.dumps(content)
            if js_callback:
                content = '%s(%s);' % (js_callback, content)
        if request.method == 'HEAD':
            streamed_content = False
            content = None
        elif not streamed_content:
            response.content_length = len(content) if content else 0

        _logger.trace('%s call: request=%s response=%r content=%r',
                self, request.environ, response, repr(content)[:256])
        _save_cookie(response, 'sugar_network_node', this.cookie)
        start_response(response.status, response.items())

        if streamed_content:
            if response.content_type == 'text/event-stream':
                for event in _event_stream(request, content):
                    yield 'data: %s\n\n' % json.dumps(event)
            else:
                for i in content:
                    yield i
        elif content is not None:
            yield content

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
            if 'event' not in event:
                commons.update(event)
            else:
                event.update(commons)
                this.localcast(event)

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
        yield {'event': 'failure',
               'exception': type(error).__name__,
               'error': str(error),
               }
    finally:
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
        return [i18n.default_lang()]
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


def _load_cookie(request, name):
    cookie_str = request.environ.get('HTTP_COOKIE')
    if not cookie_str:
        return _Cookie()
    cookie = SimpleCookie()
    cookie.load(cookie_str)
    if name not in cookie:
        return _Cookie()
    raw_value = cookie.get(name).value
    if raw_value == 'unset_%s' % name:
        _logger.debug('Found unset %r cookie', name)
        return _Cookie()
    value = _Cookie(json.loads(b64decode(raw_value)))
    value.loaded = True
    _logger.debug('Found %r cookie value=%r', name, value)
    return value


def _save_cookie(response, name, value, age=3600):
    if value:
        _logger.debug('Set %r cookie value=%r age=%s', name, value, age)
        raw_value = b64encode(json.dumps(value))
    else:
        if not value.loaded:
            return
        _logger.debug('Unset %r cookie')
        raw_value = 'unset_%s' % name
    cookie = '%s=%s; Max-Age=%s; HttpOnly' % (name, raw_value, age)
    response.setdefault('set-cookie', []).append(cookie)


class _Cookie(dict):

    loaded = False


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
        enforce(acl ^ ACL.AUTHOR or acl & ACL.AUTH,
                'ACL.AUTHOR without ACL.AUTH')
        enforce(acl ^ ACL.AUTHOR or len(path) >= 2,
                'ACL.AUTHOR requires longer path')
        enforce(acl ^ ACL.AGG_AUTHOR or len(path) >= 3,
                'ACL.AGG_AUTHOR requires longer path')

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
            self.kwarg_names = set(code.co_varnames[1:code.co_argcount])

    def __repr__(self):
        path = '/'.join(['*' if i is None else i for i in self.path])
        if self.cmd:
            path += ('?cmd=%s' % self.cmd)
        return '%s /%s (%s)' % (self.method, path, self.callback.__name__)


class _RequestHeaders(dict):

    def __init__(self, environ):
        dict.__init__(self)
        self._environ = environ

    def __contains__(self, key):
        return 'HTTP_X_%s' % key.upper() in self._environ

    def __getitem__(self, key):
        value = self._environ.get('HTTP_X_%s' % key.upper())
        if value is not None:
            return json.loads(value)

    def __setitem__(self, key, value):
        dict.__setitem__(self, 'x-%s' % key, json.dumps(value))


class _ResponseHeaders(object):

    def __init__(self, headers):
        self._headers = headers

    def __contains__(self, key):
        return 'x-%s' % key.lower() in self._headers

    def __getitem__(self, key):
        value = self._headers.get('x-%s' % key.lower())
        if value is not None:
            return json.loads(value)

    def __setitem__(self, key, value):
        self._headers.set('x-%s' % key.lower(), json.dumps(value))


File.AWAY = File(None)
