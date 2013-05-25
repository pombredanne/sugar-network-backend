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

import logging
from email.utils import formatdate

from sugar_network import toolkit
from sugar_network.db import env
from sugar_network.db.metadata import PropertyMetadata
from sugar_network.toolkit import http, enforce


_logger = logging.getLogger('db.commands')


def db_command(scope, **kwargs):

    def decorate(func):
        func.scope = scope
        func.kwargs = kwargs
        return func

    return decorate


volume_command = \
        lambda ** kwargs: db_command('volume', **kwargs)
volume_command_pre = \
        lambda ** kwargs: db_command('volume', wrapper='pre', **kwargs)
volume_command_post = \
        lambda ** kwargs: db_command('volume', wrapper='post', **kwargs)

directory_command = \
        lambda ** kwargs: db_command('directory', **kwargs)
directory_command_pre = \
        lambda ** kwargs: db_command('directory', wrapper='pre', **kwargs)
directory_command_post = \
        lambda ** kwargs: db_command('directory', wrapper='post', **kwargs)

document_command = \
        lambda ** kwargs: db_command('document', **kwargs)
document_command_pre = \
        lambda ** kwargs: db_command('document', wrapper='pre', **kwargs)
document_command_post = \
        lambda ** kwargs: db_command('document', wrapper='post', **kwargs)

property_command = \
        lambda ** kwargs: db_command('property', **kwargs)
property_command_pre = \
        lambda ** kwargs: db_command('property', wrapper='pre', **kwargs)
property_command_post = \
        lambda ** kwargs: db_command('property', wrapper='post', **kwargs)


def to_int(value):
    if isinstance(value, basestring):
        if not value:
            return 0
        enforce(value.isdigit(), 'Argument should be an integer value')
    return int(value)


def to_bool(value):
    if isinstance(value, basestring):
        return int(value.strip().lower() in ('true', '1', 'on'))
    return int(bool(value))


def to_list(value):
    if isinstance(value, basestring):
        if value:
            return value.split(',')
        else:
            return []
    return value


class Request(dict):

    #: Request payload, e.g., content passed by a HTTP POST/PUT request
    content = None
    #: If payload is a stream, :attr:`content` will be ``None`` in that case
    content_stream = None
    #: Payload stream length, if :attr:`content_stream` is set
    content_length = None
    #: Payload MIME type
    content_type = None
    access_level = env.ACCESS_REMOTE
    accept_language = None
    commands = None
    response = None
    static_prefix = None
    principal = None
    if_modified_since = None
    allow_redirects = False
    path = None

    def __init__(self, **kwargs):
        """Initialize parameters dictionary using named arguments."""
        dict.__init__(self, kwargs)
        self._pos = 0

    def __getitem__(self, key):
        enforce(key in self, 'Cannot find %r request argument', key)
        return self.get(key)

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

    def clone(self):
        request = type(self)()
        request.access_level = self.access_level
        request.accept_language = self.accept_language
        request.commands = self.commands
        return request

    def call(self, method, content=None, content_stream=None,
            content_length=None, **kwargs):
        enforce(self.commands is not None)

        request = self.clone()
        request.update(kwargs)
        request['method'] = method
        request.content = content
        request.content_stream = content_stream
        request.content_length = content_length

        return self.commands.call(request, Response())

    def __repr__(self):
        args = ['content_length=%r' % self.content_length,
                'access_level=%r' % self.access_level,
                'accept_language=%r' % self.accept_language,
                ] + ['%s=%r' % i for i in self.items()]
        return '<db.Request %s>' % ' '.join(args)


class Response(dict):

    def __init__(self, **kwargs):
        """Initialize parameters dictionary using named arguments."""
        dict.__init__(self, kwargs)

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
        args = ['%s=%r' % i for i in self.items()]
        return '<Response %s>' % ' '.join(args)

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


class CommandsProcessor(object):

    def __init__(self, volume=None):
        self._routes = {}
        self._commands = {
                'volume': _Commands(),
                'directory': _Commands(),
                'document': _Commands(),
                'property': _Commands(),
                }
        self.volume = volume

        self._scan_for_routes()

        for scope, kwargs in _scan_class(self.__class__, False):
            cmd = _Command((self,), **kwargs)
            self._commands[scope].add(cmd)

        if volume is not None:
            for directory in volume.values():
                for scope, kwargs in _scan_class(directory.document_class):
                    cmd = _ObjectCommand(directory, **kwargs)
                    self._commands[scope].add(cmd)

    def super_call(self, request, response):
        """Will be called if no commands were recognized.

        This function needs to be overloaded in child classes to implement
        proxy commands processor.

        """
        raise env.CommandNotFound()

    def call(self, request, response=None):
        """Make a command call.

        :param request:
            :class:`Request` object with call parameters
        :param response:
            optional :class:`Response` object to collect response details
        :returns:
            command call result

        """
        if request.path is not None:
            rout = self._routes.get((
                request['method'],
                request.path[0] if request.path else ''))
            if rout:
                return rout(self, request, response)

        cmd = self.resolve(request)
        enforce(cmd is not None, env.CommandNotFound, 'Unsupported command')

        enforce(request.access_level & cmd.access_level, http.Forbidden,
                'Operation is permitted on requester\'s level')

        if response is None:
            response = Response()
        request.commands = self
        request.response = response

        if not request.accept_language:
            request.accept_language = [toolkit.default_lang()]

        for arg, cast in cmd.arguments.items():
            if arg not in request:
                continue
            try:
                request[arg] = cast(request[arg])
            except Exception, error:
                raise RuntimeError('Cannot typecast %r command argument: %s' %
                        (arg, error))

        args = cmd.get_args(request)

        for pre in cmd.pre:
            pre(*args, request=request)

        kwargs = {}
        for arg in cmd.kwarg_names:
            if arg == 'request':
                kwargs[arg] = request
            elif arg == 'response':
                kwargs[arg] = response
            elif arg not in kwargs:
                kwargs[arg] = request.get(arg)

        result = cmd.callback(*args, **kwargs)

        for post in cmd.post:
            result = post(*args, result=result, request=request,
                    response=response)

        if not response.content_type:
            if isinstance(result, PropertyMetadata):
                response.content_type = result.get('mime_type')
            if not response.content_type:
                response.content_type = cmd.mime_type

        return result

    def resolve(self, request):
        """Recognize particular command from a :class:`Request` object.

        :param request:
            request object to recognize command from, the process is based
            on ``method`` and ``cmd`` parameters
        :returns:
            command object or ``None``

        """
        key = (request.get('method', 'GET'), request.get('cmd'), None)

        if 'document' not in request:
            return self._commands['volume'].get(key)

        document_key = key[:2] + (request['document'],)

        if 'guid' not in request:
            commands = self._commands['directory']
            return commands.get(key) or commands.get(document_key)

        if 'prop' not in request:
            commands = self._commands['document']
            return commands.get(key) or commands.get(document_key)

        commands = self._commands['property']
        return commands.get(key) or commands.get(document_key)

    def _scan_for_routes(self):
        cls = self.__class__
        while cls is not None:
            for name in dir(cls):
                attr = getattr(cls, name)
                if hasattr(attr, 'route'):
                    self._routes[attr.route] = attr
            # pylint: disable-msg=E1101
            cls = cls.__base__


class _Command(object):

    def __init__(self, args, callback, method='GET', document=None, cmd=None,
            mime_type=None, permissions=0, access_level=env.ACCESS_LEVELS,
            arguments=None, pre=None, post=None):
        self.args = args
        self.callback = callback
        self.mime_type = mime_type
        self.permissions = permissions
        self.access_level = access_level
        self.kwarg_names = _function_arg_names(callback)
        self.key = (method, cmd, document)
        self.arguments = arguments or {}
        self.pre = pre
        self.post = post

    def get_args(self, request):
        return self.args

    def __repr__(self):
        return '%s(method=%s, cmd=%s, document=%s)' % \
                ((self.callback.__name__,) + self.key)


class _ObjectCommand(_Command):

    def __init__(self, directory, **kwargs):
        _Command.__init__(self, (), document=directory.metadata.name, **kwargs)
        self._directory = directory

    def get_args(self, request):
        document = self._directory.get(request['guid'])
        document.request = request
        return (document,)


class _Commands(dict):

    def add(self, cmd):
        enforce(cmd.key not in self, 'Command %r already exists', cmd)
        self[cmd.key] = cmd


def _function_arg_names(func):
    if hasattr(func, 'im_func'):
        func = func.im_func
    if not hasattr(func, 'func_code'):
        return []
    code = func.func_code
    # `1:` is for skipping the first, `self` or `cls`, argument
    return code.co_varnames[1:code.co_argcount]


def _scan_class(root_cls, is_document_class=True):
    processed = set()
    commands = {}

    cls = root_cls
    while cls is not None:
        for name in dir(cls):
            if name in processed:
                continue
            attr = getattr(cls, name)
            if not hasattr(attr, 'scope'):
                continue
            enforce(not is_document_class or
                    attr.scope in ('document', 'property'),
                    'Wrong scale command')
            key = (attr.scope,
                   attr.kwargs.get('method') or 'GET',
                   attr.kwargs.get('cmd'))
            kwargs = commands.setdefault(key, {'pre': [], 'post': []})
            callback = getattr(root_cls, attr.__name__)
            if 'wrapper' not in attr.kwargs:
                kwargs.update(attr.kwargs)
                kwargs['callback'] = callback
            else:
                for key in ('arguments',):
                    if key in attr.kwargs and key not in kwargs:
                        kwargs[key] = attr.kwargs[key]
                kwargs[attr.kwargs['wrapper']].append(callback)
            processed.add(name)
        cls = cls.__base__

    for (scope, method, cmd), kwargs in commands.items():
        if 'callback' not in kwargs:
            kwargs['method'] = method
            if cmd:
                kwargs['cmd'] = cmd
            kwargs['callback'] = lambda self, request, response: \
                    self.super_call(request, response)
        yield scope, kwargs
