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

import re
import logging
from copy import copy
from gettext import gettext as _

from active_document import env
from active_toolkit import enforce


_logger = logging.getLogger('active_document.commands')


def command(scope, **kwargs):

    def decorate(func):
        func.commands_scope = scope
        func.kwargs = kwargs
        return func

    return decorate


volume_command = lambda ** kwargs: command('volume', **kwargs)
directory_command = lambda ** kwargs: command('directory', **kwargs)
document_command = lambda ** kwargs: command('document', **kwargs)
property_command = lambda ** kwargs: command('property', **kwargs)


_GUID_RE = re.compile('[a-zA-Z0-9_+-.]+$')


class CommandNotFound(Exception):
    pass


class Request(dict):

    content = None
    content_stream = None
    content_length = None
    principal = env.ANONYMOUS
    access_level = env.ACCESS_REMOTE
    accept_language = None

    def copy(self):
        return copy(self)

    def __getitem__(self, key):
        enforce(key in self, _('Cannot find %r request argument'), key)
        return self.get(key)


class Response(dict):

    content_length = None
    content_type = None


class CommandsProcessor(object):

    def __init__(self, volume=None):
        self._commands = {
                'volume': _Commands(),
                'directory': _Commands(),
                'document': _Commands(),
                'property': _Commands(),
                }
        self.volume = volume

        for scope, cb, attr in _scan_class_for_commands(self.__class__):
            cmd = _Command(cb, [self], **attr.kwargs)
            self._commands[scope].add(cmd)

        if volume is not None:
            for directory in volume.values():
                for scope, cb, attr in \
                        _scan_class_for_commands(directory.document_class):
                    if scope == 'directory':
                        enforce(attr.im_self is not None,
                                _('Command should be a @classmethod'))
                        cmd = _ClassCommand(directory, cb, **attr.kwargs)
                        self._commands[scope].add(cmd)
                    elif scope in ('document', 'property'):
                        enforce(attr.im_self is None,
                                _('Command should not be a @classmethod'))
                        cmd = _ObjectCommand(directory, cb, **attr.kwargs)
                        self._commands[scope].add(cmd)

    def call(self, request, response):
        cmd = self._resolve(request)
        enforce(cmd is not None, CommandNotFound, _('Unsupported command'))

        guid = request.get('guid')
        if guid is not None:
            enforce(_GUID_RE.match(guid) is not None,
                    _('Specified malformed GUID'))

        enforce(request.access_level & cmd.access_level, env.Forbidden,
                _('Operation is permitted on requester\'s level'))

        if request.principal is not None:
            if cmd.permissions & env.ACCESS_AUTH:
                enforce(request.principal is not env.ANONYMOUS,
                        env.Unauthorized, _('User is not authenticated'))
            if cmd.permissions & env.ACCESS_AUTHOR:
                enforce(self.volume is not None)
                doc = self.volume[request['document']].get(request['guid'])
                enforce(request.principal in doc['user'], env.Forbidden,
                        _('Operation is permitted only for authors'))

        if cmd.accept_request:
            request['request'] = request
        if cmd.accept_response:
            request['response'] = response

        result = cmd(request)

        _logger.debug('Called %r: request=%r result=%r', cmd, request, result)

        if not response.content_type:
            response.content_type = cmd.mime_type

        return result

    def _resolve(self, request):
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


class ProxyCommands(CommandsProcessor):

    def __init__(self, parent):
        CommandsProcessor.__init__(self)
        self.parent = parent
        self.volume = parent.volume

    def call(self, request, response):
        orig_request = request.copy()
        try:
            return CommandsProcessor.call(self, request, response)
        except CommandNotFound:
            return self.parent.call(orig_request, response)

    def super_call(self, method, cmd=None, content=None,
            access_level=env.ACCESS_REMOTE, principal=env.ANONYMOUS,
            response=None, **kwargs):
        request = Request(kwargs)
        request['method'] = method
        if cmd:
            request['cmd'] = cmd
        request.content = content
        request.access_level = access_level
        request.principal = principal

        if response is None:
            response = Response()

        return self.parent.call(request, response)


class _Command(object):

    def __init__(self, callback, args, method='GET', document=None, cmd=None,
            mime_type='application/json', permissions=0,
            access_level=env.ACCESS_LEVELS):
        self.callback = callback
        self.args = args
        self.mime_type = mime_type
        self.permissions = permissions
        self.access_level = access_level
        self.accept_request = 'request' in _function_arg_names(callback)
        self.accept_response = 'response' in _function_arg_names(callback)
        self.key = (method, cmd, document)

    def __call__(self, request):
        if 'method' in request:
            request.pop('method')
        if 'cmd' in request:
            request.pop('cmd')
        return self.callback(*self.args, **request)

    def __repr__(self):
        return '%s(method=%s, cmd=%s, document=%s)' % \
                ((self.callback.__name__,) + self.key)


class _ClassCommand(_Command):

    def __init__(self, directory, callback, **kwargs):
        _Command.__init__(self, callback, [], document=directory.metadata.name,
                **kwargs)
        self._directory = directory

    def __call__(self, request):
        if 'method' in request:
            request.pop('method')
        if 'cmd' in request:
            request.pop('cmd')
        request.pop('document')
        return self.callback(directory=self._directory, **request)


class _ObjectCommand(_Command):

    def __init__(self, directory, callback, **kwargs):
        _Command.__init__(self, callback, [], document=directory.metadata.name,
                **kwargs)
        self._directory = directory

    def __call__(self, request):
        if 'method' in request:
            request.pop('method')
        if 'cmd' in request:
            request.pop('cmd')
        request.pop('document')
        document = self._directory.get(request.pop('guid'))
        return self.callback(document, **request)


class _Commands(dict):

    def add(self, cmd):
        enforce(cmd.key not in self, _('Command %r already exists'), cmd)
        self[cmd.key] = cmd


def _function_arg_names(func):
    if hasattr(func, 'im_func'):
        func = func.im_func
    if not hasattr(func, 'func_code'):
        return []
    code = func.func_code
    return code.co_varnames[:code.co_argcount]


def _scan_class_for_commands(root_cls):
    processed = set()
    cls = root_cls
    while cls is not None:
        for name in dir(cls):
            if name in processed:
                continue
            attr = getattr(cls, name)
            if hasattr(attr, 'commands_scope'):
                callback = getattr(root_cls, attr.__name__)
                yield attr.commands_scope, callback, attr
            processed.add(name)
        cls = cls.__base__
