# Copyright (C) 2012, Aleksey Lim
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
from gettext import gettext as _

from active_document import env, util
from active_document.util import enforce


_logger = logging.getLogger('active_document.commands')


def document_command(**kwargs):

    def decorate(func):
        _document_commands.add(func, **kwargs)
        return func

    return decorate


def directory_command(**kwargs):

    def decorate(func):
        _directory_commands.add(func, **kwargs)
        return func

    return decorate


def volume_command(**kwargs):

    def decorate(func):
        _volume_commands.add(func, **kwargs)
        return func

    return decorate


def call(request, volume):
    enforce('cmd' in request, _('Command is not specified'))
    cmd = request.pop('cmd')

    directory = None
    if 'document' in request:
        directory = volume[request.pop('document')]

    command, args, document = _resolve(cmd, directory, request)
    enforce(command is not None, env.NoCommand, _('Unsupported command'))

    if command.permissions & env.ACCESS_AUTH:
        enforce(env.principal.user is not None, env.Unauthorized,
                _('User is not authenticated'))
    if command.permissions & env.ACCESS_AUTHOR:
        enforce(env.principal.user in document['author'], env.Forbidden,
                _('Operation is permitted only for authors'))

    if command.accept_request:
        request['request'] = request

    try:
        response = command.callback(*args, **request)
    except Exception:
        util.exception(_logger, _('Failed to call %r command: request=%r'),
                command, request)
        raise RuntimeError(_('Failed to call %r command') % command)
    else:
        _logger.debug('Called %r: request=%r response=%r',
                command, request, response)

    return response


class Command(object):

    def __init__(self, callback=None, method='GET', document=None, cmd=None,
            mime_type='application/json', permissions=0):
        self.callback = callback
        self.mime_type = mime_type
        self.permissions = permissions
        self.accept_request = 'request' in _function_arg_names(callback)

        key = [method]
        if cmd:
            key.append(cmd)
        if document:
            key.append(document)
        if len(key) > 1:
            self.key = tuple(key)
        else:
            self.key = key[0]

    def __repr__(self):
        return str(self.key)


class Commands(dict):

    def add(self, callback, **kwargs):
        cmd = Command(callback, **kwargs)
        enforce(cmd.key not in self, _('Command %r already exists'), cmd)
        self[cmd.key] = cmd


class Request(dict):
    pass


def _resolve(cmd, directory, request):
    if directory is None:
        return _volume_commands.get(cmd), [], None

    metadata = directory.metadata
    active_cmd = cmd + (metadata.name,) if type(cmd) is tuple \
            else (cmd, metadata.name)

    if 'guid' not in request:
        command = _directory_commands.get(cmd) or \
                metadata.directory_commands.get(active_cmd)
        return command, [directory], None

    command = _document_commands.get(cmd) or \
            metadata.document_commands.get(active_cmd)
    if command is None:
        return None, None, None

    document = directory.get(request.pop('guid'))
    if hasattr(command.callback, 'im_self') and \
            command.callback.im_self is None:
        args = [document, directory]
    else:
        args = [directory, document]

    return command, args, document


def _function_arg_names(func):
    if hasattr(func, 'im_func'):
        func = func.im_func
    if not hasattr(func, 'func_code'):
        return []
    code = func.func_code
    return code.co_varnames[:code.co_argcount]


_volume_commands = Commands()
_directory_commands = Commands()
_document_commands = Commands()
