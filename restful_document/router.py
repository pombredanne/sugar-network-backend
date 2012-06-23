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
import json
import types
import logging
from gettext import gettext as _

import active_document as ad
from active_toolkit import util, enforce

import restful_document as rd
from restful_document.http import Request, Response


_logger = logging.getLogger('restful_document.router')


class Router(object):

    def __init__(self, cp):
        self._cp = _ProxyCommands(cp)
        self._authenticated = set()

        if 'SSH_ASKPASS' in os.environ:
            # Otherwise ssh-keygen will popup auth dialogs on registeration
            del os.environ['SSH_ASKPASS']

    def __call__(self, environ, start_response):
        response = Response()
        result = None
        try:
            request = Request(environ)
            request.principal = self._authenticate(request)

            _logger.debug('Processing %s', request)
            result = self._cp.call(request, response)

        except ad.Redirect, error:
            response.status = '303 See Other'
            response['Location'] = error.location
            result = ''
        except Exception, error:
            if isinstance(error, ad.Unauthorized):
                response.status = '401 Unauthorized'
                response['WWW-Authenticate'] = 'Sugar'
            elif isinstance(error, ad.NotFound):
                response.status = '404 Not Found'
            else:
                util.exception(_('Error while processing %r request'),
                        environ['PATH_INFO'] or '/')
                if isinstance(error, ad.Forbidden):
                    response.status = '403 Forbidden'
                elif isinstance(error, rd.HTTPStatus):
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

        if not isinstance(result, types.GeneratorType) and \
                response.content_type == 'application/json':
            result = json.dumps(result)
            response.content_length = len(result)

        start_response(response.status, response.items())

        if isinstance(result, types.GeneratorType):
            for i in result:
                yield i
        else:
            yield result

    def _authenticate(self, request):
        user = request.environ.get('HTTP_SUGAR_USER')

        if not user:
            return ad.ANONYMOUS

        if user not in self._authenticated and \
                (request.path != ['user'] or request['method'] != 'POST'):
            _logger.debug('Logging %r user', user)
            enforce(self._cp.super_call('GET', 'exists',
                    document='user', guid=user), ad.Unauthorized,
                    _('Principal user does not exist'))
            self._authenticated.add(user)

        return user


class _ProxyCommands(ad.ProxyCommands):

    @ad.volume_command(method='POST', cmd='subscribe',
            permissions=ad.ACCESS_AUTH)
    def subscribe(self):
        return rd.SubscribeSocket.subscribe()

    @ad.document_command(method='DELETE',
            permissions=ad.ACCESS_AUTH | ad.ACCESS_AUTHOR)
    def delete(self, document, guid, request):
        # Servers should not delete documents immediately
        # to let synchronization possible
        return self.super_call('PUT', 'hide', document=document, guid=guid,
                principal=request.principal)
