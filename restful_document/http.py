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

import cgi
import json
import urllib
from urlparse import parse_qsl
from bisect import bisect_left
from gettext import gettext as _

import active_document as ad
import restful_document as rd
from active_toolkit import enforce


class Request(ad.Request):

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
                        _('Multipart request should contain only one file'))
                self.content_stream = files.list[0].file

        scope = len(self.path)
        enforce(scope >= 0 and scope < 4, rd.BadRequest,
                _('Incorrect requested path'))
        if scope == 3:
            self['document'], self['guid'], self['prop'] = self.path
        elif scope == 2:
            self['document'], self['guid'] = self.path
        elif scope == 1:
            self['document'], = self.path


class Response(ad.Response):

    status = '200 OK'

    @property
    def content_length(self):
        return self.get('Content-Length')

    # pylint: disable-msg=E1101,E0102
    @content_length.setter
    def content_length(self, value):
        self['Content-Length'] = value

    @property
    def content_type(self):
        return self.get('Content-Type')

    @content_type.setter
    def content_type(self, value):
        self['Content-Type'] = value


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
