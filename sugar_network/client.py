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

import json
import logging
import hashlib
import collections
from gettext import gettext as _

import requests
from M2Crypto import DSA

from sugar_network import env, sugar
from sugar_network.util import enforce


_PAGE_SIZE = 16
_PAGE_NUMBER = 5

_logger = logging.getLogger('client')
_headers = {}


class ServerError(Exception):

    def __init__(self, request_, error):
        self.request = request_
        Exception.__init__(self, error)


class Query(object):
    """Query resource objects."""

    def __init__(self, resource=None, query=None, order_by=None,
            reply_properties=None, **filters):
        """
        :param resource:
            resource name to search in; if `None`, look for all resource types
        :param query:
            full text search query string in Xapian format
        :param order_by:
            name of property to sort by; might be prefixed by either `+` or `-`
            to change order's direction
        :param reply_properties:
            list of property names to return for found objects;
            by default, only GUIDs will be returned; for missed properties,
            will be sent additional requests to a server on getting access
            to particular object.
        :param filters:
            a dictionary of properties to filter resulting list

        """
        self._path = '/'
        if resource:
            self._path += resource
        self._resource = resource
        self._query = query
        self._order_by = order_by
        self._reply_properties = reply_properties
        self._filters = filters
        self._total = None
        self._page_access = collections.deque([], _PAGE_NUMBER)
        self._pages = {}

        self._reset()

    @property
    def total(self):
        """Total number of objects."""
        if self._total is None:
            self._fetch_page(0)
        return self._total

    @property
    def order_by(self):
        """Current order of resulting list.

        Name of property to sort by. Might be prefixed by either `+` or `-`
        to change order's direction.

        """
        return self._order_by

    # pylint: disable-msg=E1101,E0102
    @order_by.setter
    def order_by(self, value):
        if self._order_by == value:
            return
        self._order_by = value
        self._reset()

    def filter(self, query=None, **filters):
        """Change query parameters.

        :param query:
            full text search query string in Xapian format
        :param filters:
            a dictionary of properties to filter resulting list

        """
        if query == self._query and filters == self._filters:
            return
        self._query = query
        self._filters = filters
        self._reset()

    def get(self, offset, default=None):
        """Get either object by offset or default value.

        :param offset:
            offset to get object for
        :param default:
            value to return if offset if not found
        :returns:
            `Object` value or `default`

        """
        if offset < 0 or self._total is not None and offset >= self._total:
            return default
        page = offset / _PAGE_SIZE
        if page not in self._pages:
            self._fetch_page(page)
            if offset >= self._total:
                return default
        return self._pages[page][offset - page * _PAGE_SIZE]

    def __getitem__(self, offset):
        """Get object by offset.

        :param offset:
            offset to get object for
        :returns:
            `Object` value or raise `KeyError` exception if offset is invalid

        """
        result = self.get(offset)
        enforce(result is not None, KeyError, _('Offset is out of range'))
        return result

    def _fetch_page(self, page):
        params = {}
        if self._filters:
            params.update(self._filters)
        params['offset'] = page * _PAGE_SIZE
        params['limit'] = _PAGE_SIZE
        if self._query:
            params['query'] = self._query
        if self._order_by:
            params['order_by'] = self._order_by
        if self._reply_properties:
            params['reply'] = ','.join(self._reply_properties)

        reply = request('GET', self._path, params=params)
        self._total = reply['total']

        result = [None] * len(reply['result'])
        for i, props in enumerate(reply['result']):
            result[i] = Object(self._resource or props['document'], props)

        if not self._page_access or self._page_access[-1] != page:
            if len(self._page_access) == _PAGE_NUMBER:
                del self._pages[self._page_access[0]]
            self._page_access.append(page)
        self._pages[page] = result

    def _reset(self):
        self._page_access.clear()
        self._pages.clear()
        self._total = None


class Object(dict):

    def __init__(self, resource, props=None):
        dict.__init__(self, props or {})
        self._resource = resource
        if 'guid' in self:
            self._path = '/%s/%s' % (resource, self['guid'])
        else:
            self._path = None
        self._got = False
        self._dirty = set()

    def __getitem__(self, prop):
        result = self.get(prop)
        if result is None:
            if self._path and not self._got:
                reply = request('GET', self._path)
                reply.update(self)
                self.update(reply)
                self._got = True
            enforce(prop in self, KeyError,
                    _('Property "%s" is absent in "%s" resource'),
                    prop, self._resource)
            result = self.get(prop)
        return result

    def __setitem__(self, prop, value):
        enforce(prop != 'guid', _('Property "guid" is read-only'))
        if self.get(prop) != value:
            self._dirty.add(prop)
        dict.__setitem__(self, prop, value)

    def post(self):
        if not self._dirty:
            return
        data = {}
        for i in self._dirty:
            data[i] = self[i]
        if 'guid' in self:
            request('PUT', self._path, data=data)
        else:
            reply = request('POST', '/' + self._resource, data=data)
            self.update(reply)
            self._path = '/%s/%s' % (self._resource, self['guid'])
        self._dirty.clear()


def delete(resource, guid):
    request('DELETE', '/%s/%s' % (resource, guid))


def request(method, path, data=None, params=None):
    if not _headers:
        uid = sugar.guid()
        _headers['sugar_user'] = uid
        _headers['sugar_user_signature'] = _sign(uid)

    headers = _headers.copy()
    if method in ('PUT', 'POST'):
        headers['Content-Type'] = 'application/json'
        data = json.dumps(data)

    verify = True
    if env.no_check_certificate.value:
        verify = False
    elif env.certfile.value:
        verify = env.certfile.value

    while True:
        try:
            response = requests.request(method, env.api_url.value + path,
                    params=params, data=data, verify=verify, headers=headers,
                    config={'keep_alive': True})
        except requests.exceptions.SSLError:
            _logger.warning(_('Pass --no-check-certificate ' \
                    'to avoid SSL checks'))
            raise

        if response.status_code != 200:
            if response.status_code == 401 and path != '/user':
                _register()
                continue
            content = response.content
            try:
                error = json.loads(content)
                raise ServerError(error['request'], error['error'])
            except ValueError:
                _logger.debug('Got %s HTTP error for "%s" request:\n%s',
                        response.status_code, path, content)
                response.raise_for_status()

        return json.loads(response.content)


def _register():
    request('POST', '/user', {
        'nickname': sugar.nickname() or '',
        'color': sugar.color() or '#000000,#000000',
        'machine_sn': sugar.machine_sn() or '',
        'machine_uuid': sugar.machine_uuid() or '',
        'pubkey': sugar.pubkey(),
        })


def _sign(data):
    key = DSA.load_key(sugar.profile_path('owner.key'))
    return key.sign_asn1(hashlib.sha1(data).digest()).encode('hex')
